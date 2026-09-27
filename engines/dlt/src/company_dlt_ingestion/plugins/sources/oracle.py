"""Oracle source plugin using python-oracledb's native Arrow data frames."""

from decimal import Decimal
import os
import time

from company_ingestion_core.planner import (
    Column,
    TableMetadata,
    ansi_identifier,
    desired_chunk_count,
    parse_value,
    plan_distributed_read,
    select_column,
)


def oracle_name(value, preserve_case=False):
    """Return the catalog spelling used by ordinary or quoted Oracle objects."""
    return str(value) if preserve_case else str(value).upper()


def connect(source_config):
    import oracledb

    user_env = source_config.get("user_env", "ORACLE_USER")
    password_env = source_config.get("password_env", "ORACLE_PASSWORD")
    try:
        user = os.environ[user_env]
        password = os.environ[password_env]
    except KeyError as error:
        raise ValueError(
            f"Missing source credential environment variable: {error.args[0]}"
        ) from None
    service_name = source_config.get("service_name") or source_config.get(
        "database"
    )
    dsn = oracledb.makedsn(
        source_config["host"],
        int(source_config.get("port", 1521)),
        service_name=service_name,
    )
    connection = oracledb.connect(user=user, password=password, dsn=dsn)
    connection.call_timeout = int(
        source_config.get("query_timeout_seconds", 600)
    ) * 1000
    return connection


def generic_sql_type(data_type, precision, scale):
    """Map Oracle catalog types onto the planner's technology-neutral types."""
    value = data_type.upper()
    if value == "NUMBER":
        if scale and scale > 0:
            return "decimal"
        if precision is not None and precision <= 9:
            return "int"
        if precision is not None and precision <= 18:
            return "bigint"
        return "decimal"
    if value in {"FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"}:
        return "float"
    if value == "DATE" or value.startswith("TIMESTAMP"):
        return "datetime2"
    return value.lower()


class OracleCatalog:
    """Read portable planning inputs from Oracle's ALL_* catalog views."""

    def __init__(self, connection, source_config):
        self.connection = connection
        preserve = bool(source_config.get("preserve_identifier_case", False))
        self.owner = oracle_name(source_config["schema"], preserve)
        self.table = oracle_name(source_config["table"], preserve)
        self.qualified_table = (
            f"{ansi_identifier(self.owner)}.{ansi_identifier(self.table)}"
        )
        self.warnings = []
        self.snapshot_scn = None

    def query(self, sql, parameters=None):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, parameters or {})
            names = [item[0].lower() for item in cursor.description or ()]
            return [dict(zip(names, row)) for row in cursor]

    def capture_snapshot(self):
        row = self.query(
            "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER AS scn FROM dual"
        )[0]
        self.snapshot_scn = int(row["scn"])
        return self.snapshot_scn

    def metadata(self):
        table_rows = self.query(
            """
            SELECT t.num_rows, t.avg_row_len, t.blocks, o.object_id
            FROM all_tables t
            JOIN all_objects o
              ON o.owner=t.owner AND o.object_name=t.table_name
             AND o.object_type='TABLE'
            WHERE t.owner=:owner AND t.table_name=:table_name
            """,
            {"owner": self.owner, "table_name": self.table},
        )
        if not table_rows:
            raise ValueError(
                "Table does not exist or is not visible: "
                f"{self.qualified_table}"
            )
        column_rows = self.query(
            """
            SELECT column_name, data_type, nullable, data_precision, data_scale,
                   data_length, avg_col_len, column_id
            FROM all_tab_columns
            WHERE owner=:owner AND table_name=:table_name
            ORDER BY column_id
            """,
            {"owner": self.owner, "table_name": self.table},
        )
        index_rows = self.query(
            """
            SELECT ic.column_name, i.index_name, i.uniqueness,
                   CASE WHEN c.constraint_type='P' THEN 1 ELSE 0 END primary_key
            FROM all_ind_columns ic
            JOIN all_indexes i
              ON i.owner=ic.index_owner AND i.index_name=ic.index_name
             AND i.table_owner=ic.table_owner AND i.table_name=ic.table_name
            LEFT JOIN all_constraints c
              ON c.owner=i.owner AND c.index_name=i.index_name
             AND c.table_name=i.table_name AND c.constraint_type='P'
            WHERE ic.table_owner=:owner AND ic.table_name=:table_name
              AND ic.column_position=1 AND i.status='VALID'
              AND i.index_type='NORMAL'
            ORDER BY CASE WHEN c.constraint_type='P' THEN 0 ELSE 1 END,
                     CASE WHEN i.uniqueness='UNIQUE' THEN 0 ELSE 1 END,
                     i.index_name
            """,
            {"owner": self.owner, "table_name": self.table},
        )
        leading = {}
        for index, row in enumerate(index_rows, start=1):
            leading.setdefault(row["column_name"], (index, row))
        columns = []
        for row in column_rows:
            selected = leading.get(row["column_name"])
            index_id, index_row = selected if selected else (None, {})
            precision = int(row["data_precision"] or 0)
            scale = int(row["data_scale"] or 0)
            columns.append(
                Column(
                    name=row["column_name"],
                    sql_type=generic_sql_type(
                        row["data_type"], row["data_precision"], row["data_scale"]
                    ),
                    nullable=row["nullable"] == "Y",
                    index_id=index_id,
                    clustered=False,
                    unique=index_row.get("uniqueness") == "UNIQUE",
                    primary_key=bool(index_row.get("primary_key", 0)),
                    precision=precision,
                    scale=scale,
                )
            )
        table = table_rows[0]
        estimated_rows = table["num_rows"]
        if estimated_rows is None:
            self.warnings.append(
                "Oracle table statistics were missing; planning executed COUNT(*)."
            )
            suffix = (
                f" AS OF SCN {self.snapshot_scn}" if self.snapshot_scn else ""
            )
            estimated_rows = self.query(
                f"SELECT COUNT(*) AS row_count FROM {self.qualified_table}{suffix}"
            )[0]["row_count"]
        estimated_rows = int(estimated_rows or 0)
        avg_row_len = int(table["avg_row_len"] or 0)
        if avg_row_len <= 0:
            avg_row_len = sum(
                int(row["avg_col_len"] or row["data_length"] or 1)
                for row in column_rows
            )
        logical_bytes = estimated_rows * max(1, avg_row_len)
        allocated_bytes = int(table["blocks"] or 0) * 8192
        return TableMetadata(
            estimated_rows=estimated_rows,
            estimated_bytes=max(logical_bytes, allocated_bytes),
            columns=tuple(columns),
            object_id=int(table["object_id"]),
        )

    def histogram(self, metadata, column):
        # Oracle hybrid/frequency histogram endpoint values are stored in a
        # type-dependent encoded representation. Indexed snapshot MIN/MAX is a
        # safe first strategy; adding decoded quantiles belongs in this adapter.
        return []

    def bounds(self, column):
        name = ansi_identifier(column.name)
        suffix = f" AS OF SCN {self.snapshot_scn}" if self.snapshot_scn else ""
        row = self.query(
            f"SELECT MIN({name}) AS low, MAX({name}) AS high "
            f"FROM {self.qualified_table}{suffix}"
        )[0]

        def convert(value):
            if value is None:
                return None
            if column.sql_type in {
                "tinyint", "smallint", "int", "bigint", "decimal",
                "numeric", "money", "smallmoney", "float", "real",
            }:
                return Decimal(str(value))
            if isinstance(value, str):
                return parse_value(value, column)
            return value

        return convert(row["low"]), convert(row["high"])


class OraclePartitioner:
    def __init__(self, source_config):
        self.source_config = source_config

    def plan(self, planner_config):
        connection = connect(self.source_config)
        try:
            catalog = OracleCatalog(connection, self.source_config)
            scn = catalog.capture_snapshot()
            metadata = catalog.metadata()
            common = {
                "warnings": catalog.warnings,
                "identifier_renderer": ansi_identifier,
                "snapshot": {"kind": "oracle_scn", "value": scn},
            }
            if desired_chunk_count(metadata, planner_config) == 1:
                return metadata, plan_distributed_read(
                    metadata, planner_config, **common
                )
            column = select_column(metadata.columns)
            if column is None:
                return metadata, plan_distributed_read(
                    metadata, planner_config, **common
                )
            return metadata, plan_distributed_read(
                metadata,
                planner_config,
                column=column,
                bounds=catalog.bounds(column),
                **common,
            )
        finally:
            connection.close()


def oracle_arrow_schema(connection, owner, table):
    """Build a compact, deterministic Arrow schema for supported Oracle types."""
    import pyarrow as pa

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name, data_type, data_precision, data_scale
            FROM all_tab_columns
            WHERE owner=:owner AND table_name=:table_name
            ORDER BY column_id
            """,
            {"owner": owner, "table_name": table},
        )
        rows = cursor.fetchall()

    def data_type(row):
        _name, oracle_type, precision, scale = row
        value = oracle_type.upper()
        if value == "NUMBER":
            if precision is None:
                return pa.float64()
            precision, scale = int(precision), int(scale or 0)
            if scale == 0 and precision <= 18:
                return pa.int64()
            if precision <= 38:
                return pa.decimal128(precision, scale)
            if precision <= 76:
                return pa.decimal256(precision, scale)
            return pa.float64()
        if value == "BINARY_FLOAT":
            return pa.float32()
        if value in {"BINARY_DOUBLE", "FLOAT"}:
            return pa.float64()
        if value in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG"}:
            return pa.string()
        if value in {"RAW", "BLOB", "LONG RAW"}:
            return pa.binary()
        if value == "DATE":
            return pa.timestamp("s")
        if value.startswith("TIMESTAMP"):
            fractional = int(scale or 6)
            unit = "s" if fractional == 0 else "ms" if fractional <= 3 else "us" if fractional <= 6 else "ns"
            return pa.timestamp(unit)
        if value == "BOOLEAN":
            return pa.bool_()
        raise ValueError(
            f"Oracle Arrow reader does not yet support column {_name} type {oracle_type}"
        )

    return pa.schema([(row[0], data_type(row)) for row in rows])


class OracleArrowReader:
    """Stream Oracle nanoarrow batches into PyArrow without Python row objects."""

    columnar = True

    def __init__(self, source_config, plan):
        self.source = source_config
        preserve = bool(source_config.get("preserve_identifier_case", False))
        self.owner = oracle_name(source_config["schema"], preserve)
        self.table = oracle_name(source_config["table"], preserve)
        self.qualified = (
            f"{ansi_identifier(self.owner)}.{ansi_identifier(self.table)}"
        )
        self.snapshot_scn = (plan.get("snapshot") or {}).get("value")
        if not self.snapshot_scn:
            raise ValueError("Oracle worker requires the planner snapshot SCN")
        self.durations = {
            "source_connect": 0.0,
            "source_schema": 0.0,
            "source_execute": 0.0,
            "source_fetch": 0.0,
            "arrow_handoff": 0.0,
        }
        phase = time.perf_counter()
        self.client = connect(source_config)
        self.durations["source_connect"] += time.perf_counter() - phase
        phase = time.perf_counter()
        self.schema = oracle_arrow_schema(self.client, self.owner, self.table)
        self.durations["source_schema"] += time.perf_counter() - phase

    def batches(self, predicate, fetch_size):
        import pyarrow as pa

        sql = (
            f"SELECT * FROM {self.qualified} AS OF SCN :snapshot_scn "
            f"WHERE {predicate}"
        )
        phase = time.perf_counter()
        frames = iter(
            self.client.fetch_df_batches(
                statement=sql,
                parameters={"snapshot_scn": int(self.snapshot_scn)},
                size=fetch_size,
                requested_schema=self.schema,
            )
        )
        self.durations["source_execute"] += time.perf_counter() - phase
        while True:
            phase = time.perf_counter()
            try:
                frame = next(frames)
            except StopIteration:
                self.durations["source_fetch"] += time.perf_counter() - phase
                return
            self.durations["source_fetch"] += time.perf_counter() - phase
            phase = time.perf_counter()
            batch = pa.table(frame)
            self.durations["arrow_handoff"] += time.perf_counter() - phase
            yield batch

    def close(self):
        self.client.close()


class OracleSourceAdapter:
    name = "oracle"
    default_backend = "oracle_arrow"

    def __init__(self, source_config):
        missing = [
            key for key in ("host", "schema", "table")
            if not source_config.get(key)
        ]
        if not source_config.get("service_name") and not source_config.get(
            "database"
        ):
            missing.append("service_name (or database)")
        if missing:
            raise ValueError("Oracle source requires: " + ", ".join(missing))
        self.config = source_config

    def partitioner(self):
        return OraclePartitioner(self.config)

    def reader(self, backend, plan=None):
        if backend != "oracle_arrow":
            raise ValueError("Oracle extract_backend must be oracle_arrow")
        return OracleArrowReader(self.config, plan or {})
