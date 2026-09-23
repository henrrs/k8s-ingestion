"""SQL Server catalog access for Python runtimes without a Spark JVM."""

import os

from company_ingestion_core.planner import (
    DATE_TYPES,
    Column,
    HistogramStep,
    TableMetadata,
    desired_chunk_count,
    identifier,
    parse_value,
    plan_distributed_read,
    select_column,
)


def sqlalchemy_url(config):
    import sqlalchemy as sa

    try:
        username = os.environ[config.get("user_env", "SQLSERVER_USER")]
        password = os.environ[config.get("password_env", "SQLSERVER_PASSWORD")]
    except KeyError as error:
        raise ValueError(
            f"Missing source credential environment variable: {error.args[0]}"
        ) from None
    return sa.URL.create(
        "mssql+pymssql",
        username=username,
        password=password,
        host=config["host"],
        port=int(config.get("port", 1433)),
        database=config["database"],
    )


class SqlServerSqlAlchemyCatalog:
    def __init__(self, engine, source_config):
        self.engine = engine
        self.config = source_config
        self.schema = source_config["schema"]
        self.table = source_config["table"]
        self.qualified_table = (
            f"{identifier(self.schema)}.{identifier(self.table)}"
        )
        self.query_timeout_seconds = int(
            source_config.get("query_timeout_seconds", 600)
        )
        self.warnings = []

    def query(self, sql, parameters=None):
        import sqlalchemy as sa

        with self.engine.connect() as connection:
            result = connection.execute(sa.text(sql), parameters or {})
            return [dict(row) for row in result.mappings().all()]

    def metadata(self):
        found = self.query(
            "SELECT OBJECT_ID(:qualified, 'U') AS object_id",
            {"qualified": self.qualified_table},
        )
        if not found or found[0]["object_id"] is None:
            raise ValueError(
                f"Table does not exist or is not visible: {self.qualified_table}"
            )
        object_id = int(found[0]["object_id"])
        column_rows = self.query(f"""
            SELECT c.name, typ.name AS sql_type, c.is_nullable, c.precision, c.scale,
                   ix.index_id, ix.is_unique, ix.is_primary_key, ix.index_type,
                   st.stats_id
            FROM sys.columns AS c
            JOIN sys.types AS typ
              ON c.system_type_id=typ.user_type_id AND typ.is_user_defined=0
            OUTER APPLY (
                SELECT TOP (1) i.index_id, i.is_unique, i.is_primary_key,
                       i.type AS index_type
                FROM sys.indexes AS i
                JOIN sys.index_columns AS ic
                  ON i.object_id=ic.object_id AND i.index_id=ic.index_id
                WHERE i.object_id=c.object_id AND ic.column_id=c.column_id
                  AND ic.key_ordinal=1 AND i.is_disabled=0
                  AND i.is_hypothetical=0 AND i.has_filter=0
                  AND i.type IN (1, 2)
                ORDER BY CASE WHEN i.type=1 THEN 0 ELSE 1 END,
                         i.is_unique DESC, i.is_primary_key DESC
            ) AS ix
            OUTER APPLY (
                SELECT TOP (1) s.stats_id
                FROM sys.stats AS s
                JOIN sys.stats_columns AS sc
                  ON s.object_id=sc.object_id AND s.stats_id=sc.stats_id
                WHERE s.object_id=c.object_id AND sc.column_id=c.column_id
                  AND sc.stats_column_id=1 AND s.has_filter=0
                ORDER BY CASE WHEN s.stats_id=ix.index_id THEN 0 ELSE 1 END,
                         s.stats_id
            ) AS st
            WHERE c.object_id={object_id}
            ORDER BY c.column_id
        """)
        columns = tuple(
            Column(
                name=row["name"],
                sql_type=row["sql_type"],
                nullable=bool(row["is_nullable"]),
                index_id=int(row["index_id"]) if row["index_id"] else None,
                clustered=row["index_type"] == 1,
                unique=bool(row["is_unique"]) if row["is_unique"] is not None else False,
                primary_key=bool(row["is_primary_key"])
                if row["is_primary_key"] is not None else False,
                stats_id=int(row["stats_id"]) if row["stats_id"] else None,
                precision=int(row["precision"]),
                scale=int(row["scale"]),
            )
            for row in column_rows
        )
        try:
            size = self.query(f"""
                SELECT COALESCE(SUM(row_count),0) AS rows,
                       COALESCE(SUM(used_page_count),0)*CAST(8192 AS bigint) AS bytes
                FROM sys.dm_db_partition_stats
                WHERE object_id={object_id} AND index_id IN (0,1)
            """)[0]
        except Exception:
            self.warnings.append(
                "Physical size DMV unavailable; bytes estimated from catalog row "
                "count and column widths."
            )
            size = self.query(f"""
                SELECT COALESCE(SUM(p.rows),0) AS rows,
                       COALESCE(SUM(p.rows),0) * (
                           SELECT COALESCE(SUM(
                               CASE WHEN max_length=-1 THEN 1024 ELSE max_length END
                           ),1)
                           FROM sys.columns WHERE object_id={object_id}
                       ) AS bytes
                FROM sys.partitions AS p
                WHERE p.object_id={object_id} AND p.index_id IN (0,1)
            """)[0]
        return TableMetadata(
            int(size["rows"]), int(size["bytes"]), columns, object_id
        )

    def histogram(self, metadata, column):
        if not column.stats_id:
            return []
        conversion = "CONVERT(nvarchar(128), range_high_key)"
        if column.sql_type in DATE_TYPES:
            conversion = (
                "CONVERT(nvarchar(128), CAST(range_high_key AS datetime2), 126)"
            )
        try:
            rows = self.query(f"""
                SELECT {conversion} AS high, range_rows, equal_rows
                FROM sys.dm_db_stats_histogram(
                    {metadata.object_id}, {column.stats_id}
                )
                ORDER BY step_number
            """)
            return [
                HistogramStep(
                    parse_value(row["high"], column),
                    float(row["range_rows"]),
                    float(row["equal_rows"]),
                )
                for row in rows
            ]
        except Exception:
            self.warnings.append(
                "Histogram unavailable; ordered index seeks will be used."
            )
            return []

    def bounds(self, column):
        name = identifier(column.name)
        conversion = f"CONVERT(nvarchar(128), {name})"
        if column.sql_type in DATE_TYPES:
            conversion = (
                f"CONVERT(nvarchar(128), CAST({name} AS datetime2), 126)"
            )
        values = []
        for direction in ("ASC", "DESC"):
            rows = self.query(
                f"SELECT TOP (1) {conversion} AS value "
                f"FROM {self.qualified_table} WHERE {name} IS NOT NULL "
                f"ORDER BY {name} {direction}"
            )
            values.append(
                parse_value(rows[0]["value"], column) if rows else None
            )
        return tuple(values)

    def plan(self, planner_config):
        metadata = self.metadata()
        if desired_chunk_count(metadata, planner_config) == 1:
            return metadata, plan_distributed_read(
                metadata, planner_config, warnings=self.warnings
            )
        column = select_column(metadata.columns)
        if column is None:
            return metadata, plan_distributed_read(
                metadata, planner_config, warnings=self.warnings
            )
        histogram = self.histogram(metadata, column)
        bounds = None if histogram else self.bounds(column)
        return metadata, plan_distributed_read(
            metadata,
            planner_config,
            column=column,
            histogram=histogram,
            bounds=bounds,
            warnings=self.warnings,
        )
