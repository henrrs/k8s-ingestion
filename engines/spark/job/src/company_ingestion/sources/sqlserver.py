"""SQL Server metadata plus JDBC and native Arrow Spark read paths."""

from dataclasses import asdict
from functools import partial
import os

from ..planner import Column, HistogramStep, TableMetadata, DATE_TYPES, identifier, parse_value, plan_read, select_column


def sql_string(value):
    return "N'" + value.replace("'", "''") + "'"


class SqlServerSource:
    def __init__(self, spark, config, planner):
        self.spark = spark
        self.config = config
        self.planner_config = planner
        self.qualified_table = f"{identifier(config.schema)}.{identifier(config.table)}"
        # Braces escape JDBC property values. Credentials are never in the URL.
        database = config.database.replace("}", "}}")
        self.url = (f"jdbc:sqlserver://{config.host}:{config.port};databaseName={{{database}}};"
                    f"encrypt={str(config.encrypt).lower()};"
                    f"trustServerCertificate={str(config.trust_server_certificate).lower()};"
                    "applicationName=CompanyIngestion;loginTimeout=30;")
        try:
            user, password = os.environ[config.user_env], os.environ[config.password_env]
        except KeyError as error:
            raise ValueError(f"Missing source credential environment variable: {error.args[0]}") from None
        self.properties = {"user": user, "password": password,
                           "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                           "queryTimeout": str(config.query_timeout_seconds),
                           "responseBuffering": "adaptive"}
        java_properties = spark._jvm.java.util.Properties()
        java_properties.setProperty("user", user)
        java_properties.setProperty("password", password)
        java_properties.setProperty("responseBuffering", "adaptive")
        driver = spark._jvm.com.microsoft.sqlserver.jdbc.SQLServerDriver()
        self.connection = driver.connect(self.url, java_properties)
        self.warnings = []
        self._arrow_metrics = None
        self._metadata = None
        self._resolved_fetch_size = None

    def query(self, sql):
        statement = self.connection.createStatement()
        statement.setQueryTimeout(self.config.query_timeout_seconds)
        result = None
        try:
            result = statement.executeQuery(sql)
            metadata = result.getMetaData()
            names = [metadata.getColumnLabel(i) for i in range(1, metadata.getColumnCount() + 1)]
            rows = []
            while result.next():
                rows.append({name: result.getString(i) for i, name in enumerate(names, 1)})
            return rows
        finally:
            if result is not None:
                result.close()
            statement.close()

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def metadata(self):
        found = self.query(f"SELECT OBJECT_ID({sql_string(self.qualified_table)}, 'U') AS object_id")
        if not found or found[0]["object_id"] is None:
            raise ValueError(f"Table does not exist or is not visible: {self.qualified_table}")
        object_id = int(found[0]["object_id"])
        column_rows = self.query(f"""
            SELECT c.name, typ.name AS sql_type, c.is_nullable, c.precision, c.scale,
                   ix.index_id, ix.is_unique, ix.is_primary_key, ix.index_type,
                   st.stats_id
            FROM sys.columns AS c
            JOIN sys.types AS typ ON c.system_type_id=typ.user_type_id AND typ.is_user_defined=0
            OUTER APPLY (
                SELECT TOP (1) i.index_id, i.is_unique, i.is_primary_key, i.type AS index_type
                FROM sys.indexes AS i
                JOIN sys.index_columns AS ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
                WHERE i.object_id=c.object_id AND ic.column_id=c.column_id
                  AND ic.key_ordinal=1 AND i.is_disabled=0 AND i.is_hypothetical=0
                  AND i.has_filter=0 AND i.type IN (1, 2)
                ORDER BY CASE WHEN i.type=1 THEN 0 ELSE 1 END, i.is_unique DESC, i.is_primary_key DESC
            ) AS ix
            OUTER APPLY (
                SELECT TOP (1) s.stats_id
                FROM sys.stats AS s
                JOIN sys.stats_columns AS sc ON s.object_id=sc.object_id AND s.stats_id=sc.stats_id
                WHERE s.object_id=c.object_id AND sc.column_id=c.column_id
                  AND sc.stats_column_id=1 AND s.has_filter=0
                ORDER BY CASE WHEN s.stats_id=ix.index_id THEN 0 ELSE 1 END, s.stats_id
            ) AS st
            WHERE c.object_id={object_id}
            ORDER BY c.column_id
        """)
        columns = tuple(Column(
            name=r["name"], sql_type=r["sql_type"], nullable=r["is_nullable"] == "1",
            index_id=int(r["index_id"]) if r["index_id"] else None,
            clustered=r["index_type"] == "1", unique=r["is_unique"] == "1",
            primary_key=r["is_primary_key"] == "1",
            stats_id=int(r["stats_id"]) if r["stats_id"] else None,
            precision=int(r["precision"]), scale=int(r["scale"]),
        ) for r in column_rows)
        try:
            size = self.query(f"""
                SELECT COALESCE(SUM(row_count),0) AS rows,
                       COALESCE(SUM(used_page_count),0)*CAST(8192 AS bigint) AS bytes
                FROM sys.dm_db_partition_stats
                WHERE object_id={object_id} AND index_id IN (0,1)
            """)[0]
        except Exception:
            # Allocations have different permissions from table SELECT. Conservative
            # catalog estimate remains possible without a source COUNT(*) scan.
            self.warnings.append("Physical size DMV unavailable; bytes estimated from catalog row count and column widths.")
            size = self.query(f"""
                SELECT COALESCE(SUM(p.rows),0) AS rows,
                       COALESCE(SUM(p.rows),0) * (
                           SELECT COALESCE(SUM(CASE WHEN max_length=-1 THEN 1024 ELSE max_length END),1)
                           FROM sys.columns WHERE object_id={object_id}
                       ) AS bytes
                FROM sys.partitions AS p WHERE p.object_id={object_id} AND p.index_id IN (0,1)
            """)[0]
        return TableMetadata(int(size["rows"]), int(size["bytes"]), columns, object_id)

    def histogram(self, metadata, column):
        if not column.stats_id:
            return []
        conversion = "CONVERT(nvarchar(128), range_high_key)"
        if column.sql_type in DATE_TYPES:
            conversion = "CONVERT(nvarchar(128), CAST(range_high_key AS datetime2), 126)"
        try:
            rows = self.query(f"""
                SELECT {conversion} AS high, range_rows, equal_rows
                FROM sys.dm_db_stats_histogram({metadata.object_id}, {column.stats_id})
                ORDER BY step_number
            """)
            return [HistogramStep(parse_value(r["high"], column), float(r["range_rows"]), float(r["equal_rows"])) for r in rows]
        except Exception:
            self.warnings.append("Histogram unavailable; planner can use ordered index seeks. Histogram access requires metadata permissions.")
            return []

    def bounds(self, column):
        name = identifier(column.name)
        conversion = f"CONVERT(nvarchar(128), {name})"
        if column.sql_type in DATE_TYPES:
            conversion = f"CONVERT(nvarchar(128), CAST({name} AS datetime2), 126)"
        # Two ordered index seeks avoid aggregate/full scans for MIN and MAX.
        values = []
        for direction in ("ASC", "DESC"):
            row = self.query(f"SELECT TOP (1) {conversion} AS value FROM {self.qualified_table} "
                             f"WHERE {name} IS NOT NULL ORDER BY {name} {direction}")
            values.append(parse_value(row[0]["value"], column) if row else None)
        return tuple(values)

    def plan(self):
        metadata = self.metadata()
        self._metadata = metadata
        preliminary = plan_read(metadata, self.planner_config)
        if preliminary.requested_partitions == 1:
            preliminary.warnings.extend(self.warnings)
            return metadata, preliminary
        column = select_column(metadata.columns)
        if column is None:
            preliminary.warnings.extend(self.warnings)
            return metadata, preliminary
        histogram = self.histogram(metadata, column)
        bounds = self.bounds(column) if not histogram else None
        return metadata, plan_read(metadata, self.planner_config, column, histogram, bounds, self.warnings)

    def read(self, plan):
        self._resolved_fetch_size = plan.fetch_size
        if self.config.read_mode == "mssql_arrow":
            # Metadata discovery is complete. Do not occupy an extra connection
            # while the executor partitions consume the configured budget.
            self.close()
            return self._read_arrow(plan)
        properties = {**self.properties, "fetchsize": str(plan.fetch_size)}
        return self.spark.read.jdbc(
            url=self.url,
            table=self.qualified_table,
            predicates=plan.predicates,
            properties=properties,
        )

    def reader_metrics(self):
        if self.config.read_mode != "mssql_arrow":
            return {
                "mode": "jdbc",
                "instrumentation": "spark_jvm_scan",
                "measurement_note": (
                    "JDBC source and Delta sink are pipelined by Spark; read_write is "
                    "their production end-to-end wall time."
                ),
                "fetch_size": self._resolved_fetch_size,
            }
        from .mssql_arrow import summarize_task_metrics

        return {
            "mode": "mssql_arrow",
            "fetch_size": self._arrow_metrics["fetch_size"],
            "target_batch_bytes": self._arrow_metrics["target_batch_bytes"],
            **summarize_task_metrics(self._arrow_metrics["accumulator"].value),
        }

    def write_pyarrow_parquet(self, plan, destination_uri, compression):
        """Execute ranges in Spark tasks and return immutable Parquet manifests."""
        from company_ingestion.writers.pyarrow_parquet import write_partitions

        _, projection = self._arrow_schema_and_projection()
        self.close()
        source = asdict(self.config)
        source["qualified_table"] = self.qualified_table
        writer = partial(
            write_partitions,
            source=source,
            predicates=tuple(plan.predicates),
            projection=projection,
            batch_size=plan.fetch_size,
            destination_uri=destination_uri,
            compression=compression,
        )
        return (
            self.spark.sparkContext.parallelize(
                range(plan.partitions), plan.partitions
            )
            .mapPartitions(writer)
            .collect()
        )

    def _read_arrow(self, plan):
        from .mssql_arrow import (
            TaskMetricsAccumulatorParam,
            extract_batches,
        )

        schema, projection = self._arrow_schema_and_projection()
        task_metrics = self.spark.sparkContext.accumulator(
            {}, TaskMetricsAccumulatorParam()
        )
        self._arrow_metrics = {
            "accumulator": task_metrics,
            "fetch_size": plan.fetch_size,
            "target_batch_bytes": plan.target_fetch_batch_bytes,
        }
        source = asdict(self.config)
        source["qualified_table"] = self.qualified_table
        reader = partial(
            extract_batches,
            source=source,
            predicates=tuple(plan.predicates),
            projection=projection,
            batch_size=plan.fetch_size,
            metrics_accumulator=task_metrics,
        )
        # Range creates exactly one lightweight control row per desired source
        # partition without a shuffle. The business data never enters this frame.
        controls = self.spark.range(
            0, plan.partitions, 1, numPartitions=plan.partitions
        ).selectExpr("CAST(id AS INT) AS chunk_id")
        return controls.mapInArrow(reader, schema)

    def _arrow_schema_and_projection(self):
        """Return a Spark schema and deterministic SQL projection for Arrow."""
        from pyspark.sql.types import (
            BinaryType,
            BooleanType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
            StringType,
            StructField,
            StructType,
            TimestampNTZType,
            TimestampType,
        )

        if self._metadata is None:
            raise RuntimeError("Source metadata must be planned before building the Arrow scan")
        metadata = self._metadata
        fields = []
        expressions = []
        for column in metadata.columns:
            name = identifier(column.name)
            sql_type = column.sql_type.lower()
            expression = name
            if sql_type == "bigint":
                spark_type = LongType()
            elif sql_type == "int":
                spark_type = IntegerType()
            elif sql_type == "smallint":
                spark_type = ShortType()
            elif sql_type == "tinyint":
                # SQL Server tinyint is unsigned while Spark ByteType is signed.
                spark_type = ShortType()
                expression = f"CAST({name} AS smallint)"
            elif sql_type in {"decimal", "numeric"}:
                spark_type = DecimalType(column.precision, column.scale)
            elif sql_type == "money":
                spark_type = DecimalType(19, 4)
            elif sql_type == "smallmoney":
                spark_type = DecimalType(10, 4)
            elif sql_type == "float":
                spark_type = DoubleType()
            elif sql_type == "real":
                spark_type = FloatType()
            elif sql_type == "bit":
                spark_type = BooleanType()
            elif sql_type in {"char", "varchar", "nchar", "nvarchar", "xml", "uniqueidentifier"}:
                spark_type = StringType()
            elif sql_type in {"text", "ntext"}:
                spark_type = StringType()
                expression = f"CAST({name} AS nvarchar(max))"
            elif sql_type in {"binary", "varbinary", "timestamp", "rowversion"}:
                spark_type = BinaryType()
            elif sql_type == "image":
                spark_type = BinaryType()
                expression = f"CAST({name} AS varbinary(max))"
            elif sql_type == "date":
                spark_type = DateType()
            elif sql_type in {"datetime", "datetime2", "smalldatetime"}:
                # SQL Server values have no zone. mssql-python exposes a
                # timezone-free Arrow timestamp, which Spark 4 maps to NTZ.
                spark_type = TimestampNTZType()
            elif sql_type == "datetimeoffset":
                spark_type = TimestampType()
            elif sql_type == "time":
                # Spark has no portable SQL time-of-day type across supported
                # runtimes, so preserve its exact textual representation.
                spark_type = StringType()
                expression = f"CONVERT(nvarchar(32), {name})"
            else:
                raise ValueError(
                    f"mssql_arrow does not support SQL Server type {sql_type!r} "
                    f"for column {column.name!r}; use jdbc or an explicit source view"
                )
            fields.append(StructField(column.name, spark_type, column.nullable))
            expressions.append(f"{expression} AS {name}")
        return StructType(fields), ", ".join(expressions)
