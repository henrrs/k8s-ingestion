"""SQL Server source plugin: catalog, adaptive partitioner and batch readers."""

import time

from company_ingestion_core.planner import (
    desired_chunk_count,
    identifier,
    plan_distributed_read,
    select_column,
)
from .sqlserver_arrow import connect as arrow_connect
from .sqlserver_catalog import (
    SqlServerSqlAlchemyCatalog,
    sqlalchemy_url,
)


def sqlalchemy_engine(source_config):
    import sqlalchemy as sa

    return sa.create_engine(
        sqlalchemy_url(source_config), pool_pre_ping=True, pool_size=1,
        max_overflow=0,
    )


class SqlServerPartitioner:
    """Turns catalog metadata and statistics into disjoint SQL predicates."""

    def __init__(self, engine, source_config):
        self.engine = engine
        self.catalog = SqlServerSqlAlchemyCatalog(engine, source_config)

    def plan(self, planner_config):
        try:
            metadata = self.catalog.metadata()
            if desired_chunk_count(metadata, planner_config) == 1:
                return metadata, plan_distributed_read(
                    metadata, planner_config, warnings=self.catalog.warnings
                )
            column = select_column(metadata.columns)
            if column is None:
                return metadata, plan_distributed_read(
                    metadata, planner_config, warnings=self.catalog.warnings
                )
            histogram = self.catalog.histogram(metadata, column)
            bounds = None if histogram else self.catalog.bounds(column)
            return metadata, plan_distributed_read(
                metadata,
                planner_config,
                column=column,
                histogram=histogram,
                bounds=bounds,
                warnings=self.catalog.warnings,
            )
        finally:
            self.engine.dispose()


class _Reader:
    def __init__(self, source_config):
        self.source = source_config
        self.qualified = (
            f"{identifier(source_config['schema'])}."
            f"{identifier(source_config['table'])}"
        )
        self.durations = {
            "source_connect": 0.0,
            "source_execute": 0.0,
            "source_fetch": 0.0,
            "row_materialization": 0.0,
        }


class SqlServerArrowReader(_Reader):
    columnar = True

    def __init__(self, source_config):
        super().__init__(source_config)
        self.client = arrow_connect(source_config)

    def batches(self, predicate, fetch_size):
        phase = time.perf_counter()
        cursor = self.client.cursor()
        self.durations["source_connect"] += time.perf_counter() - phase
        try:
            phase = time.perf_counter()
            cursor.execute(f"SELECT * FROM {self.qualified} WHERE {predicate}")
            self.durations["source_execute"] += time.perf_counter() - phase
            reader = iter(cursor.arrow_reader(batch_size=fetch_size))
            while True:
                phase = time.perf_counter()
                try:
                    batch = next(reader)
                except StopIteration:
                    self.durations["source_fetch"] += time.perf_counter() - phase
                    return
                self.durations["source_fetch"] += time.perf_counter() - phase
                yield batch
        finally:
            cursor.close()

    def close(self):
        self.client.close()


class SqlServerRowReader(_Reader):
    columnar = False

    def __init__(self, source_config):
        super().__init__(source_config)
        self.engine = sqlalchemy_engine(source_config)

    def batches(self, predicate, fetch_size):
        import sqlalchemy as sa

        phase = time.perf_counter()
        with self.engine.connect().execution_options(
            stream_results=True
        ) as connection:
            self.durations["source_connect"] += time.perf_counter() - phase
            phase = time.perf_counter()
            result = connection.execute(
                sa.text(f"SELECT * FROM {self.qualified} WHERE {predicate}")
            )
            self.durations["source_execute"] += time.perf_counter() - phase
            while True:
                phase = time.perf_counter()
                rows = result.mappings().fetchmany(fetch_size)
                self.durations["source_fetch"] += time.perf_counter() - phase
                if not rows:
                    return
                phase = time.perf_counter()
                values = [dict(row) for row in rows]
                self.durations["row_materialization"] += (
                    time.perf_counter() - phase
                )
                yield values

    def close(self):
        self.engine.dispose()


class SqlServerSourceAdapter:
    name = "sqlserver"
    default_backend = "mssql_arrow"

    def __init__(self, source_config):
        missing = [
            key for key in ("host", "database", "schema", "table")
            if not source_config.get(key)
        ]
        if missing:
            raise ValueError(
                "SQL Server source requires: " + ", ".join(missing)
            )
        self.config = source_config

    def partitioner(self):
        return SqlServerPartitioner(
            sqlalchemy_engine(self.config), self.config
        )

    def reader(self, backend, plan=None):
        readers = {
            "mssql_arrow": SqlServerArrowReader,
            "sqlalchemy_rows": SqlServerRowReader,
        }
        try:
            reader = readers[backend]
        except KeyError:
            raise ValueError(
                "SQL Server extract_backend must be mssql_arrow or "
                "sqlalchemy_rows"
            ) from None
        return reader(self.config)
