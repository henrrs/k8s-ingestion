"""Executor-side SQL Server Arrow reader used by Spark ``mapInArrow``.

Only immutable connection metadata is serialized with the Spark closure. User
and password values are resolved from executor environment variables.
"""

from __future__ import annotations

import os
import time


class TaskMetricsAccumulatorParam:
    """Merge one terminal metric document per Spark task attempt."""

    def zero(self, _value):
        return {}

    def addInPlace(self, current, update):
        merged = dict(current)
        merged.update(update)
        return merged


def _odbc_value(value):
    return "{" + str(value).replace("}", "}}") + "}"


def _connection_string(source):
    try:
        user = os.environ[source["user_env"]]
        password = os.environ[source["password_env"]]
    except KeyError as error:
        raise ValueError(
            f"Missing source credential environment variable: {error.args[0]}"
        ) from None
    server = f"{source['host']},{source['port']}"
    return ";".join(
        [
            f"Server={_odbc_value(server)}",
            f"Database={_odbc_value(source['database'])}",
            f"UID={_odbc_value(user)}",
            f"PWD={_odbc_value(password)}",
            f"Encrypt={'yes' if source['encrypt'] else 'no'}",
            "TrustServerCertificate="
            + ("yes" if source["trust_server_certificate"] else "no"),
        ]
    )


def extract_batches(
    control_batches,
    *,
    source,
    predicates,
    projection,
    batch_size,
    metrics_accumulator,
):
    """Turn tiny control batches into SQL Server Arrow result batches."""
    import mssql_python
    from pyspark import TaskContext

    context = TaskContext.get()
    partition_id = context.partitionId() if context else -1
    attempt_number = context.attemptNumber() if context else 0
    task_attempt_id = context.taskAttemptId() if context else -1
    task_key = f"{partition_id}:{attempt_number}:{task_attempt_id}"
    metrics = {
        "partition_id": partition_id,
        "attempt_number": attempt_number,
        "task_attempt_id": task_attempt_id,
        "chunks": 0,
        "batches": 0,
        "rows": 0,
        "arrow_bytes": 0,
        "source_connect_seconds": 0.0,
        "source_execute_seconds": 0.0,
        "source_fetch_seconds": 0.0,
        "arrow_consumer_wait_seconds": 0.0,
        "started_at_epoch": time.time(),
    }
    task_started = time.perf_counter()
    try:
        for control_batch in control_batches:
            for chunk_id in control_batch.column(0).to_pylist():
                chunk_id = int(chunk_id)
                metrics["chunks"] += 1
                phase = time.perf_counter()
                connection = mssql_python.connect(
                    _connection_string(source),
                    timeout=source["query_timeout_seconds"],
                )
                metrics["source_connect_seconds"] += time.perf_counter() - phase
                cursor = connection.cursor()
                reader = None
                try:
                    phase = time.perf_counter()
                    cursor.execute(
                        f"SELECT {projection} FROM {source['qualified_table']} "
                        f"WHERE {predicates[chunk_id]}"
                    )
                    metrics["source_execute_seconds"] += time.perf_counter() - phase
                    reader = cursor.arrow_reader(batch_size=batch_size)
                    iterator = iter(reader)
                    while True:
                        phase = time.perf_counter()
                        try:
                            batch = next(iterator)
                        except StopIteration:
                            metrics["source_fetch_seconds"] += (
                                time.perf_counter() - phase
                            )
                            break
                        metrics["source_fetch_seconds"] += time.perf_counter() - phase
                        metrics["batches"] += 1
                        metrics["rows"] += batch.num_rows
                        metrics["arrow_bytes"] += batch.nbytes
                        handoff_started = time.perf_counter()
                        yield batch
                        metrics["arrow_consumer_wait_seconds"] += (
                            time.perf_counter() - handoff_started
                        )
                finally:
                    if reader is not None:
                        reader.close()
                    cursor.close()
                    connection.close()
    finally:
        metrics["finished_at_epoch"] = time.time()
        metrics["task_wall_seconds"] = time.perf_counter() - task_started
        metrics_accumulator.add({task_key: metrics})


def summarize_task_metrics(tasks):
    """Aggregate executor metrics while preserving their concurrency semantics."""
    values = list(tasks.values())
    if not values:
        return {
            "instrumentation": "spark_accumulator",
            "tasks": 0,
            "chunks": 0,
            "batches": 0,
            "rows": 0,
            "arrow_bytes": 0,
            "durations_seconds": {},
        }
    duration_names = (
        "source_connect_seconds",
        "source_execute_seconds",
        "source_fetch_seconds",
        "arrow_consumer_wait_seconds",
        "task_wall_seconds",
    )
    durations = {
        name + "_sum": sum(float(value[name]) for value in values)
        for name in duration_names
    }
    durations.update(
        {
            "task_wall_max": max(float(value["task_wall_seconds"]) for value in values),
            "source_pipeline_span": max(value["finished_at_epoch"] for value in values)
            - min(value["started_at_epoch"] for value in values),
        }
    )
    rows = sum(int(value["rows"]) for value in values)
    arrow_bytes = sum(int(value["arrow_bytes"]) for value in values)
    span = max(durations["source_pipeline_span"], 1e-9)
    return {
        "instrumentation": "spark_accumulator_successful_task_attempts",
        "measurement_note": (
            "Arrow bytes are uncompressed in-memory batch bytes, not TDS network bytes. "
            "Sum durations are executor time and overlap across concurrent tasks; "
            "source_pipeline_span is wall-clock span."
        ),
        "tasks": len(values),
        "chunks": sum(int(value["chunks"]) for value in values),
        "batches": sum(int(value["batches"]) for value in values),
        "rows": rows,
        "arrow_bytes": arrow_bytes,
        "throughput_rows_per_second": rows / span,
        "throughput_arrow_bytes_per_second": arrow_bytes / span,
        "durations_seconds": durations,
        "task_attempts": values,
    }
