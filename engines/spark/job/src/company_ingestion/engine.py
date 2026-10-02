"""Reusable source ingestion engine; table identities are execution parameters."""
from datetime import datetime, timezone
import json
import os
import shutil
import time

from . import __version__
from .metrics import push, write_json
from .sources.sqlserver import SqlServerSource

SOURCES = {"sqlserver": SqlServerSource}

def run(config):
    from pyspark.sql import SparkSession
    started = time.perf_counter()
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if config.source.read_mode == "mssql_arrow":
        wheel_path = os.getenv("COMPANY_WHEEL_LOCAL_PATH")
        if wheel_path:
            # Spark only adds .zip/.egg/.py artifacts to the executor Python
            # path. A wheel is already a ZIP archive, so preserve its verified
            # bytes under a recognized suffix before distributing it.
            executor_bundle = wheel_path + ".zip"
            shutil.copyfile(wheel_path, executor_bundle)
            spark.sparkContext.addPyFile(executor_bundle)
    result = {"engine": "spark", "run_id": config.run_id, "profile": config.profile, "table": config.source.table,
              "database": config.source.database, "schema": config.source.schema,
              "read_mode": config.source.read_mode,
              "started_at": datetime.now(timezone.utc).isoformat(),
              "spark_application_id": spark.sparkContext.applicationId, "spark_version": spark.version,
              "engine_version": __version__, "runtime_version": os.getenv("COMPANY_RUNTIME_VERSION"),
              "wheel_sha256": os.getenv("COMPANY_WHEEL_DIGEST"), "destination": config.run_uri,
              "source_bytes_definition": "SQL Server allocated used pages (or catalog width fallback), an estimate; not TDS/network bytes",
              "output_bytes_definition": "Committed Delta data file sizes, compressed, excluding transaction log",
              "throughput_definition": "Committed rows / timed source-read-and-Delta-write pipeline; excludes startup, planning and readback",
              "success": 0}
    source = None
    try:
        plan_started = time.perf_counter()
        source = SOURCES[config.source.type](spark, config.source, config.planner)
        metadata, plan = source.plan()
        result["plan"] = plan.to_dict()
        planning_seconds = time.perf_counter() - plan_started
        print("COMPANY_INGESTION_PLAN=" + json.dumps(result["plan"], default=str), flush=True)
        frame = source.read(plan)
        result["column_count"] = len(frame.columns)
        materialize_seconds = 0.0
        materialized_rows = None
        if config.benchmark.isolate_io_phases:
            from pyspark import StorageLevel

            frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
            materialize_started = time.perf_counter()
            materialized_rows = frame.count()
            materialize_seconds = time.perf_counter() - materialize_started
        # Each run owns a new Delta table path. Delta's transaction log publishes
        # one atomic snapshot; no cross-driver writes to the same table in the lab.
        write_started = time.perf_counter()
        frame.write.format("delta").mode("errorifexists").save(config.run_uri)
        delta_write_seconds = time.perf_counter() - write_started
        pipeline_seconds = materialize_seconds + delta_write_seconds
        reader_metrics = source.reader_metrics()
        metrics_started = time.perf_counter()
        quoted_path = config.run_uri.replace("`", "``")
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{quoted_path}`").first().asDict()
        history = spark.sql(f"DESCRIBE HISTORY delta.`{quoted_path}` LIMIT 1").first().asDict()
        operation = history.get("operationMetrics") or {}
        rows = int(operation["numOutputRows"])
        if materialized_rows is not None and materialized_rows != rows:
            raise RuntimeError(
                f"Materialized source row count mismatch: {materialized_rows} != {rows}"
            )
        output_bytes = int(detail["sizeInBytes"])
        collect_seconds = time.perf_counter() - metrics_started
        verify_started = time.perf_counter()
        # Distinct from timed write: recompute a content checksum from committed
        # files, forcing a read of every column instead of a metadata-only count.
        from pyspark.sql import functions as F
        readback = spark.read.format("delta").load(config.run_uri)
        checksum = readback.select(F.xxhash64(*[F.col("`" + c.replace("`", "``") + "`") for c in readback.columns]).alias("hash"))
        validation = checksum.agg(F.count("*").alias("rows"), F.sum(F.col("hash").cast("decimal(38,0)")).alias("checksum")).first()
        if validation["rows"] != rows:
            raise RuntimeError(f"Delta readback mismatch: {validation['rows']} != {rows}")
        verify_seconds = time.perf_counter() - verify_started
        durations = {"planning": planning_seconds,
                     "read_write": pipeline_seconds,
                     "metrics_collection": collect_seconds,
                     "readback": verify_seconds,
                     "engine_total": time.perf_counter() - started}
        if config.benchmark.isolate_io_phases:
            durations.update({"source_materialization": materialize_seconds,
                              "delta_write_from_cache": delta_write_seconds})
        else:
            durations["streaming_source_and_delta_write"] = delta_write_seconds
        result.update({"rows": rows, "output_bytes": output_bytes, "files": int(detail["numFiles"]),
                       "source_estimated_bytes": metadata.estimated_bytes, "source_estimated_rows": metadata.estimated_rows,
                       "partitions": plan.partitions, "readback_rows": validation["rows"], "readback_checksum": str(validation["checksum"]),
                       "delta_version": int(history["version"]), "delta_operation_metrics": operation,
                       "throughput_rows_per_second": rows / max(pipeline_seconds, 1e-9),
                       "throughput_output_bytes_per_second": output_bytes / max(pipeline_seconds, 1e-9),
                       "reader_metrics": reader_metrics,
                       "benchmark": {"isolate_io_phases": config.benchmark.isolate_io_phases,
                                     "materialized_rows": materialized_rows,
                                     "measurement_note": (
                                         "When isolate_io_phases=true, source_materialization includes Spark cache "
                                         "encoding and the production pipeline is intentionally changed."
                                     )},
                       "durations_seconds": durations, "success": 1})
        if config.benchmark.isolate_io_phases:
            frame.unpersist(blocking=False)
        write_json(spark, f"{config.metrics.output_uri.rstrip('/')}/{config.run_id}.json", result)
        try:
            push(result, config.metrics.pushgateway)
        except Exception as error:
            print(f"Observability push unavailable: {type(error).__name__}; durable summary remains in storage", flush=True)
        print("COMPANY_INGESTION_RESULT=" + json.dumps(result, default=str), flush=True)
        return result
    except Exception as error:
        # Spark/Airflow preserve the traceback. Do not serialize connection secrets.
        result.update({"error_type": type(error).__name__, "elapsed_seconds": time.perf_counter() - started})
        try:
            write_json(spark, f"{config.metrics.output_uri.rstrip('/')}/{config.run_id}.failed.json", result)
        except Exception:
            pass
        raise
    finally:
        if source is not None:
            source.close()
        spark.stop()
