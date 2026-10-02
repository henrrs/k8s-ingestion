"""Reusable source ingestion engine; table identities are execution parameters."""
from datetime import datetime, timezone
import json
import math
import os
import shutil
import time

from . import __version__
from .metrics import push, write_json
from .sources.sqlserver import SqlServerSource

SOURCES = {"sqlserver": SqlServerSource}


def _select_output_partitions(frame, destination, estimated_source_bytes):
    current = frame.rdd.getNumPartitions()
    if destination.output_partitions is not None:
        desired = destination.output_partitions
    elif destination.target_file_bytes is not None:
        estimated_output = max(
            1, math.ceil(
                estimated_source_bytes * destination.estimated_compression_ratio
            ),
        )
        desired = max(1, math.ceil(estimated_output / destination.target_file_bytes))
    else:
        desired = current
    if desired < current:
        return frame.coalesce(desired), current, desired, "coalesce"
    if desired > current:
        return frame.repartition(desired), current, desired, "repartition"
    return frame, current, desired, "preserve"


def _data_file_stats(spark, uri):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    files = path.getFileSystem(spark._jsc.hadoopConfiguration()).listFiles(path, True)
    count = 0
    size = 0
    while files.hasNext():
        status = files.next()
        if status.getPath().getName().endswith(".parquet"):
            count += 1
            size += status.getLen()
    return count, size


def _validate_output(spark, output_format, uri):
    from pyspark.sql import functions as F

    frame = spark.read.format(output_format).load(uri)
    checksum = frame.select(
        F.xxhash64(
            *[F.col("`" + column.replace("`", "``") + "`") for column in frame.columns]
        ).alias("hash")
    )
    validation = checksum.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("hash").cast("decimal(38,0)")).alias("checksum"),
    ).first()
    return int(validation["rows"]), str(validation["checksum"])


def _write_variant(spark, frame, *, name, output_format, compression,
                   output_partitions, base_uri):
    current = frame.rdd.getNumPartitions()
    if output_partitions < current:
        output = frame.coalesce(output_partitions)
        partition_operation = "coalesce"
    elif output_partitions > current:
        output = frame.repartition(output_partitions)
        partition_operation = "repartition"
    else:
        output = frame
        partition_operation = "preserve"
    uri = f"{base_uri.rstrip('/')}/{name}"
    started = time.perf_counter()
    (
        output.write.format(output_format)
        .option("compression", compression)
        .mode("errorifexists")
        .save(uri)
    )
    write_seconds = time.perf_counter() - started
    files, output_bytes = _data_file_stats(spark, uri)
    validation_started = time.perf_counter()
    rows, checksum = _validate_output(spark, output_format, uri)
    return {
        "name": name,
        "format": output_format,
        "compression": compression,
        "input_partitions": current,
        "output_partitions": output_partitions,
        "partition_operation": partition_operation,
        "uri": uri,
        "write_seconds": write_seconds,
        "validation_seconds": time.perf_counter() - validation_started,
        "rows": rows,
        "checksum": checksum,
        "files": files,
        "output_bytes": output_bytes,
        "throughput_rows_per_second": rows / max(write_seconds, 1e-9),
        "throughput_bytes_per_second": output_bytes / max(write_seconds, 1e-9),
    }

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
        result["column_count"] = len(metadata.columns)
        planning_seconds = time.perf_counter() - plan_started
        print("COMPANY_INGESTION_PLAN=" + json.dumps(result["plan"], default=str), flush=True)
        if config.destination.write_mode == "pyarrow":
            if config.source.read_mode != "mssql_arrow":
                raise ValueError(
                    "destination.write_mode=pyarrow requires source.read_mode=mssql_arrow"
                )
            from .writers.pyarrow_parquet import commit_delta, summarize

            direct_started = time.perf_counter()
            manifests = source.write_pyarrow_parquet(
                plan, config.run_uri, config.destination.compression
            )
            direct_seconds = time.perf_counter() - direct_started
            reader_metrics = summarize(manifests)
            if reader_metrics["rows"] != metadata.estimated_rows:
                raise RuntimeError(
                    "Direct Parquet row count differs from the source metadata: "
                    f"{reader_metrics['rows']} != {metadata.estimated_rows}"
                )
            commit_started = time.perf_counter()
            commit_delta(config.run_uri, manifests)
            delta_commit_seconds = time.perf_counter() - commit_started
            pipeline_seconds = direct_seconds + delta_commit_seconds
            metrics_started = time.perf_counter()
            quoted_path = config.run_uri.replace("`", "``")
            detail = spark.sql(
                f"DESCRIBE DETAIL delta.`{quoted_path}`"
            ).first().asDict()
            history = spark.sql(
                f"DESCRIBE HISTORY delta.`{quoted_path}` LIMIT 1"
            ).first().asDict()
            collect_seconds = time.perf_counter() - metrics_started
            verify_started = time.perf_counter()
            rows, checksum = _validate_output(spark, "delta", config.run_uri)
            verify_seconds = time.perf_counter() - verify_started
            if rows != reader_metrics["rows"]:
                raise RuntimeError(
                    f"Delta readback mismatch: {rows} != {reader_metrics['rows']}"
                )
            output_bytes = int(detail["sizeInBytes"])
            result.update({
                "rows": rows,
                "output_bytes": output_bytes,
                "files": int(detail["numFiles"]),
                "source_estimated_bytes": metadata.estimated_bytes,
                "source_estimated_rows": metadata.estimated_rows,
                "partitions": plan.partitions,
                "readback_rows": rows,
                "readback_checksum": checksum,
                "delta_version": int(history["version"]),
                "delta_operation_metrics": history.get("operationMetrics") or {},
                "throughput_rows_per_second": rows / max(pipeline_seconds, 1e-9),
                "throughput_output_bytes_per_second": output_bytes / max(
                    pipeline_seconds, 1e-9
                ),
                "writer": {
                    "mode": "pyarrow",
                    "compression": config.destination.compression,
                    "publication": "delta_manifest_commit",
                },
                "writer_benchmark": None,
                "reader_metrics": reader_metrics,
                "benchmark": {
                    "isolate_io_phases": False,
                    "writer_matrix": False,
                    "materialized_rows": None,
                    "measurement_note": (
                        "Direct mode writes Parquet in executor Python tasks and "
                        "publishes immutable files as Delta AddActions."
                    ),
                },
                "durations_seconds": {
                    "planning": planning_seconds,
                    "read_write": pipeline_seconds,
                    "direct_extract_encode_upload": direct_seconds,
                    "delta_commit": delta_commit_seconds,
                    "metrics_collection": collect_seconds,
                    "readback": verify_seconds,
                    "engine_total": time.perf_counter() - started,
                },
                "success": 1,
            })
            write_json(
                spark,
                f"{config.metrics.output_uri.rstrip('/')}/{config.run_id}.json",
                result,
            )
            try:
                push(result, config.metrics.pushgateway)
            except Exception as error:
                print(
                    "Observability push unavailable: "
                    f"{type(error).__name__}; durable summary remains in storage",
                    flush=True,
                )
            print(
                "COMPANY_INGESTION_RESULT=" + json.dumps(result, default=str),
                flush=True,
            )
            return result
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
        writer_benchmark = None
        operation = {}
        delta_version = None
        if config.benchmark.writer_matrix:
            input_partitions = frame.rdd.getNumPartitions()
            variants = [
                ("delta_snappy_current", "delta", "snappy", input_partitions),
                ("parquet_snappy_current", "parquet", "snappy", input_partitions),
                ("delta_zstd_current", "delta", "zstd", input_partitions),
                ("delta_uncompressed_current", "delta", "uncompressed", input_partitions),
            ]
            if input_partitions > 1:
                variants.append(("delta_snappy_1_file", "delta", "snappy", 1))
            matrix_started = time.perf_counter()
            writer_benchmark = [
                _write_variant(
                    spark,
                    frame,
                    name=name,
                    output_format=output_format,
                    compression=compression,
                    output_partitions=output_partitions,
                    base_uri=config.run_uri,
                )
                for name, output_format, compression, output_partitions in variants
            ]
            matrix_seconds = time.perf_counter() - matrix_started
            baseline = writer_benchmark[0]
            expected_rows = baseline["rows"]
            expected_checksum = baseline["checksum"]
            for variant in writer_benchmark:
                if (
                    variant["rows"] != expected_rows
                    or variant["checksum"] != expected_checksum
                ):
                    raise RuntimeError(
                        f"Writer variant {variant['name']} produced different data"
                    )
            rows = expected_rows
            output_bytes = baseline["output_bytes"]
            files = baseline["files"]
            readback_rows = rows
            readback_checksum = expected_checksum
            delta_write_seconds = baseline["write_seconds"]
            collect_seconds = sum(
                variant["validation_seconds"] for variant in writer_benchmark
            )
            verify_seconds = baseline["validation_seconds"]
            output_partition_plan = {
                "input_partitions": input_partitions,
                "output_partitions": input_partitions,
                "operation": "preserve",
            }
        else:
            output, input_partitions, output_partitions, partition_operation = (
                _select_output_partitions(
                    frame, config.destination, metadata.estimated_bytes
                )
            )
            output_partition_plan = {
                "input_partitions": input_partitions,
                "output_partitions": output_partitions,
                "operation": partition_operation,
            }
            # Each run owns a new Delta table path. Delta publishes one atomic
            # snapshot; no cross-driver writes target the same path in the lab.
            write_started = time.perf_counter()
            (
                output.write.format("delta")
                .option("compression", config.destination.compression)
                .mode("errorifexists")
                .save(config.run_uri)
            )
            delta_write_seconds = time.perf_counter() - write_started
            metrics_started = time.perf_counter()
            quoted_path = config.run_uri.replace("`", "``")
            detail = spark.sql(
                f"DESCRIBE DETAIL delta.`{quoted_path}`"
            ).first().asDict()
            history = spark.sql(
                f"DESCRIBE HISTORY delta.`{quoted_path}` LIMIT 1"
            ).first().asDict()
            operation = history.get("operationMetrics") or {}
            delta_version = int(history["version"])
            rows = int(operation["numOutputRows"])
            output_bytes = int(detail["sizeInBytes"])
            files = int(detail["numFiles"])
            collect_seconds = time.perf_counter() - metrics_started
            verify_started = time.perf_counter()
            readback_rows, readback_checksum = _validate_output(
                spark, "delta", config.run_uri
            )
            verify_seconds = time.perf_counter() - verify_started
            if readback_rows != rows:
                raise RuntimeError(
                    f"Delta readback mismatch: {readback_rows} != {rows}"
                )
            matrix_seconds = None
        if materialized_rows is not None and materialized_rows != rows:
            raise RuntimeError(
                f"Materialized source row count mismatch: {materialized_rows} != {rows}"
            )
        pipeline_seconds = materialize_seconds + delta_write_seconds
        reader_metrics = source.reader_metrics()
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
        if matrix_seconds is not None:
            durations["writer_matrix_total"] = matrix_seconds
        result.update({"rows": rows, "output_bytes": output_bytes, "files": files,
                       "source_estimated_bytes": metadata.estimated_bytes, "source_estimated_rows": metadata.estimated_rows,
                       "partitions": plan.partitions, "readback_rows": readback_rows, "readback_checksum": readback_checksum,
                       "delta_version": delta_version, "delta_operation_metrics": operation,
                       "throughput_rows_per_second": rows / max(pipeline_seconds, 1e-9),
                       "throughput_output_bytes_per_second": output_bytes / max(pipeline_seconds, 1e-9),
                       "writer": {"compression": config.destination.compression,
                                  "partition_plan": output_partition_plan},
                       "writer_benchmark": writer_benchmark,
                       "reader_metrics": reader_metrics,
                       "benchmark": {"isolate_io_phases": config.benchmark.isolate_io_phases,
                                     "writer_matrix": config.benchmark.writer_matrix,
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
