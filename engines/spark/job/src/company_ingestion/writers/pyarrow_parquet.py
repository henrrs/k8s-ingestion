"""Direct SQL Server Arrow-to-Parquet writer for Spark executor tasks."""

from __future__ import annotations

import os
from pathlib import Path
import json
import shutil
import tempfile
import time
from urllib.parse import urlparse


def _s3_location(uri):
    parsed = urlparse(uri)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError("PyArrow direct writer requires an s3:// or s3a:// URI")
    return parsed.netloc, parsed.path.lstrip("/")


def _filesystem():
    from pyarrow import fs

    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://seaweedfs:8333")
    parsed = urlparse(endpoint)
    return fs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.environ.get("AWS_REGION", "us-east-1"),
        scheme=parsed.scheme or "http",
        endpoint_override=parsed.netloc or parsed.path,
        force_virtual_addressing=False,
    )


def write_partitions(chunk_ids, *, source, predicates, projection, batch_size,
                     destination_uri, compression):
    """Write deterministic Parquet objects and return one manifest per range."""
    import pyarrow.parquet as parquet
    from pyspark import TaskContext

    from company_ingestion.sources.mssql_arrow import _prefetched_chunk_batches

    context = TaskContext.get()
    partition_id = context.partitionId() if context else -1
    attempt_number = context.attemptNumber() if context else 0
    task_attempt_id = context.taskAttemptId() if context else -1
    bucket, prefix = _s3_location(destination_uri)
    filesystem = _filesystem()
    codec = None if compression == "uncompressed" else compression

    for chunk_id in chunk_ids:
        chunk_id = int(chunk_id)
        task_started = time.perf_counter()
        started_epoch = time.time()
        metrics = {
            "source_connect_seconds": 0.0,
            "source_execute_seconds": 0.0,
            "source_fetch_seconds": 0.0,
            "prefetch_queue_wait_seconds": 0.0,
            "arrow_consumer_wait_seconds": 0.0,
            "batches": 0,
            "rows": 0,
            "arrow_bytes": 0,
        }
        object_name = f"part-{chunk_id:05d}.parquet"
        object_path = "/".join(value for value in (bucket, prefix, object_name) if value)
        query = (
            f"SELECT {projection} FROM {source['qualified_table']} "
            f"WHERE {predicates[chunk_id]}"
        )
        local_handle = tempfile.NamedTemporaryFile(
            prefix=f"company-parquet-{chunk_id:05d}-", suffix=".parquet",
            delete=False,
        )
        local_path = Path(local_handle.name)
        local_handle.close()
        writer = None
        encode_seconds = 0.0
        try:
            for batch in _prefetched_chunk_batches(
                source, query, batch_size, metrics
            ):
                if writer is None:
                    encode_started = time.perf_counter()
                    writer = parquet.ParquetWriter(
                        local_path,
                        batch.schema,
                        compression=codec,
                        use_dictionary=True,
                        write_statistics=True,
                    )
                    encode_seconds += time.perf_counter() - encode_started
                consumer_started = time.perf_counter()
                writer.write_batch(batch)
                batch_encode_seconds = time.perf_counter() - consumer_started
                encode_seconds += batch_encode_seconds
                metrics["arrow_consumer_wait_seconds"] += batch_encode_seconds
            if writer is None:
                raise RuntimeError(f"Range {chunk_id} returned no Arrow batches")
            close_started = time.perf_counter()
            writer.close()
            encode_seconds += time.perf_counter() - close_started
            writer = None
            local_bytes = local_path.stat().st_size
            upload_started = time.perf_counter()
            with local_path.open("rb") as source_stream:
                with filesystem.open_output_stream(object_path) as destination:
                    shutil.copyfileobj(
                        source_stream, destination, length=8 * 1024 * 1024
                    )
            upload_seconds = time.perf_counter() - upload_started
        finally:
            if writer is not None:
                writer.close()
            local_path.unlink(missing_ok=True)
        yield {
            "chunk_id": chunk_id,
            "partition_id": partition_id,
            "attempt_number": attempt_number,
            "task_attempt_id": task_attempt_id,
            "object": f"s3://{object_path}",
            "rows": metrics["rows"],
            "batches": metrics["batches"],
            "arrow_bytes": metrics["arrow_bytes"],
            "parquet_bytes": local_bytes,
            "source_connect_seconds": metrics["source_connect_seconds"],
            "source_execute_seconds": metrics["source_execute_seconds"],
            "source_fetch_seconds": metrics["source_fetch_seconds"],
            "prefetch_queue_wait_seconds": metrics[
                "prefetch_queue_wait_seconds"
            ],
            "parquet_encode_seconds": encode_seconds,
            "upload_seconds": upload_seconds,
            "task_wall_seconds": time.perf_counter() - task_started,
            "started_at_epoch": started_epoch,
            "finished_at_epoch": time.time(),
        }


def summarize(manifests):
    duration_names = (
        "source_connect_seconds",
        "source_execute_seconds",
        "source_fetch_seconds",
        "prefetch_queue_wait_seconds",
        "parquet_encode_seconds",
        "upload_seconds",
        "task_wall_seconds",
    )
    durations = {
        name + "_sum": sum(float(item[name]) for item in manifests)
        for name in duration_names
    }
    durations["task_wall_max"] = max(
        float(item["task_wall_seconds"]) for item in manifests
    )
    durations["source_pipeline_span"] = (
        max(item["finished_at_epoch"] for item in manifests)
        - min(item["started_at_epoch"] for item in manifests)
    )
    return {
        "mode": "mssql_pyarrow_parquet",
        "instrumentation": "executor_manifests",
        "tasks": len(manifests),
        "chunks": len(manifests),
        "batches": sum(int(item["batches"]) for item in manifests),
        "rows": sum(int(item["rows"]) for item in manifests),
        "arrow_bytes": sum(int(item["arrow_bytes"]) for item in manifests),
        "parquet_bytes": sum(int(item["parquet_bytes"]) for item in manifests),
        "durations_seconds": durations,
        "task_attempts": manifests,
    }


def commit_delta(destination_uri, manifests):
    """Create a Delta snapshot by registering immutable Parquet manifests."""
    import pyarrow as pa
    import pyarrow.parquet as parquet
    from deltalake import Schema
    from deltalake.transaction import AddAction, create_table_with_add_actions

    if not manifests:
        raise RuntimeError("Cannot publish an empty direct Parquet snapshot")
    table_bucket, table_prefix = _s3_location(destination_uri)
    table_prefix = table_prefix.rstrip("/") + "/"
    filesystem = _filesystem()
    first_bucket, first_key = _s3_location(manifests[0]["object"])
    schema = parquet.read_schema(
        f"{first_bucket}/{first_key}", filesystem=filesystem
    )
    # Delta models NOT NULL as an invariant table feature. delta-rs 1.6.6 can
    # infer required Parquet fields without listing that writer feature, which
    # Spark 4.2/Delta 4.4 correctly rejects. Landing snapshots do not need NOT
    # NULL enforcement, so publish a nullable logical schema while preserving
    # the physical Parquet types.
    schema = pa.schema(
        [
            pa.field(field.name, field.type, nullable=True, metadata=field.metadata)
            for field in schema
        ],
        metadata=schema.metadata,
    )
    actions = []
    for manifest in manifests:
        bucket, key = _s3_location(manifest["object"])
        if bucket != table_bucket or not key.startswith(table_prefix):
            raise RuntimeError(
                f"Parquet object is outside the Delta table: {manifest['object']}"
            )
        actions.append(
            AddAction(
                path=key[len(table_prefix):],
                size=int(manifest["parquet_bytes"]),
                partition_values={},
                modification_time=int(manifest["finished_at_epoch"] * 1000),
                data_change=True,
                stats=json.dumps({"numRecords": int(manifest["rows"])}),
            )
        )
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://seaweedfs:8333")
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        "allow_http": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    table_uri = destination_uri.replace("s3a://", "s3://", 1)
    create_table_with_add_actions(
        table_uri,
        Schema.from_arrow(schema),
        actions,
        mode="error",
        storage_options=storage_options,
    )
