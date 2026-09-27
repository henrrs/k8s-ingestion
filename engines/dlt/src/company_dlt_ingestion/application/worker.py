"""Persistent worker use case: read, encode and checkpoint owned chunks."""

from datetime import datetime, timezone
import json
import os
import time

from ..bootstrap.composition import assemble
from .config import DistributedConfig
from ..infrastructure.metrics import push_chunk, push_progress
from .state import (
    assigned_chunks,
    attempt_manifest_uri,
    chunk_manifests,
    completed_chunk_count,
    plan_uri,
)


def extract_chunk(
    config, store, plan, chunk, worker_index, pod_uid, reader, encoder,
):
    started = time.perf_counter()
    cpu_started = time.process_time()
    index = int(chunk["index"])
    predicate = chunk["predicate"]
    attempt_uri = (
        f"{config.staging_uri}/chunks/{index:05d}/attempts/{pod_uid}"
    )
    before = dict(reader.durations)
    encoded = encoder.encode(
        resource_name=config.source["table"],
        batch_supplier=lambda: reader.batches(
            predicate, int(plan["fetch_size"])
        ),
        attempt_uri=attempt_uri,
        pipeline_name=f"{config.run_id}_{index}_{pod_uid}",
        store=store,
    )
    source_durations = {
        name: reader.durations.get(name, 0.0) - before.get(name, 0.0)
        for name in reader.durations
    }
    phase_seconds = {**source_durations, **encoded.durations}
    source_seconds = sum(source_durations.values())
    transform_phase = (
        "dlt_arrow_parquet_upload"
        if getattr(reader, "columnar", False)
        else "dlt_normalize_parquet_upload"
    )
    phase_seconds[transform_phase] = max(
        0.0, encoded.durations["pipeline_run"] - source_seconds
    )
    manifest = {
        "version": 1,
        "run_id": config.run_id,
        "index": index,
        "worker_index": worker_index,
        "pod_uid": pod_uid,
        "predicate": predicate,
        "rows": encoded.rows,
        "bytes": sum(item["size"] for item in encoded.files),
        "files": encoded.files,
        "load_ids": encoded.load_ids,
        "duration_seconds": time.perf_counter() - started,
        "cpu_seconds": time.process_time() - cpu_started,
        "durations_seconds": phase_seconds,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    store.write_immutable_json(
        attempt_manifest_uri(config, index, pod_uid), manifest
    )
    try:
        push_chunk(config, manifest)
        completed = completed_chunk_count(config, store, plan)
        push_progress(
            config,
            plan,
            min(plan["parallelism"], plan["chunk_count"] - completed),
            completed,
            0,
        )
    except Exception as error:
        print(
            f"Chunk observability push unavailable: {type(error).__name__}",
            flush=True,
        )
    print("COMPANY_CHUNK_RESULT=" + json.dumps(manifest), flush=True)
    return manifest


def run_worker(config=None):
    config = config or DistributedConfig(
        json.loads(os.environ["COMPANY_JOB_CONFIG"])
    )
    components = assemble(config)
    started = time.perf_counter()
    worker_index = int(os.environ["JOB_COMPLETION_INDEX"])
    pod_uid = os.environ["POD_UID"]
    plan = components.store.read_json(plan_uri(config))
    chunks = assigned_chunks(plan, worker_index)
    backend = config.execution.get("extract_backend") or (
        components.source.default_backend
    )
    reader = components.source.reader(backend, plan)
    processed = []
    skipped = []
    print(
        f"Worker {worker_index}/{plan['parallelism']} owns "
        f"{len(chunks)} chunks: {[chunk['index'] for chunk in chunks]}",
        flush=True,
    )
    try:
        for chunk in chunks:
            index = int(chunk["index"])
            if chunk_manifests(config, components.store, index):
                skipped.append(index)
                print(
                    f"Worker {worker_index}: chunk {index} already "
                    "checkpointed; skipping",
                    flush=True,
                )
                continue
            processed.append(
                extract_chunk(
                    config,
                    components.store,
                    plan,
                    chunk,
                    worker_index,
                    pod_uid,
                    reader,
                    components.encoder,
                )
            )
    finally:
        reader.close()
    result = {
        "run_id": config.run_id,
        "worker_index": worker_index,
        "pod_uid": pod_uid,
        "assigned_chunks": [int(chunk["index"]) for chunk in chunks],
        "processed_chunks": [item["index"] for item in processed],
        "skipped_chunks": skipped,
        "rows": sum(item["rows"] for item in processed),
        "duration_seconds": time.perf_counter() - started,
    }
    print("COMPANY_WORKER_RESULT=" + json.dumps(result), flush=True)
    return result
