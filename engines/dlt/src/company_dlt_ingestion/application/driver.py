"""One-run driver: plan, launch workers, validate and publish."""

from datetime import datetime, timezone
import json
import os
import time

from .. import __version__
from ..bootstrap.composition import assemble
from .config import DistributedConfig
from ..infrastructure.kubernetes import WorkerJobClient, worker_job_manifest
from ..infrastructure.metrics import push, push_progress
from ..core.serialization import content_hash
from .planning import build_plan
from .state import completed_chunk_count, request_uri, result_uri, select_manifests


def load_config():
    return DistributedConfig(json.loads(os.environ["COMPANY_JOB_CONFIG"]))


def run_driver(config=None):
    config = config or load_config()
    components = assemble(config)
    store = components.store
    started = time.perf_counter()
    if store.exists(result_uri(config)):
        result = store.read_json(result_uri(config))
        print("COMPANY_INGESTION_RESULT=" + json.dumps(result), flush=True)
        return result
    request = {key: value for key, value in config.raw.items() if key != "run_id"}
    request["run_id"] = config.run_id
    request["request_hash"] = content_hash(request)
    store.write_immutable_json(request_uri(config), request)
    durations = {}
    try:
        phase = time.perf_counter()
        plan = build_plan(config, store, components.source)
        durations["planning"] = time.perf_counter() - phase
        print("COMPANY_INGESTION_PLAN=" + json.dumps(plan), flush=True)
        phase = time.perf_counter()
        jobs = WorkerJobClient(config.orchestration["namespace"])
        parent_uid = jobs.parent_uid(config.run_id)
        worker_job = jobs.submit(worker_job_manifest(config, plan, parent_uid))
        worker_name = worker_job["metadata"]["name"]
        print(
            f"Worker Job {worker_name}: chunks={plan['chunk_count']} "
            f"persistent_workers={plan['parallelism']}", flush=True,
        )

        def report_progress(active_workers, _succeeded, _failed):
            try:
                completed = completed_chunk_count(config, store, plan)
                running = min(active_workers, max(0, plan["chunk_count"] - completed))
                push_progress(config, plan, running, completed, 0)
            except Exception as error:
                print(
                    "Progress observability push unavailable: "
                    f"{type(error).__name__}", flush=True,
                )

        jobs.wait(
            worker_name,
            int(config.orchestration.get("timeout_seconds", 7200)),
            on_progress=report_progress,
        )
        durations["extraction"] = time.perf_counter() - phase
        phase = time.perf_counter()
        manifests = select_manifests(config, store, plan)
        staging_data_bytes = sum(
            item["size"] for item in store.list(config.staging_uri)
            if item["key"].endswith(".parquet")
        )
        durations["validation"] = time.perf_counter() - phase
        phase = time.perf_counter()
        published = components.publisher.publish(config, store, plan, manifests)
        durations["publication"] = time.perf_counter() - phase
        durations["engine_total"] = time.perf_counter() - started
        extraction_seconds = max(durations["extraction"], 1e-9)
        publication_data_bytes_written = (
            published["output_bytes"]
            if published["publication_mode"] == "rewrite" else 0
        )
        result = {
            "engine": "dlt-distributed",
            "engine_version": __version__,
            "run_id": config.run_id,
            "profile": config.profile,
            "table": config.source["table"],
            "database": config.source["database"],
            "schema": config.source["schema"],
            "destination": config.final_uri,
            "worker_job": worker_name,
            "execution_model": "persistent-workers-v1",
            "components": {
                "source": config.source["type"],
                "reader": config.execution.get("extract_backend", "mssql_arrow"),
                "encoder": config.execution.get("encoder", "dlt_parquet"),
                "store": config.storage.get("type", "s3"),
                "publisher": config.destination.get("format", "delta"),
            },
            "extract_backend": config.execution.get("extract_backend", "mssql_arrow"),
            "worker_count": plan["parallelism"],
            "plan": plan,
            "chunks": manifests,
            "rows": published["rows"],
            "readback_rows": published["rows"],
            "output_bytes": published["output_bytes"],
            "staging_data_bytes": staging_data_bytes,
            "publication_data_bytes_written": publication_data_bytes_written,
            "data_write_amplification_ratio": (
                staging_data_bytes + publication_data_bytes_written
            ) / max(published["output_bytes"], 1),
            "files": published["files"],
            "delta_version": published["delta_version"],
            "publication_mode": published["publication_mode"],
            "source_estimated_rows": plan["estimated_rows"],
            "source_estimated_bytes": plan["estimated_source_bytes"],
            "column_count": plan["column_count"],
            "retries": sum(
                max(0, len(values) - 1)
                for values in jobs.attempts(worker_name).values()
            ),
            "throughput_rows_per_second": published["rows"] / extraction_seconds,
            "throughput_output_bytes_per_second": (
                published["output_bytes"] / extraction_seconds
            ),
            "durations_seconds": durations,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "success": 1,
        }
        store.write_immutable_json(result_uri(config), result)
        store.write_json(config.metrics_uri, result)
        try:
            push(result, config.metrics.get("pushgateway"))
        except Exception as error:
            print(f"Observability push unavailable: {type(error).__name__}", flush=True)
        print("COMPANY_INGESTION_RESULT=" + json.dumps(result), flush=True)
        return result
    except Exception as error:
        failure = {
            "run_id": config.run_id,
            "engine": "dlt-distributed",
            "error_type": type(error).__name__,
            "message": str(error),
            "elapsed_seconds": time.perf_counter() - started,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            store.write_json(f"{config.control_uri}/state/failed.json", failure)
        except Exception as state_error:
            print(
                "Could not persist failure state: "
                f"{type(state_error).__name__}: {state_error}", flush=True,
            )
        raise
