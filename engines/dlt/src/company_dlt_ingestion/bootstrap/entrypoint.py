"""Command entrypoint that selects the isolated driver or worker role."""

import os

from ..application.driver import load_config, run_driver
from ..application.planning import build_plan
from ..application.state import (
    assigned_chunks,
    attempt_manifest_uri,
    chunk_manifests,
    completed_chunk_count,
    plan_uri,
    request_uri,
    result_uri,
    selected_uri,
    select_manifests,
)
from ..application.worker import extract_chunk, run_worker
from ..plugins.publishers.delta import relative_delta_path

driver = run_driver
dlt_worker = run_worker
_extract_chunk = extract_chunk
_relative_delta_path = relative_delta_path


def main(role=None):
    role = role or os.environ.get("COMPANY_ROLE", "driver")
    if role == "driver":
        return run_driver()
    if role == "worker":
        return run_worker()
    raise ValueError(f"Unknown distributed runtime role: {role}")


__all__ = [
    "assigned_chunks", "attempt_manifest_uri", "build_plan",
    "chunk_manifests", "completed_chunk_count", "dlt_worker", "driver",
    "load_config", "main", "plan_uri", "request_uri", "result_uri",
    "selected_uri", "select_manifests",
]
