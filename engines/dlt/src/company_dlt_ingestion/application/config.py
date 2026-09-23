"""Validated contract used by the driver and its workers."""

from dataclasses import dataclass
import re

from company_ingestion_core.config import DistributedPlannerConfig


def required(mapping, key):
    value = mapping.get(key)
    if value is None or value == "":
        raise ValueError(f"{key} is required")
    return value


def automatic_or_integer(mapping, key):
    value = mapping.get(key, "auto")
    if value is None or value == "auto":
        return None
    if isinstance(value, bool):
        raise ValueError(f"execution.{key} must be 'auto' or a positive integer")
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"execution.{key} must be 'auto' or a positive integer"
        ) from error


@dataclass(frozen=True)
class DistributedConfig:
    raw: dict

    def __post_init__(self):
        source = required(self.raw, "source")
        destination = required(self.raw, "destination")
        orchestration = required(self.raw, "_orchestration")
        required(source, "type")
        required(destination, "format")
        required(destination, "uri")
        for key in ("image", "namespace", "worker_service_account"):
            required(orchestration, key)
        if not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", self.run_id):
            raise ValueError("run_id must be a DNS-compatible Kubernetes name")
        self.planner_config

    @property
    def run_id(self):
        return required(self.raw, "run_id")

    @property
    def profile(self):
        return required(self.raw, "profile")

    @property
    def source(self):
        return self.raw["source"]

    @property
    def destination(self):
        return self.raw["destination"]

    @property
    def execution(self):
        return self.raw.get("execution", {})

    @property
    def metrics(self):
        return self.raw.get("metrics", {})

    @property
    def storage(self):
        return self.raw.get("storage", {})

    @property
    def orchestration(self):
        return self.raw["_orchestration"]

    @property
    def planner_config(self):
        values = self.execution
        return DistributedPlannerConfig(
            max_workers=int(values.get("max_workers", 4)),
            max_source_connections=int(values.get("max_source_connections", 4)),
            max_chunks=int(values.get("max_chunks", 128)),
            target_chunk_bytes=automatic_or_integer(
                values, "target_chunk_bytes"
            ),
            fetch_size=automatic_or_integer(values, "fetch_size"),
        )

    @property
    def control_uri(self):
        root = self.execution.get(
            "control_uri", "s3://ingestion-control/runs"
        ).rstrip("/")
        return f"{root}/{self.run_id}"

    @property
    def staging_uri(self):
        root = self.execution.get("staging_uri")
        if not root:
            return self.final_uri.rstrip("/") + "/_staging"
        return f"{root.rstrip('/')}/{self.run_id}"

    @property
    def final_uri(self):
        return f"{self.destination['uri'].rstrip('/')}/{self.run_id}"

    @property
    def metrics_uri(self):
        root = self.metrics.get(
            "output_uri", "s3://metrics/runs-dlt-distributed"
        ).rstrip("/")
        return f"{root}/{self.run_id}.json"

    @property
    def endpoint_url(self):
        return self.storage.get("endpoint_url", "http://seaweedfs:8333")
