"""Ports between the application flow and technology-specific adapters.

The contracts intentionally describe capabilities instead of vendors. Arrow
RecordBatch objects (or the legacy row batches during migration) cross the
reader/encoder boundary; immutable JSON manifests cross pod boundaries.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Protocol


class Catalog(Protocol):
    def metadata(self) -> Any: ...
    def histogram(self, metadata: Any, column: Any) -> list[Any]: ...
    def bounds(self, column: Any) -> tuple[Any, Any]: ...


class Partitioner(Protocol):
    def plan(self, planner_config: Any) -> tuple[Any, Any]: ...


class BatchReader(Protocol):
    durations: dict[str, float]

    def batches(self, predicate: str, fetch_size: int) -> Iterable[Any]: ...
    def close(self) -> None: ...


@dataclass
class ChunkEncoding:
    rows: int
    files: list[dict]
    load_ids: list[str]
    durations: dict[str, float] = field(default_factory=dict)


class BatchEncoder(Protocol):
    def encode(
        self,
        *,
        resource_name: str,
        batch_supplier: Callable[[], Iterable[Any]],
        attempt_uri: str,
        pipeline_name: str,
        store: "ArtifactStore",
    ) -> ChunkEncoding: ...


class ArtifactStore(Protocol):
    def exists(self, uri: str) -> bool: ...
    def read_json(self, uri: str) -> dict: ...
    def write_json(self, uri: str, value: dict) -> None: ...
    def write_immutable_json(self, uri: str, value: dict) -> dict: ...
    def list(self, uri: str) -> list[dict]: ...


class Publisher(Protocol):
    def publish(
        self, config: Any, store: ArtifactStore, plan: dict,
        manifests: list[dict],
    ) -> dict: ...


class SourceAdapter(Protocol):
    def partitioner(self) -> Partitioner: ...
    def reader(self, backend: str) -> BatchReader: ...
