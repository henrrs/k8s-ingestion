"""Validated limits for adaptive source planning."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PlannerConfig:
    max_connections: int = 8
    target_partition_bytes: int = 32 * 1024 * 1024
    task_slots: int = 2
    fetch_size: int | None = None

    def __post_init__(self):
        for name in ("max_connections", "target_partition_bytes", "task_slots"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"planner.{name} must be a positive integer")
        if self.fetch_size is not None and (
            isinstance(self.fetch_size, bool)
            or not isinstance(self.fetch_size, int)
            or self.fetch_size < 1
        ):
            raise ValueError("planner.fetch_size must be 'auto' or a positive integer")


@dataclass(frozen=True)
class DistributedPlannerConfig:
    max_workers: int = 4
    max_source_connections: int = 4
    max_chunks: int = 128
    target_chunk_bytes: int | None = None
    fetch_size: int | None = None

    def __post_init__(self):
        for name in ("max_workers", "max_source_connections", "max_chunks"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"execution.{name} must be a positive integer")
        for name in ("target_chunk_bytes", "fetch_size"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise ValueError(
                    f"execution.{name} must be 'auto' or a positive integer"
                )
