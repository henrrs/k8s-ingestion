"""Pure planner: metadata estimates and statistics determine range predicates.

Statistics affect performance only: unbounded, disjoint predicates cover all keys
even when the histogram is sampled or stale. No table-specific hints are accepted.
"""

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
import math

from .config import DistributedPlannerConfig, PlannerConfig

NUMERIC_TYPES = {"tinyint", "smallint", "int", "bigint", "decimal", "numeric", "money", "smallmoney", "float", "real"}
DATE_TYPES = {"date", "datetime", "datetime2", "smalldatetime"}
INTEGER_TYPES = {"tinyint", "smallint", "int", "bigint"}

MIB = 1024 * 1024
GIB = 1024 * MIB
AUTO_PARALLEL_THRESHOLD_BYTES = 128 * MIB
AUTO_PREFERRED_CHUNK_BYTES = 256 * MIB
AUTO_MIN_CHUNK_BYTES = 32 * MIB
AUTO_MAX_CHUNK_BYTES = 4 * GIB
AUTO_MIN_ROWS_PER_CHUNK = 100_000
AUTO_MAX_ROWS_PER_CHUNK = 5_000_000
AUTO_FETCH_BATCH_BYTES = 16 * MIB
AUTO_MIN_FETCH_ROWS = 1_000
AUTO_MAX_FETCH_ROWS = 100_000


def identifier(value):
    """Quote a SQL Server identifier (kept as the compatibility default)."""
    return "[" + value.replace("]", "]]") + "]"


def ansi_identifier(value):
    """Quote an ANSI/Oracle identifier without changing its case."""
    return '"' + value.replace('"', '""') + '"'


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    nullable: bool = False
    index_id: int | None = None
    clustered: bool = False
    unique: bool = False
    primary_key: bool = False
    stats_id: int | None = None
    precision: int = 0
    scale: int = 0


@dataclass(frozen=True)
class HistogramStep:
    high: object | None
    range_rows: float
    equal_rows: float


@dataclass(frozen=True)
class TableMetadata:
    estimated_rows: int
    estimated_bytes: int
    columns: tuple[Column, ...]
    object_id: int = 0


@dataclass
class ReadPlan:
    strategy: str
    predicates: list[str]
    column: str | None
    requested_partitions: int
    rationale: str
    estimated_rows: int
    estimated_source_bytes: int
    fetch_size: int = 0
    fetch_size_mode: str = "auto"
    target_fetch_batch_bytes: int = AUTO_FETCH_BATCH_BYTES
    warnings: list[str] = field(default_factory=list)

    @property
    def partitions(self):
        return len(self.predicates)

    def to_dict(self):
        return {**asdict(self), "partitions": self.partitions}


@dataclass
class DistributedReadPlan:
    strategy: str
    predicates: list[str]
    column: str | None
    requested_chunks: int
    parallelism: int
    rationale: str
    estimated_rows: int
    estimated_source_bytes: int
    target_chunk_bytes: int = 0
    fetch_size: int = 0
    chunk_size_mode: str = "manual"
    fetch_size_mode: str = "manual"
    snapshot: dict = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def chunks(self):
        return [{"index": index, "predicate": predicate}
                for index, predicate in enumerate(self.predicates)]

    @property
    def chunk_count(self):
        return len(self.predicates)

    def to_dict(self):
        return {**asdict(self), "chunk_count": self.chunk_count,
                "chunks": self.chunks}


def select_column(columns):
    candidates = [c for c in columns if c.index_id and c.sql_type in NUMERIC_TYPES | DATE_TYPES]
    if not candidates:
        return None
    # Metadata presents only leading keys of enabled, unfiltered indexes.
    # Clustered ranges avoid repeated heap scans and key lookups for wide tables.
    return max(candidates, key=lambda c: (c.clustered, c.unique, c.primary_key, not c.nullable, c.sql_type in INTEGER_TYPES))


def parse_value(value, column):
    if value is None:
        return None
    if column.sql_type in NUMERIC_TYPES:
        return Decimal(str(value))
    if column.sql_type == "date":
        return date.fromisoformat(str(value).split("T")[0].split(" ")[0])
    return datetime.fromisoformat(str(value))


def sql_literal(value, column):
    if column.sql_type in NUMERIC_TYPES:
        decimal = Decimal(str(value))
        if not decimal.is_finite():
            raise ValueError("Non-finite histogram value")
        return format(decimal, "f")
    value = parse_value(value, column) if isinstance(value, str) else value
    return "'" + value.isoformat() + "'"


def interpolate(low, high, ratio, column):
    ratio = min(1.0, max(0.0, ratio))
    if isinstance(low, Decimal):
        with localcontext() as context:
            context.prec = 80
            result = low + (high - low) * Decimal(str(ratio))
            if column.sql_type in INTEGER_TYPES:
                result = result.to_integral_value(rounding="ROUND_CEILING")
            elif column.sql_type in {"decimal", "numeric", "money", "smallmoney"}:
                result = result.quantize(Decimal(1).scaleb(-column.scale))
            return result
    delta = high - low
    if isinstance(low, datetime):
        return low + timedelta(seconds=delta.total_seconds() * ratio)
    return low + timedelta(days=round(delta.days * ratio))


def histogram_boundaries(steps, partitions, column):
    ordered = [s for s in steps if s.high is not None]
    if not ordered:
        return []
    total = sum(max(0, s.range_rows) + max(0, s.equal_rows) for s in steps)
    if total <= 0:
        return []
    boundaries = []
    null_rows = sum(max(0, s.equal_rows) for s in steps if s.high is None)
    for index in range(1, partitions):
        target = total * index / partitions
        cumulative = null_rows
        previous = None
        for step in ordered:
            end_range = cumulative + max(0, step.range_rows)
            end_step = end_range + max(0, step.equal_rows)
            if target <= end_step:
                if previous is not None and target < end_range and step.range_rows:
                    point = interpolate(previous, step.high, (target - cumulative) / step.range_rows, column)
                else:
                    point = step.high
                boundaries.append(point)
                break
            cumulative = end_step
            previous = step.high
    # Repeated heavy hitters cannot be divided by a range predicate; coalesce
    # boundaries instead of generating duplicate/empty partitions.
    low, high = ordered[0].high, ordered[-1].high
    return sorted({b for b in boundaries if low < b <= high})


def predicates_for(column, boundaries, identifier_renderer=identifier):
    name = identifier_renderer(column.name)
    points = sorted(set(boundaries))
    if not points:
        return ["1=1"]
    values = [sql_literal(b, column) for b in points]
    first = f"{name} < {values[0]}"
    if column.nullable:
        first = f"({first} OR {name} IS NULL)"
    predicates = [first]
    predicates.extend(f"{name} >= {low} AND {name} < {high}" for low, high in zip(values, values[1:]))
    predicates.append(f"{name} >= {values[-1]}")
    return predicates


def plan_read(metadata, config, column=None, histogram=(), bounds=None, warnings=(),
              identifier_renderer=identifier):
    requested = max(1, min(
        config.max_connections,
        config.task_slots * config.max_partition_oversubscription,
        math.ceil(metadata.estimated_bytes / config.target_partition_bytes),
    ))
    fetch_size, fetch_size_mode = adaptive_fetch_size(
        metadata, requested, config.fetch_size,
        target_bytes=config.target_fetch_batch_bytes,
    )
    common = dict(estimated_rows=metadata.estimated_rows, estimated_source_bytes=metadata.estimated_bytes,
                  requested_partitions=requested, fetch_size=fetch_size,
                  fetch_size_mode=fetch_size_mode,
                  target_fetch_batch_bytes=config.target_fetch_batch_bytes,
                  warnings=list(warnings))
    if requested == 1:
        return ReadPlan("single_scan", ["1=1"], None, rationale="Estimated table size fits one task or compute/connection budget is one.", **common)
    column = column or select_column(metadata.columns)
    if column is None:
        return ReadPlan("single_scan", ["1=1"], None, rationale="No leading numeric/date key on an enabled, unfiltered index. Avoid repeated full-table hash scans.", **common)
    boundaries = histogram_boundaries(histogram, requested, column)
    strategy = "histogram_ranges"
    rationale = "Weighted catalog histogram quantiles on an indexed leading key; partition count follows estimated bytes and compute/connection budgets."
    if not boundaries and bounds and bounds[0] is not None and bounds[0] < bounds[1]:
        low, high = bounds
        boundaries = sorted({interpolate(low, high, i / requested, column) for i in range(1, requested)})
        boundaries = [b for b in boundaries if low < b <= high]
        strategy = "indexed_ranges"
        rationale = "Histogram unavailable or degenerate; indexed min/max seeks supply range boundaries. Skew is not known."
    if not boundaries:
        return ReadPlan("single_scan", ["1=1"], column.name, rationale="Indexed key has no usable split points; a single scan avoids empty or overlapping tasks.", **common)
    if len(boundaries) + 1 < requested:
        common["warnings"].append("Heavy hitters/low cardinality reduced the partition count; ranges cannot split identical values.")
    return ReadPlan(
        strategy,
        predicates_for(column, boundaries, identifier_renderer),
        column.name,
        rationale=rationale,
        **common,
    )


def adaptive_chunk_settings(metadata, config):
    """Resolve chunk count and byte target from source scale and concurrency.

    The byte estimate is the SQL Server allocated size, so it deliberately does
    not assume a Parquet compression ratio that may change with the data.
    """
    source_bytes = max(0, metadata.estimated_bytes)
    source_rows = max(0, metadata.estimated_rows)
    if config.target_chunk_bytes is not None:
        count = max(
            1,
            min(
                config.max_chunks,
                math.ceil(source_bytes / config.target_chunk_bytes),
            ),
        )
        return count, config.target_chunk_bytes, "manual"
    if source_bytes <= AUTO_PARALLEL_THRESHOLD_BYTES or source_rows == 0:
        return 1, max(1, source_bytes), "auto"

    connection_budget = max(
        1, min(config.max_workers, config.max_source_connections)
    )
    max_useful_chunks = max(
        1,
        min(
            config.max_chunks,
            math.ceil(source_bytes / AUTO_MIN_CHUNK_BYTES),
            math.ceil(source_rows / AUTO_MIN_ROWS_PER_CHUNK),
        ),
    )
    minimum_for_retry_size = max(
        math.ceil(source_bytes / AUTO_MAX_CHUNK_BYTES),
        math.ceil(source_rows / AUTO_MAX_ROWS_PER_CHUNK),
    )
    preferred = math.ceil(source_bytes / AUTO_PREFERRED_CHUNK_BYTES)
    balance_floor = min(connection_budget * 2, max_useful_chunks)
    count = max(minimum_for_retry_size, preferred, balance_floor)
    count = max(1, min(config.max_chunks, max_useful_chunks, count))
    resolved_bytes = max(1, math.ceil(source_bytes / count))
    return count, resolved_bytes, "auto"


def adaptive_fetch_size(
    metadata, chunk_count, configured=None, target_bytes=AUTO_FETCH_BATCH_BYTES
):
    if configured is not None:
        return configured, "manual"
    if metadata.estimated_rows <= 0:
        return AUTO_MIN_FETCH_ROWS, "auto"
    average_row_bytes = max(
        1, math.ceil(metadata.estimated_bytes / metadata.estimated_rows)
    )
    batch_rows = target_bytes // average_row_bytes
    batch_rows = max(AUTO_MIN_FETCH_ROWS, min(AUTO_MAX_FETCH_ROWS, batch_rows))
    estimated_chunk_rows = max(
        1, math.ceil(metadata.estimated_rows / max(1, chunk_count))
    )
    return min(batch_rows, estimated_chunk_rows), "auto"


def desired_chunk_count(metadata, config):
    return adaptive_chunk_settings(metadata, config)[0]


def plan_distributed_read(metadata, config: DistributedPlannerConfig, column=None,
                          histogram=(), bounds=None, warnings=(),
                          identifier_renderer=identifier, snapshot=None):
    """Plan durable chunks independently from concurrent source connections."""
    requested, target_chunk_bytes, chunk_size_mode = adaptive_chunk_settings(
        metadata, config
    )
    fetch_size, fetch_size_mode = adaptive_fetch_size(
        metadata, requested, config.fetch_size
    )
    # Reuse the exact range and safety logic used by Spark while removing Spark's
    # task-slot and connection limits from the durable chunk count.
    base_config = PlannerConfig(
        max_connections=requested,
        target_partition_bytes=target_chunk_bytes,
        task_slots=max(1, math.ceil(requested / 2)),
        fetch_size=fetch_size,
        max_partition_oversubscription=2,
    )
    base = plan_read(
        metadata, base_config, column, histogram, bounds, warnings,
        identifier_renderer,
    )
    parallelism = min(config.max_workers, config.max_source_connections,
                      base.partitions)
    rationale = base.rationale + (
        f" Durable chunks are independent from concurrency; at most {parallelism} "
        "source connections run simultaneously."
    )
    return DistributedReadPlan(
        strategy=base.strategy,
        predicates=base.predicates,
        column=base.column,
        requested_chunks=requested,
        parallelism=parallelism,
        rationale=rationale,
        estimated_rows=base.estimated_rows,
        estimated_source_bytes=base.estimated_source_bytes,
        target_chunk_bytes=target_chunk_bytes,
        fetch_size=fetch_size,
        chunk_size_mode=chunk_size_mode,
        fetch_size_mode=fetch_size_mode,
        snapshot=dict(snapshot or {}),
        warnings=base.warnings,
    )
