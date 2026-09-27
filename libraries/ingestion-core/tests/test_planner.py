from datetime import datetime
from decimal import Decimal
import re
import pytest

from company_ingestion_core.config import DistributedPlannerConfig, PlannerConfig
from company_ingestion_core.planner import Column, HistogramStep, TableMetadata, adaptive_chunk_settings, adaptive_fetch_size, ansi_identifier, histogram_boundaries, interpolate, plan_distributed_read, plan_read, predicates_for

def column(**kw):
    return Column(name="id", sql_type="bigint", index_id=1, clustered=True, unique=True, **kw)

def metadata(cols=None, size=1024**3):
    return TableMetadata(1000000, size, tuple(cols or [column()]))

def matches(predicate, value):
    if predicate=="1=1": return True
    if value is None: return "IS NULL" in predicate
    clauses=re.findall(r"\[id\] (>=|<) (-?[0-9.]+)", predicate)
    return all(value>=Decimal(bound) if op==">=" else value<Decimal(bound) for op,bound in clauses)

@pytest.mark.parametrize("value", [None,Decimal(-100),Decimal(0),Decimal(10),Decimal(20),Decimal(1000000000000)])
def test_predicates_cover_every_value_once_even_outside_stale_histogram(value):
    predicates=predicates_for(column(nullable=True),[Decimal(10),Decimal(20)])
    assert sum(matches(p,value) for p in predicates)==1

def test_tiny_table_does_not_parallelize():
    result=plan_read(metadata(size=1000),PlannerConfig())
    assert result.predicates==["1=1"]
    assert result.strategy=="single_scan"

def test_large_unindexed_table_avoids_repeated_hash_scans():
    result=plan_read(metadata([Column("id","bigint")]),PlannerConfig())
    assert result.strategy=="single_scan"
    assert "No leading" in result.rationale

def test_histogram_adapts_to_population_not_key_space():
    c=column()
    histogram=[HistogramStep(Decimal(1),0,1),HistogramStep(Decimal(900000),899998,1),HistogramStep(Decimal(1100000000),99999,1)]
    result=plan_read(metadata(),PlannerConfig(task_slots=2,max_connections=8),c,histogram)
    assert result.partitions==4
    # Three quartiles stay in the dense 90%; equal-width splitting would not.
    assert all(b<Decimal(900000) for b in histogram_boundaries(histogram,4,c))

def test_generic_compute_and_connection_limits():
    result=plan_read(metadata(),PlannerConfig(task_slots=8,max_connections=3),bounds=(Decimal(0),Decimal(100)))
    assert result.partitions==3

def test_repeated_histogram_values_do_not_create_overlapping_predicates():
    c=column()
    result=plan_read(metadata(),PlannerConfig(),c,[HistogramStep(Decimal(7),0,1000000)])
    assert result.predicates==["1=1"]

def test_decimal_38_key_does_not_overflow_default_decimal_context():
    c=Column("value","decimal",index_id=1,precision=38,scale=0)
    low=Decimal("10000000000000000000000000000000000000")
    high=Decimal("99999999999999999999999999999999999999")
    point=interpolate(low,high,0.5,c)
    assert low<point<high

def test_temporal_boundaries_are_supported():
    c=Column("when","datetime2",index_id=1)
    point=interpolate(datetime(2025,1,1),datetime(2025,1,3),.5,c)
    assert point==datetime(2025,1,2)

def test_identifier_escaping():
    c=Column("odd] column","int",index_id=1)
    assert predicates_for(c,[Decimal(5)])[0]=="[odd]] column] < 5"


def test_oracle_plan_uses_ansi_identifiers_and_persists_snapshot():
    result = plan_distributed_read(
        metadata(size=512 * 1024**2),
        DistributedPlannerConfig(
            max_workers=4, max_source_connections=4,
            target_chunk_bytes=64 * 1024**2,
        ),
        column(),
        bounds=(Decimal(1), Decimal(1_000_000)),
        identifier_renderer=ansi_identifier,
        snapshot={"kind": "oracle_scn", "value": 123456},
    )
    assert result.predicates[0].startswith('"id" < ')
    assert result.snapshot == {"kind": "oracle_scn", "value": 123456}

def test_distributed_plan_separates_durable_chunks_from_concurrent_connections():
    cfg = DistributedPlannerConfig(
        max_workers=4, max_source_connections=3, max_chunks=64,
        target_chunk_bytes=16 * 1024**2,
    )
    c = column()
    result = plan_distributed_read(
        metadata(size=512 * 1024**2), cfg, c,
        bounds=(Decimal(1), Decimal(32000000)),
    )
    assert result.chunk_count == 32
    assert result.parallelism == 3
    assert len(result.chunks) == 32
    assert [chunk["index"] for chunk in result.chunks] == list(range(32))


def test_distributed_plan_keeps_unsafe_table_on_one_worker():
    cfg = DistributedPlannerConfig(
        max_workers=8, max_source_connections=8, max_chunks=64,
        target_chunk_bytes=1024,
    )
    result = plan_distributed_read(metadata([Column("payload", "nvarchar")]), cfg)
    assert result.strategy == "single_scan"
    assert result.chunk_count == 1
    assert result.parallelism == 1


def test_distributed_auto_settings_avoid_tiny_chunks_for_wide_table():
    cfg = DistributedPlannerConfig(
        max_workers=4, max_source_connections=4, max_chunks=64,
    )
    source = metadata(size=512 * 1024**2)
    chunks, target_bytes, mode = adaptive_chunk_settings(source, cfg)
    fetch_size, fetch_mode = adaptive_fetch_size(source, chunks)
    assert chunks == 8
    assert target_bytes == 64 * 1024**2
    assert 30_000 <= fetch_size <= 32_000
    assert mode == fetch_mode == "auto"


def test_distributed_auto_settings_keep_small_source_single():
    cfg = DistributedPlannerConfig()
    source = TableMetadata(200_000, 68 * 1024**2, (column(),))
    result = plan_distributed_read(
        source, cfg, column(), bounds=(Decimal(1), Decimal(200_000))
    )
    assert result.chunk_count == 1
    assert result.parallelism == 1
    assert result.chunk_size_mode == "auto"


def test_distributed_manual_settings_remain_an_override():
    cfg = DistributedPlannerConfig(
        max_workers=4, max_source_connections=4, max_chunks=64,
        target_chunk_bytes=8 * 1024**2, fetch_size=50_000,
    )
    result = plan_distributed_read(
        metadata(size=512 * 1024**2), cfg, column(),
        bounds=(Decimal(1), Decimal(1_000_000)),
    )
    assert result.chunk_count == 64
    assert result.target_chunk_bytes == 8 * 1024**2
    assert result.fetch_size == 50_000
    assert result.chunk_size_mode == result.fetch_size_mode == "manual"
