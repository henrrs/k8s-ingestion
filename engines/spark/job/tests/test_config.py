import pytest

from company_ingestion.config import ExecutionConfig, SourceConfig


def test_table_partition_hints_are_not_an_input_contract():
    with pytest.raises(TypeError):
        SourceConfig(
            type="sqlserver", host="db", database="db", schema="dbo",
            table="x", partitionColumn="id",
        )


@pytest.mark.parametrize("read_mode", ["jdbc", "mssql_arrow"])
def test_sqlserver_read_modes_are_explicit(read_mode):
    source = SourceConfig(
        type="sqlserver", host="db", database="db", schema="dbo",
        table="wide", read_mode=read_mode,
    )
    assert source.read_mode == read_mode


def test_unknown_read_mode_is_rejected():
    with pytest.raises(ValueError, match="read_mode"):
        SourceConfig(
            type="sqlserver", host="db", database="db", schema="dbo",
            table="wide", read_mode="magic",
        )


def test_benchmark_isolation_defaults_off():
    config = ExecutionConfig.from_dict({
        "run_id": "test", "profile": "small",
        "source": {"type": "sqlserver", "host": "db", "database": "db",
                   "schema": "dbo", "table": "wide"},
        "destination": {"uri": "s3a://lakehouse/test"},
    })
    assert config.benchmark.isolate_io_phases is False


def test_fetch_size_auto_is_normalized_for_the_shared_planner():
    config = ExecutionConfig.from_dict({
        "run_id": "test", "profile": "small",
        "source": {"type": "sqlserver", "host": "db", "database": "db",
                   "schema": "dbo", "table": "wide"},
        "destination": {"uri": "s3a://lakehouse/test"},
        "planner": {"fetch_size": "auto"},
    })
    assert config.planner.fetch_size is None
