import pytest

from company_ingestion.sources.mssql_arrow import (
    _connection_string,
    summarize_task_metrics,
)


def test_connection_string_reads_credentials_from_executor_environment(monkeypatch):
    monkeypatch.setenv("TEST_DB_USER", "reader")
    monkeypatch.setenv("TEST_DB_PASSWORD", "p;ass}")
    value = _connection_string({
        "host": "sqlserver", "port": 1433, "database": "Benchmark",
        "user_env": "TEST_DB_USER", "password_env": "TEST_DB_PASSWORD",
        "encrypt": False, "trust_server_certificate": True,
        "query_timeout_seconds": 600,
    })
    assert "Server={sqlserver,1433}" in value
    assert "UID={reader}" in value
    assert "PWD={p;ass}}}" in value
    assert "Timeout" not in value


def test_missing_executor_secret_names_the_variable_without_leaking_values(monkeypatch):
    monkeypatch.delenv("MISSING_USER", raising=False)
    with pytest.raises(ValueError, match="MISSING_USER"):
        _connection_string({
            "host": "db", "port": 1433, "database": "db",
            "user_env": "MISSING_USER", "password_env": "MISSING_PASSWORD",
            "encrypt": True, "trust_server_certificate": False,
            "query_timeout_seconds": 60,
        })


def test_reader_metrics_distinguish_parallel_executor_time_from_wall_clock():
    tasks = {
        "0:0:10": {
            "chunks": 1, "batches": 2, "rows": 100, "arrow_bytes": 1_000,
            "source_connect_seconds": 1, "source_execute_seconds": 2,
            "source_fetch_seconds": 4, "arrow_consumer_wait_seconds": 3,
            "task_wall_seconds": 10, "started_at_epoch": 100,
            "finished_at_epoch": 110,
        },
        "1:0:11": {
            "chunks": 1, "batches": 3, "rows": 200, "arrow_bytes": 2_000,
            "source_connect_seconds": 1, "source_execute_seconds": 1,
            "source_fetch_seconds": 5, "arrow_consumer_wait_seconds": 5,
            "task_wall_seconds": 12, "started_at_epoch": 101,
            "finished_at_epoch": 113,
        },
    }
    result = summarize_task_metrics(tasks)
    assert result["rows"] == 300
    assert result["arrow_bytes"] == 3_000
    assert result["durations_seconds"]["source_fetch_seconds_sum"] == 9
    assert result["durations_seconds"]["task_wall_max"] == 12
    assert result["durations_seconds"]["source_pipeline_span"] == 13
