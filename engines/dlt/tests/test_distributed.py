import json

import pytest

from company_dlt_ingestion.application.config import DistributedConfig
from company_dlt_ingestion.infrastructure.kubernetes import child_job_name, worker_job_manifest
from company_dlt_ingestion.bootstrap.entrypoint import (
    _relative_delta_path,
    assigned_chunks,
)
from company_dlt_ingestion.plugins.sources.sqlserver_arrow import connection_string, odbc_value
from company_dlt_ingestion.plugins.sources.oracle import (
    OracleSourceAdapter,
    generic_sql_type,
    oracle_name,
)


def configuration():
    return {
        "run_id": "enterprise-orders-123",
        "profile": "small",
        "source": {"type": "sqlserver", "host": "sqlserver", "database": "Sales",
                   "schema": "dbo", "table": "orders"},
        "destination": {"format": "delta", "uri": "s3://lakehouse/orders"},
        "execution": {"max_workers": 4, "max_source_connections": 3,
                      "target_chunk_bytes": 1024},
        "storage": {"endpoint_url": "http://seaweedfs:8333"},
        "_orchestration": {
            "image": "company-dlt-ingestion:0.6.0", "namespace": "spark-lab",
            "worker_service_account": "ingestion-worker",
            "worker_resources": {"cpu": "1", "memory": "2Gi"},
            "worker_env": {"AWS_REGION": "us-east-1"},
            "secret_env": [{"name": "PASSWORD", "secret": "sql", "key": "password"}],
            "timeout_seconds": 900,
        },
    }


def test_distributed_paths_are_scoped_by_run():
    config = DistributedConfig(configuration())
    assert config.control_uri.endswith("/enterprise-orders-123")
    assert config.staging_uri.endswith("/enterprise-orders-123/_staging")
    assert config.final_uri.endswith("/enterprise-orders-123")
    assert config.planner_config.max_source_connections == 3


def test_worker_job_is_indexed_and_only_references_secrets():
    config = DistributedConfig(configuration())
    plan = {"chunk_count": 12, "parallelism": 3, "plan_hash": "abc"}
    job = worker_job_manifest(config, plan, "parent-uid")
    assert job["spec"]["completionMode"] == "Indexed"
    assert job["spec"]["completions"] == 3
    assert job["spec"]["parallelism"] == 3
    assert job["metadata"]["annotations"]["company.io/execution-model"] == (
        "persistent-workers-v1"
    )
    pod = job["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == "ingestion-worker"
    env = pod["containers"][0]["env"]
    secret = next(item for item in env if item["name"] == "PASSWORD")
    assert secret["valueFrom"]["secretKeyRef"] == {"name": "sql", "key": "password"}
    embedded = json.loads(next(item["value"] for item in env if item["name"] == "COMPANY_JOB_CONFIG"))
    assert embedded["run_id"] == "enterprise-orders-123"


def test_persistent_workers_cover_each_chunk_exactly_once():
    plan = {
        "parallelism": 4,
        "chunks": [
            {"index": index, "predicate": f"id = {index}"}
            for index in range(63)
        ],
    }
    assignments = [assigned_chunks(plan, worker) for worker in range(4)]
    assert [len(chunks) for chunks in assignments] == [16, 16, 16, 15]
    indexes = [chunk["index"] for chunks in assignments for chunk in chunks]
    assert sorted(indexes) == list(range(63))
    assert len(indexes) == len(set(indexes))


def test_persistent_worker_rejects_an_index_outside_parallelism():
    with pytest.raises(ValueError):
        assigned_chunks({"parallelism": 2, "chunks": []}, 2)


def test_child_job_name_is_stable_and_bounded():
    assert child_job_name("a" * 63) == child_job_name("a" * 63)
    assert len(child_job_name("a" * 63)) <= 63


def test_invalid_run_id_is_rejected():
    value = configuration()
    value["run_id"] = "NOT valid"
    with pytest.raises(ValueError):
        DistributedConfig(value)


def test_default_staging_files_are_relative_to_delta_table():
    config = DistributedConfig(configuration())
    bucket, prefix = config.final_uri.removeprefix("s3://").split("/", 1)
    item = {
        "bucket": bucket,
        "key": prefix + "/_staging/chunks/00000/data.parquet",
    }
    assert _relative_delta_path(config.final_uri, item) == (
        "_staging/chunks/00000/data.parquet"
    )


def test_odbc_values_escape_closing_braces(monkeypatch):
    monkeypatch.setenv("SQLSERVER_USER", "reader")
    monkeypatch.setenv("SQLSERVER_PASSWORD", "a;secret}value")
    value = connection_string(configuration()["source"])
    assert "PWD={a;secret}}value}" in value
    assert "UID={reader}" in value
    assert odbc_value("a}b") == "{a}}b}"


def test_distributed_defaults_to_native_arrow_backend():
    config = DistributedConfig(configuration())
    assert config.execution.get("extract_backend", "mssql_arrow") == "mssql_arrow"


def test_oracle_adapter_selects_native_arrow_backend():
    source = {
        "type": "oracle", "host": "oracle", "service_name": "FREEPDB1",
        "schema": "benchmark", "table": "wide",
    }
    adapter = OracleSourceAdapter(source)
    assert adapter.default_backend == "oracle_arrow"
    assert oracle_name("benchmark") == "BENCHMARK"
    assert generic_sql_type("NUMBER", 18, 0) == "bigint"
    assert generic_sql_type("NUMBER", 14, 2) == "decimal"
    assert generic_sql_type("TIMESTAMP", None, 6) == "datetime2"


def test_oracle_adapter_requires_service_name_or_database():
    with pytest.raises(ValueError, match="service_name"):
        OracleSourceAdapter({
            "type": "oracle", "host": "oracle",
            "schema": "benchmark", "table": "wide",
        })
