from company_airflow.ingestion import build_ingestion_configuration


def test_operator_encapsulates_worker_runtime_without_secret_values():
    configuration = build_ingestion_configuration(
        source={"type": "sqlserver", "host": "sqlserver", "database": "Sales",
                "schema": "dbo", "table": "orders"},
        destination={"format": "delta", "uri": "s3://lakehouse/orders"},
        execution={"max_workers": 4},
        storage={}, metrics={}, validation={},
        image="company-dlt-ingestion:0.3.0", namespace="spark-lab",
        worker_service_account="ingestion-worker", secret_env=None, env=None,
        timeout_seconds=3600,
        compute_profile="medium",
    )
    runtime = configuration["_orchestration"]
    assert runtime["worker_resources"] == {"cpu": "4", "memory": "6Gi"}
    assert runtime["worker_service_account"] == "ingestion-worker"
    assert all(set(item) == {"name", "secret", "key"}
               for item in runtime["secret_env"])
