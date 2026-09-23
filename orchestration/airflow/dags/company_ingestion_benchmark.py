"""Manual benchmark: table and compute profile are the only tuning inputs."""
import os
from datetime import datetime, timedelta, timezone
from airflow.sdk import DAG, Param
from company_airflow.operators.spark import CompanySparkOperator

with DAG(
    dag_id="company_ingestion_benchmark",
    description="SQL Server → Delta: metadata-adaptive ingestion on Minikube",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), schedule=None, catchup=False,
    max_active_runs=1, render_template_as_native_obj=True,
    params={"table": Param("all", type="string", enum=["all", "small", "medium", "wide"]),
            "compute_profile": Param("small", type="string", enum=["small", "medium"])},
    tags=["company-spark", "benchmark", "delta"],
    doc_md="""Choose **Trigger DAG**, then a dataset and compute profile. No source partition column or partition count is supplied.

Results: Grafana http://localhost:3000 · Spark History http://localhost:18080.
Each task logs its selected plan and a JSON result, also returned through XCom.
""",
) as dag:
    previous = None
    for table in ["small", "medium", "wide"]:
        task = CompanySparkOperator(
            task_id=f"ingest_{table}",
            enabled="{{ params.table in ['all', '" + table + "'] }}",
            compute_profile="{{ params.compute_profile }}",
            wheel_url="http://artifacts:8080/company_ingestion-0.6.0-py3-none-any.whl",
            wheel_sha256=os.environ.get("COMPANY_WHEEL_SHA256"),
            parameters={
                "source": {"type": "sqlserver", "host": "sqlserver", "database": "Benchmark", "schema": "dbo", "table": table,
                           "trust_server_certificate": True},
                "destination": {"format": "delta", "uri": f"s3a://lakehouse/bronze/{table}"},
                "planner": {"max_connections": 8, "target_partition_bytes": 8388608},
                "metrics": {"pushgateway": "http://pushgateway:9091", "output_uri": "s3a://metrics/runs"},
            },
            timeout_seconds=1800, retries=0,
            trigger_rule="none_failed", execution_timeout=timedelta(minutes=35),
        )
        if previous is not None:
            previous >> task
        previous = task
