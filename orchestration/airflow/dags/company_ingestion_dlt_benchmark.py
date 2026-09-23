"""Manual dltHub benchmark executed as generic Kubernetes Jobs."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param
from company_airflow.operators.enterprise_k8s import EnterpriseK8sOperator

secrets = [
    {"name": "SQLSERVER_USER", "secret": "sqlserver-credentials", "key": "username"},
    {"name": "SQLSERVER_PASSWORD", "secret": "sqlserver-credentials", "key": "password"},
    {"name": "AWS_ACCESS_KEY_ID", "secret": "seaweedfs-credentials", "key": "access_key"},
    {"name": "AWS_SECRET_ACCESS_KEY", "secret": "seaweedfs-credentials", "key": "secret_key"},
]

with DAG(
    dag_id="company_ingestion_dlt_benchmark",
    description="SQL Server → Delta with dltHub in a generic Kubernetes Job",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), schedule=None, catchup=False,
    max_active_runs=1, render_template_as_native_obj=True,
    params={"table": Param("all", type="string", enum=["all", "small", "medium", "wide"]),
            "compute_profile": Param("small", type="string", enum=["small", "medium"])},
    tags=["company-k8s", "dlt", "benchmark", "delta"],
    doc_md="""Runs the same SQL Server fixtures as the Spark benchmark through dltHub.

The workload is a regular Kubernetes Job created by `EnterpriseK8sOperator`.
Results use the same Prometheus metric names with `engine=dlt` and are persisted under `s3://metrics/runs-dlt/`.
""",
) as dag:
    previous = None
    for table in ["small", "medium", "wide"]:
        task = EnterpriseK8sOperator(
            task_id=f"ingest_{table}",
            image="company-dlt-ingestion:0.6.0",
            enabled="{{ params.table in ['all', '" + table + "'] }}",
            compute_profile="{{ params.compute_profile }}",
            configuration={
                "source": {"type": "sqlserver", "host": "sqlserver", "port": 1433,
                           "database": "Benchmark", "schema": "dbo", "table": table},
                "destination": {"format": "delta", "uri": f"s3://lakehouse/dlt/bronze/{table}"},
                "storage": {"endpoint_url": "http://seaweedfs:8333"},
                "extract": {"backend": "pyarrow", "chunk_size": 50000},
                "metrics": {"pushgateway": "http://pushgateway:9091"},
            },
            env={"AWS_REGION": "us-east-1", "DLT_TELEMETRY": "false"},
            secret_env=secrets,
            timeout_seconds=1800, retries=0,
            trigger_rule="none_failed", execution_timeout=timedelta(minutes=35),
        )
        if previous is not None:
            previous >> task
        previous = task
