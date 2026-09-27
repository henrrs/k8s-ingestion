"""Oracle ingestion exercised through the official KubernetesJobOperator path."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param
from company_airflow.operators import AdaptiveIngestionOperator


with DAG(
    dag_id="company_ingestion_oracle_adaptive",
    description="Oracle → Delta through an isolated adaptive Driver Job",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "compute_profile": Param("small", type="string", enum=["small", "medium"]),
        "max_workers": Param(4, type="integer", minimum=1, maximum=8),
    },
    tags=["company-k8s", "oracle", "adaptive", "delta"],
) as dag:
    AdaptiveIngestionOperator(
        task_id="ingest_oracle_wide",
        image="company-dlt-ingestion:0.6.0",
        compute_profile="{{ params.compute_profile }}",
        source={
            "type": "oracle",
            "host": "oracle",
            "port": 1521,
            "service_name": "FREEPDB1",
            "database": "FREEPDB1",
            "schema": "BENCHMARK",
            "table": "WIDE",
            "query_timeout_seconds": 1800,
        },
        destination={
            "format": "delta",
            "uri": "s3://lakehouse/oracle-airflow/bronze/wide",
        },
        execution={
            "max_workers": "{{ params.max_workers }}",
            "max_source_connections": "{{ params.max_workers }}",
            "max_chunks": 64,
            "extract_backend": "oracle_arrow",
            "target_chunk_bytes": "auto",
            "fetch_size": "auto",
            "publication_mode": "auto",
            "chunk_retries": 2,
            "control_uri": "s3://ingestion-control/runs",
        },
        storage={"type": "s3", "endpoint_url": "http://seaweedfs:8333"},
        validation={"require_source_row_match": True},
        metrics={
            "output_uri": "s3://metrics/runs-dlt-distributed",
            "pushgateway": "http://pushgateway:9091",
        },
        secret_env=[
            {"name": "ORACLE_USER", "secret": "oracle-credentials", "key": "username"},
            {"name": "ORACLE_PASSWORD", "secret": "oracle-credentials", "key": "password"},
            {"name": "AWS_ACCESS_KEY_ID", "secret": "seaweedfs-credentials", "key": "access_key"},
            {"name": "AWS_SECRET_ACCESS_KEY", "secret": "seaweedfs-credentials", "key": "secret_key"},
        ],
        timeout_seconds=3600,
        retries=0,
        execution_timeout=timedelta(minutes=65),
    )
