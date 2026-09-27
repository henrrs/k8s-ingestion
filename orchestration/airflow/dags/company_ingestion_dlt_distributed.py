"""Driver-coordinated distributed dltHub ingestion on Kubernetes."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param
from company_airflow.operators import AdaptiveIngestionOperator


with DAG(
    dag_id="company_ingestion_dlt_distributed",
    description="SQL Server → Delta with one isolated driver and adaptive dlt workers",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "table": Param("wide", type="string", enum=["small", "medium", "wide"]),
        "compute_profile": Param("small", type="string", enum=["small", "medium"]),
        "max_workers": Param(4, type="integer", minimum=1, maximum=8),
    },
    tags=["company-k8s", "dlt", "distributed", "delta"],
    doc_md="""A single Airflow task starts an isolated Driver Job.

The driver discovers SQL Server metadata, creates an adaptive Indexed Job,
validates immutable chunk manifests and publishes one Delta snapshot.
""",
) as dag:
    AdaptiveIngestionOperator(
        task_id="ingest_table",
        image="company-dlt-ingestion:0.6.0",
        compute_profile="{{ params.compute_profile }}",
        source={
            "type": "sqlserver",
            "host": "sqlserver",
            "port": 1433,
            "database": "Benchmark",
            "schema": "dbo",
            "table": "{{ params.table }}",
        },
        destination={
            "format": "delta",
            "uri": "s3://lakehouse/dlt-distributed/bronze/{{ params.table }}",
        },
        execution={
            "max_workers": "{{ params.max_workers }}",
            "max_source_connections": "{{ params.max_workers }}",
            "max_chunks": 64,
            "extract_backend": "mssql_arrow",
            "target_chunk_bytes": "auto",
            "fetch_size": "auto",
            "target_file_bytes": 256 * 1024 * 1024,
            "control_uri": "s3://ingestion-control/runs",
        },
        storage={"endpoint_url": "http://seaweedfs:8333"},
        validation={"require_source_row_match": True},
        metrics={
            "output_uri": "s3://metrics/runs-dlt-distributed",
            "pushgateway": "http://pushgateway:9091",
        },
        timeout_seconds=3600,
        retries=0,
        execution_timeout=timedelta(minutes=65),
    )
