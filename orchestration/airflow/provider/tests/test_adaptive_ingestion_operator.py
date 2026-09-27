import json
from types import SimpleNamespace

from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator

from company_airflow.operators.adaptive_ingestion import (
    AdaptiveIngestionOperator,
    adaptive_job_name,
)


def context(*, try_number=1, map_index=-1, run_id="scheduled__2026-09-27"):
    return {
        "run_id": run_id,
        "dag": SimpleNamespace(parent_dag=False),
        "ti": SimpleNamespace(
            dag_id="benchmark",
            task_id="ingest_wide",
            try_number=try_number,
            map_index=map_index,
        ),
    }


def operator(**kwargs):
    values = {
        "task_id": "ingest_wide",
        "source": {
            "type": "oracle",
            "host": "oracle.internal",
            "service_name": "FINPRD",
            "schema": "FINANCE",
            "table": "WIDE",
        },
        "destination": {
            "format": "delta",
            "uri": "s3://lakehouse/wide",
        },
        "execution": {
            "control_uri": "s3://ingestion-control/runs",
            "max_workers": 4,
        },
        "storage": {},
        "metrics": {},
        "validation": {},
        "in_cluster": True,
    }
    values.update(kwargs)
    task = AdaptiveIngestionOperator(**values)
    # Manifest construction asks the provider hook how it authenticated so it
    # can add an informational label. No API client is needed for this unit test.
    task.__dict__["hook"] = SimpleNamespace(is_in_cluster=True)
    return task


def test_operator_uses_official_kubernetes_job_operator_contract():
    task = operator()

    assert isinstance(task, KubernetesJobOperator)
    assert task.wait_until_job_complete is True
    assert task.deferrable is True
    assert task.do_xcom_push is False
    assert task.arguments == ["driver"]
    assert task.service_account_name == "ingestion-driver"


def test_operator_builds_one_stable_driver_job_and_runtime_contract():
    task = operator(compute_profile="medium")
    job = task.build_job_request_obj(context())
    container = job.spec.template.spec.containers[0]
    payload = json.loads(
        next(item.value for item in container.env
             if item.name == "COMPANY_JOB_CONFIG")
    )

    expected_name = "job-" + adaptive_job_name(
        "benchmark", "ingest_wide", "scheduled__2026-09-27", -1
    )
    assert job.metadata.name == expected_name
    assert job.spec.parallelism == 1
    assert job.spec.completions == 1
    assert job.spec.template.spec.service_account_name == "ingestion-driver"
    assert container.args == ["driver"]
    assert payload["run_id"] == expected_name
    assert payload["profile"] == "medium"
    assert payload["_orchestration"]["worker_resources"] == {
        "cpu": "4", "memory": "6Gi"
    }
    assert job.metadata.annotations["company.io/config-hash"]


def test_airflow_retry_reattaches_same_logical_job():
    first = operator()
    second = operator()

    first_job = first.build_job_request_obj(context(try_number=1))
    second_job = second.build_job_request_obj(context(try_number=2))

    assert first_job.metadata.name == second_job.metadata.name
    assert (
        first_job.metadata.annotations["company.io/config-hash"]
        == second_job.metadata.annotations["company.io/config-hash"]
    )


def test_workload_identity_labels_driver_and_workers_without_k8s_secrets():
    task = operator(
        credential_mode="workload_identity",
        source={
            "type": "oracle",
            "profile": "finance-oracle-prod",
            "host": "oracle.internal",
            "service_name": "FINPRD",
            "schema": "FINANCE",
            "table": "WIDE",
            "secret": {
                "provider": "azure-key-vault",
                "vault_url": "https://kv-prod.vault.azure.net",
                "name": "finance-oracle-readonly",
            },
        },
    )
    job = task.build_job_request_obj(context())
    container = job.spec.template.spec.containers[0]
    payload = json.loads(
        next(item.value for item in container.env
             if item.name == "COMPANY_JOB_CONFIG")
    )

    assert job.spec.template.metadata.labels[
        "azure.workload.identity/use"
    ] == "true"
    assert payload["_orchestration"]["workload_identity"] is True
    assert payload["_orchestration"]["secret_env"] == []
    assert all(item.value_from is None for item in container.env)


def test_local_mode_preserves_kubernetes_secret_references_without_values():
    task = operator(
        secret_env=[
            {"name": "ORACLE_PASSWORD", "secret": "oracle", "key": "password"}
        ]
    )
    job = task.build_job_request_obj(context())
    password = next(
        item for item in job.spec.template.spec.containers[0].env
        if item.name == "ORACLE_PASSWORD"
    )

    assert password.value is None
    assert password.value_from.secret_key_ref.name == "oracle"
    assert password.value_from.secret_key_ref.key == "password"


def test_result_contract_points_to_durable_driver_result():
    task = operator()
    task.build_job_request_obj(context())

    result = task._result_contract()

    assert result["job"].startswith("job-adaptive-ingest-wide-")
    assert result["result_uri"] == (
        f"s3://ingestion-control/runs/{result['job']}/result.json"
    )
