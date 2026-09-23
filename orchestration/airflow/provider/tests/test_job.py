import json
import pytest

from company_airflow.job import build_job, job_name, job_result


def test_job_name_is_stable_per_attempt():
    value = job_name("dag", "extract_table", "run", 1)
    assert value == job_name("dag", "extract_table", "run", 1)
    assert value != job_name("dag", "extract_table", "run", 2)
    assert len(value) <= 63


def test_job_manifest_injects_identity_resources_and_secret_refs():
    config = {"source": {"table": "wide"}}
    job = build_job(
        name="enterprise-test", namespace="spark-lab", image="engine:1",
        configuration=config, compute_profile="medium",
        secret_env=[{"name": "PASSWORD", "secret": "database", "key": "password"}],
        timeout_seconds=900)
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert job["apiVersion"] == "batch/v1"
    assert container["resources"]["limits"] == {"cpu": "4", "memory": "6Gi"}
    assert next(e for e in container["env"] if e["name"] == "PASSWORD")["valueFrom"] == {
        "secretKeyRef": {"name": "database", "key": "password"}}
    assert "run_id" not in config
    assert job["spec"]["activeDeadlineSeconds"] == 900


def test_job_manifest_rejects_unknown_profile_and_malformed_secret():
    with pytest.raises(ValueError):
        build_job(name="test", namespace="n", image="i", configuration={}, compute_profile="large")
    with pytest.raises(ValueError):
        build_job(name="test", namespace="n", image="i", configuration={},
                  secret_env=[{"name": "X", "value": "secret"}])
    with pytest.raises(ValueError):
        build_job(name="test", namespace="n", image="i", configuration="not-json")
    with pytest.raises(ValueError):
        build_job(name="test", namespace="n", image="", configuration={})


def test_job_result():
    assert job_result({"status": {"succeeded": 1}})["status"] == "success"
    assert job_result({"status": {"failed": 1, "conditions": [{"type": "Failed", "message": "boom"}]}}) == {
        "status": "error", "state": "Failed", "message": "boom"}
    assert job_result({"status": {"active": 1}}) is None


def test_job_supports_service_account_and_driver_retry():
    job = build_job(
        name="driver-test", namespace="spark-lab", image="engine:2",
        configuration={}, service_account_name="ingestion-driver",
        backoff_limit=1,
    )
    assert job["spec"]["backoffLimit"] == 1
    assert job["spec"]["template"]["spec"]["serviceAccountName"] == "ingestion-driver"


def test_job_separates_pod_resources_from_reported_execution_profile():
    job = build_job(
        name="driver-test", namespace="spark-lab", image="engine:2",
        configuration={}, compute_profile="small", execution_profile="medium",
    )
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["resources"]["limits"] == {"cpu": "1", "memory": "2Gi"}
    config = json.loads(next(item["value"] for item in container["env"]
                             if item["name"] == "COMPANY_JOB_CONFIG"))
    assert config["profile"] == "medium"
