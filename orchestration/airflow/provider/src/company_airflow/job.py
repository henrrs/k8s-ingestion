"""Build generic Kubernetes Jobs for containerized enterprise workloads."""
from copy import deepcopy
import hashlib
import json
import re
from collections.abc import Mapping

JOB_PROFILES = {
    "small": {"cpu": "1", "memory": "2Gi"},
    "medium": {"cpu": "4", "memory": "6Gi"},
}


def job_name(dag_id, task_id, run_id, try_number):
    identity = json.dumps([dag_id, task_id, run_id, try_number], separators=(",", ":"))
    digest = hashlib.sha256(identity.encode()).hexdigest()[:16]
    stem = re.sub("[^a-z0-9-]", "-", task_id.lower()).strip("-")[:35] or "job"
    return f"enterprise-{stem}-{digest}"


def build_job(*, name, namespace, image, configuration, compute_profile="small",
              command=None, arguments=None, env=None, secret_env=None,
              timeout_seconds=3600, service_account_name=None, backoff_limit=0,
              execution_profile=None):
    if compute_profile not in JOB_PROFILES:
        raise ValueError(f"Unknown compute profile {compute_profile!r}; choose {list(JOB_PROFILES)}")
    if not isinstance(configuration, Mapping):
        raise ValueError("configuration must be a mapping")
    if env is not None and not isinstance(env, Mapping):
        raise ValueError("env must be a mapping")
    if not isinstance(image, str) or not image.strip():
        raise ValueError("image must be a non-empty string")
    resources = JOB_PROFILES[compute_profile]
    config = deepcopy(configuration)
    config["run_id"] = name
    config["profile"] = execution_profile or compute_profile
    container_env = [{"name": "COMPANY_JOB_CONFIG", "value": json.dumps(config, separators=(",", ":"))}]
    container_env.extend({"name": key, "value": str(value)} for key, value in (env or {}).items())
    for item in secret_env or []:
        if not isinstance(item, Mapping) or set(item) != {"name", "secret", "key"}:
            raise ValueError("Each secret_env entry must contain only name, secret and key")
        if not all(isinstance(item[key], str) and item[key] for key in ("name", "secret", "key")):
            raise ValueError("secret_env name, secret and key must be non-empty strings")
        container_env.append({"name": item["name"], "valueFrom": {
            "secretKeyRef": {"name": item["secret"], "key": item["key"]}}})
    container = {
        "name": "job", "image": image, "imagePullPolicy": "IfNotPresent",
        "env": container_env,
        "resources": {"requests": resources, "limits": resources},
        "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
    }
    if command:
        container["command"] = command
    if arguments:
        container["args"] = arguments
    labels = {"app.kubernetes.io/managed-by": "company-airflow", "company-run": name,
              "company-profile": compute_profile, "company-workload": "enterprise-job"}
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": namespace, "labels": labels},
        "spec": {
            "backoffLimit": backoff_limit,
            "activeDeadlineSeconds": timeout_seconds,
            "ttlSecondsAfterFinished": 86400,
            "template": {"metadata": {"labels": labels}, "spec": {
                "restartPolicy": "Never", "containers": [container],
                **({"serviceAccountName": service_account_name}
                   if service_account_name else {})}},
        },
    }


def job_result(job):
    status = job.get("status") or {}
    if status.get("succeeded", 0) > 0:
        return {"status": "success", "state": "Succeeded", "message": "Kubernetes Job completed"}
    if status.get("failed", 0) > 0:
        message = "Kubernetes Job failed"
        for condition in status.get("conditions") or []:
            if condition.get("type") == "Failed":
                message = condition.get("message") or condition.get("reason") or message
        return {"status": "error", "state": "Failed", "message": message}
    return None
