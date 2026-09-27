"""Creation and observation of the worker Indexed Job."""

import hashlib
import json
import time


def child_job_name(run_id):
    digest = hashlib.sha256(run_id.encode()).hexdigest()[:10]
    stem = run_id[:43].rstrip("-")
    return f"workers-{stem}-{digest}"


def worker_job_manifest(config, plan, parent_uid):
    orchestration = config.orchestration
    name = child_job_name(config.run_id)
    labels = {
        "app.kubernetes.io/managed-by": "company-ingestion-driver",
        "company-run": config.run_id,
        "company-role": "dlt-worker",
    }
    if orchestration.get("workload_identity"):
        labels["azure.workload.identity/use"] = "true"
    env = [
        {
            "name": "COMPANY_JOB_CONFIG",
            "value": json.dumps(config.raw, separators=(",", ":")),
        },
        {"name": "COMPANY_ROLE", "value": "worker"},
        {
            "name": "POD_UID",
            "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}},
        },
    ]
    env.extend(
        {"name": key, "value": str(value)}
        for key, value in orchestration.get("worker_env", {}).items()
    )
    for item in orchestration.get("secret_env", []):
        env.append(
            {
                "name": item["name"],
                "valueFrom": {
                    "secretKeyRef": {
                        "name": item["secret"],
                        "key": item["key"],
                    }
                },
            }
        )
    resources = orchestration["worker_resources"]
    container = {
        "name": "worker",
        "image": orchestration["image"],
        "imagePullPolicy": "IfNotPresent",
        "args": ["worker"],
        "env": env,
        "resources": {"requests": resources, "limits": resources},
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
        },
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": orchestration["namespace"],
            "labels": labels,
            "annotations": {
                "company.io/plan-hash": plan["plan_hash"],
                "company.io/execution-model": "persistent-workers-v1",
            },
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": config.run_id,
                    "uid": parent_uid,
                    "controller": False,
                    "blockOwnerDeletion": False,
                }
            ],
        },
        "spec": {
            "completionMode": "Indexed",
            # An index identifies one persistent worker. Each worker loops over
            # several durable chunks and checkpoints each chunk independently.
            "completions": plan["parallelism"],
            "parallelism": plan["parallelism"],
            "backoffLimitPerIndex": int(
                config.execution.get("chunk_retries", 2)
            ),
            "maxFailedIndexes": 0,
            "activeDeadlineSeconds": int(
                orchestration.get("timeout_seconds", 7200)
            ),
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "serviceAccountName": orchestration[
                        "worker_service_account"
                    ],
                    "restartPolicy": "Never",
                    "containers": [container],
                },
            },
        },
    }


class WorkerJobClient:
    def __init__(self, namespace):
        from kubernetes import client, config

        config.load_incluster_config()
        self.client_module = client
        self.namespace = namespace
        self.api_client = client.ApiClient()
        self.batch = client.BatchV1Api(self.api_client)
        self.core = client.CoreV1Api(self.api_client)

    def _dict(self, value):
        return self.api_client.sanitize_for_serialization(value)

    def parent_uid(self, name):
        job = self.batch.read_namespaced_job(name, self.namespace)
        return job.metadata.uid

    def submit(self, manifest):
        from kubernetes.client.exceptions import ApiException

        name = manifest["metadata"]["name"]
        try:
            value = self.batch.create_namespaced_job(
                self.namespace, manifest, _request_timeout=(5, 30)
            )
        except ApiException as error:
            if error.status != 409:
                raise
            value = self.batch.read_namespaced_job(name, self.namespace)
            existing = self._dict(value)
            annotations = existing.get("metadata", {}).get("annotations", {})
            if annotations.get("company.io/plan-hash") != manifest["metadata"][
                "annotations"
            ]["company.io/plan-hash"]:
                raise RuntimeError(
                    f"Worker Job {name} exists with another plan"
                ) from error
            if annotations.get("company.io/execution-model") != (
                "persistent-workers-v1"
            ):
                raise RuntimeError(
                    f"Worker Job {name} uses another execution model"
                ) from error
        return self._dict(value)

    def wait(self, name, timeout_seconds, poll_seconds=3, on_progress=None):
        deadline = time.monotonic() + timeout_seconds
        previous = None
        while time.monotonic() < deadline:
            job = self._dict(
                self.batch.read_namespaced_job(name, self.namespace)
            )
            status = job.get("status", {})
            progress = (
                int(status.get("active", 0) or 0),
                int(status.get("succeeded", 0) or 0),
                int(status.get("failed", 0) or 0),
            )
            if progress != previous:
                print(
                    f"Worker progress: active={progress[0]} "
                    f"succeeded={progress[1]} failed={progress[2]}",
                    flush=True,
                )
                if on_progress:
                    on_progress(*progress)
                previous = progress
            for condition in job.get("status", {}).get("conditions", []) or []:
                if condition.get("status") != "True":
                    continue
                if condition.get("type") == "Complete":
                    return job
                if condition.get("type") == "Failed":
                    raise RuntimeError(
                        condition.get("message")
                        or condition.get("reason")
                        or f"Worker Job {name} failed"
                    )
            time.sleep(poll_seconds)
        raise TimeoutError(f"Worker Job {name} did not finish in time")

    def successful_attempts(self, job_name):
        pods = self.core.list_namespaced_pod(
            self.namespace,
            label_selector=f"job-name={job_name}",
            _request_timeout=(5, 30),
        )
        attempts = {}
        for pod in pods.items:
            if pod.status.phase != "Succeeded":
                continue
            metadata = pod.metadata
            index = (metadata.labels or {}).get(
                "batch.kubernetes.io/job-completion-index"
            ) or (metadata.annotations or {}).get(
                "batch.kubernetes.io/job-completion-index"
            )
            if index is None:
                continue
            attempts.setdefault(int(index), []).append(str(metadata.uid))
        return {index: sorted(values) for index, values in attempts.items()}

    def attempts(self, job_name):
        """Return all pod attempts grouped by persistent worker index."""
        pods = self.core.list_namespaced_pod(
            self.namespace,
            label_selector=f"job-name={job_name}",
            _request_timeout=(5, 30),
        )
        attempts = {}
        for pod in pods.items:
            metadata = pod.metadata
            index = (metadata.labels or {}).get(
                "batch.kubernetes.io/job-completion-index"
            ) or (metadata.annotations or {}).get(
                "batch.kubernetes.io/job-completion-index"
            )
            if index is not None:
                attempts.setdefault(int(index), []).append(str(metadata.uid))
        return {index: sorted(values) for index, values in attempts.items()}
