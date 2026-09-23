"""Thin Kubernetes batch Job client."""
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


class KubernetesJobHook:
    def __init__(self, namespace="spark-lab", in_cluster=True, kube_context=None):
        self.namespace = namespace
        if in_cluster:
            config.load_incluster_config()
            self.api_client = client.ApiClient()
        else:
            self.api_client = config.new_client_from_config(context=kube_context)
        self.batch = client.BatchV1Api(self.api_client)
        self.core = client.CoreV1Api(self.api_client)

    def _dict(self, value):
        return self.api_client.sanitize_for_serialization(value)

    def get(self, name):
        return self._dict(self.batch.read_namespaced_job(name, self.namespace, _request_timeout=(5, 30)))

    def submit(self, body):
        try:
            return self._dict(self.batch.create_namespaced_job(self.namespace, body, _request_timeout=(5, 30)))
        except ApiException as error:
            if error.status != 409:
                raise
            existing = self.get(body["metadata"]["name"])
            if existing.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/managed-by") != "company-airflow":
                raise RuntimeError("Refusing to attach to a Kubernetes Job owned by another client") from error
            return existing

    def delete(self, name):
        try:
            self.batch.delete_namespaced_job(
                name, self.namespace, propagation_policy="Foreground", _request_timeout=(5, 30))
        except ApiException as error:
            if error.status != 404:
                raise

    def logs(self, name):
        pods = self.core.list_namespaced_pod(
            self.namespace, label_selector=f"job-name={name}", _request_timeout=(5, 30))
        for pod in pods.items:
            yield pod.metadata.name, self.core.read_namespaced_pod_log(
                pod.metadata.name, self.namespace, container="job", tail_lines=500,
                _request_timeout=(5, 30))
