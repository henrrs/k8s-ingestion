"""Thin client for the native Apache Spark application API."""
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

GROUP, VERSION, PLURAL = "spark.apache.org", "v1", "sparkapplications"


class SparkApplicationHook:
    def __init__(self, namespace="spark-lab", in_cluster=True, kube_context=None):
        self.namespace = namespace
        if in_cluster:
            config.load_incluster_config()
            api_client = client.ApiClient()
        else:
            api_client = config.new_client_from_config(context=kube_context)
        self.custom = client.CustomObjectsApi(api_client)
        self.core = client.CoreV1Api(api_client)

    def get(self, name):
        return self.custom.get_namespaced_custom_object(
            GROUP, VERSION, self.namespace, PLURAL, name, _request_timeout=(5, 30))

    def submit(self, body):
        try:
            return self.custom.create_namespaced_custom_object(
                GROUP, VERSION, self.namespace, PLURAL, body, _request_timeout=(5, 30))
        except ApiException as error:
            if error.status != 409:
                raise
            # Stable name provides reattachment when a worker repeats a submission.
            existing = self.get(body["metadata"]["name"])
            if existing.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/managed-by") != "company-airflow":
                raise RuntimeError("Refusing to attach to a SparkApplication owned by another client") from error
            return existing

    def delete(self, name):
        try:
            self.custom.delete_namespaced_custom_object(
                GROUP, VERSION, self.namespace, PLURAL, name,
                body=client.V1DeleteOptions(propagation_policy="Foreground"),
                _request_timeout=(5, 30))
        except ApiException as error:
            if error.status != 404:
                raise

    def driver_logs(self, name):
        pods = self.core.list_namespaced_pod(
            self.namespace, label_selector=f"company-run={name},spark-role=driver",
            _request_timeout=(5, 30))
        for pod in pods.items:
            yield pod.metadata.name, self.core.read_namespaced_pod_log(
                pod.metadata.name, self.namespace, container="spark-kubernetes-driver",
                tail_lines=500, _request_timeout=(5, 30))
