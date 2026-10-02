"""Public Airflow interface for CompanySparkRuntime executions."""
from datetime import timedelta
import json
import time

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowException, AirflowSkipException

from company_airflow.application import build_application, execution_name
from company_airflow.hooks.spark_kubernetes import SparkApplicationHook
from company_airflow.status import application_result
from company_airflow.triggers.spark import SparkApplicationTrigger


class CompanySparkOperator(BaseOperator):
    """Run a wheel with the Apache Spark Operator (spark.apache.org/v1).

    Parameters must contain secret references, never secret values. The generic
    runtime owns wheel loading; Airflow never imports the ingestion engine.
    """
    template_fields = ("parameters", "compute_profile", "wheel_url", "wheel_sha256", "enabled")
    template_fields_renderers = {"parameters": "json"}
    ui_color = "#E6F2FA"

    def __init__(self, *, parameters, wheel_url, compute_profile="small",
                 runtime="company-spark-runtime:0.4.0", spark_version="4.2.0",
                 namespace="spark-lab", entrypoint="company_ingestion.entrypoint:main",
                 wheel_sha256=None, spark_conf=None, timeout_seconds=3600,
                 poll_interval=10, in_cluster=True, kube_context=None,
                 enabled=True, **kwargs):
        super().__init__(**kwargs)
        self.parameters = parameters
        self.wheel_url = wheel_url
        self.compute_profile = compute_profile
        self.runtime = runtime
        self.spark_version = spark_version
        self.namespace = namespace
        self.entrypoint = entrypoint
        self.wheel_sha256 = wheel_sha256
        self.spark_conf = spark_conf
        self.timeout_seconds = timeout_seconds
        self.poll_interval = poll_interval
        self.in_cluster = in_cluster
        self.kube_context = kube_context
        self.enabled = enabled
        self.application_name = None

    def _hook(self):
        return SparkApplicationHook(self.namespace, self.in_cluster, self.kube_context)

    def execute(self, context):
        if str(self.enabled).lower() in {"false", "0", "none"}:
            raise AirflowSkipException("Dataset was not selected for this benchmark")
        ti = context["ti"]
        self.application_name = execution_name(ti.dag_id, ti.task_id, context["run_id"], ti.try_number)
        body = build_application(
            name=self.application_name, namespace=self.namespace, runtime=self.runtime,
            spark_version=self.spark_version, wheel_url=self.wheel_url,
            wheel_sha256=self.wheel_sha256, parameters=self.parameters,
            compute_profile=self.compute_profile, entrypoint=self.entrypoint, spark_conf=self.spark_conf)
        app = self._hook().submit(body)
        self.log.info("SparkApplication %s submitted/reattached in %s; profile=%s",
                      self.application_name, self.namespace, self.compute_profile)
        self.log.info("History UI: http://localhost:18080 ; benchmark dashboard: http://localhost:3000")
        ti.xcom_push(key="spark_application", value=self.application_name)
        result = application_result(app)
        if result:
            return self.execute_complete(context, {"name": self.application_name, **result})
        self.defer(
            trigger=SparkApplicationTrigger(
                name=self.application_name, namespace=self.namespace,
                deadline=time.time() + self.timeout_seconds, poll_interval=self.poll_interval,
                in_cluster=self.in_cluster, kube_context=self.kube_context),
            method_name="execute_complete", timeout=timedelta(seconds=self.timeout_seconds + 120))

    def execute_complete(self, context, event=None):
        if not event or not event.get("name"):
            raise AirflowException("Spark trigger returned no application result")
        self.application_name = event["name"]
        summary = None
        try:
            for pod_name, logs in self._hook().driver_logs(self.application_name):
                self.log.info("Driver %s (last 500 lines):\n%s", pod_name, logs)
                for line in logs.splitlines():
                    if "COMPANY_INGESTION_RESULT=" in line:
                        summary = json.loads(line.split("COMPANY_INGESTION_RESULT=", 1)[1])
        except Exception as error:
            self.log.warning("Driver logs unavailable (%s); check persisted event logs", type(error).__name__)
        if event.get("status") != "success":
            if event.get("state") in {"TimedOut", "MonitoringFailed"}:
                try:
                    self._hook().delete(self.application_name)
                except Exception:
                    self.log.exception("Could not delete failed SparkApplication %s", self.application_name)
            raise AirflowException(f"Spark {self.application_name}: {event.get('state')} — {event.get('message', '')}")
        self.log.info("Spark execution succeeded: %s", self.application_name)
        return {"application": self.application_name, "namespace": self.namespace,
                "profile": self.compute_profile, "metrics": summary, "state": event.get("state")}

    def on_kill(self):
        if self.application_name:
            self._hook().delete(self.application_name)
