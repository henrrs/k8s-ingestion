"""Generic deferrable operator for versioned container workloads on Kubernetes."""
from datetime import timedelta
import json
import time

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowException, AirflowSkipException

from company_airflow.hooks.kubernetes_job import KubernetesJobHook
from company_airflow.job import build_job, job_name, job_result
from company_airflow.triggers.job import KubernetesJobTrigger


class EnterpriseK8sOperator(BaseOperator):
    template_fields = ("configuration", "compute_profile", "execution_profile", "image", "command", "arguments", "env", "enabled")
    template_fields_renderers = {"configuration": "json", "env": "json"}
    ui_color = "#E8F5E9"

    def __init__(self, *, image, configuration, compute_profile="small",
                 command=None, arguments=None, env=None, secret_env=None,
                 namespace="spark-lab", timeout_seconds=3600, poll_interval=5,
                 result_marker="COMPANY_JOB_RESULT=", in_cluster=True,
                 kube_context=None, enabled=True, service_account_name=None,
                 backoff_limit=0, execution_profile=None, **kwargs):
        super().__init__(**kwargs)
        self.image, self.configuration, self.compute_profile = image, configuration, compute_profile
        self.command, self.arguments, self.env = command, arguments, env
        self.secret_env, self.namespace = secret_env, namespace
        self.timeout_seconds, self.poll_interval = timeout_seconds, poll_interval
        self.result_marker = result_marker
        self.service_account_name = service_account_name
        self.backoff_limit = backoff_limit
        self.execution_profile = execution_profile
        self.in_cluster, self.kube_context, self.enabled = in_cluster, kube_context, enabled
        self.job_name = None

    def _hook(self):
        return KubernetesJobHook(self.namespace, self.in_cluster, self.kube_context)

    def execute(self, context):
        if str(self.enabled).lower() in {"false", "0", "none"}:
            raise AirflowSkipException("Workload was not selected")
        ti = context["ti"]
        self.job_name = job_name(ti.dag_id, ti.task_id, context["run_id"], ti.try_number)
        body = build_job(
            name=self.job_name, namespace=self.namespace, image=self.image,
            configuration=self.configuration, compute_profile=self.compute_profile,
            command=self.command, arguments=self.arguments, env=self.env,
            secret_env=self.secret_env, timeout_seconds=self.timeout_seconds,
            service_account_name=self.service_account_name,
            backoff_limit=self.backoff_limit,
            execution_profile=self.execution_profile)
        job = self._hook().submit(body)
        self.log.info("Kubernetes Job %s submitted/reattached; image=%s profile=%s",
                      self.job_name, self.image, self.compute_profile)
        ti.xcom_push(key="kubernetes_job", value=self.job_name)
        result = job_result(job)
        if result:
            return self.execute_complete(context, {"name": self.job_name, **result})
        self.defer(
            trigger=KubernetesJobTrigger(
                name=self.job_name, namespace=self.namespace,
                deadline=time.time() + self.timeout_seconds,
                poll_interval=self.poll_interval, in_cluster=self.in_cluster,
                kube_context=self.kube_context),
            method_name="execute_complete", timeout=timedelta(seconds=self.timeout_seconds + 120))

    def execute_complete(self, context, event=None):
        if not event or not event.get("name"):
            raise AirflowException("Kubernetes Job trigger returned no result")
        self.job_name = event["name"]
        summary = None
        try:
            for pod_name, logs in self._hook().logs(self.job_name):
                self.log.info("Pod %s (last 500 lines):\n%s", pod_name, logs)
                for line in logs.splitlines():
                    if self.result_marker in line:
                        summary = json.loads(line.split(self.result_marker, 1)[1])
        except Exception as error:
            self.log.warning("Job logs unavailable: %s", type(error).__name__)
        if event.get("status") != "success":
            raise AirflowException(f"Kubernetes Job {self.job_name}: {event.get('state')} — {event.get('message', '')}")
        return {"job": self.job_name, "namespace": self.namespace,
                "profile": self.execution_profile or self.compute_profile,
                "metrics": summary, "state": event.get("state")}

    def on_kill(self):
        if self.job_name:
            self._hook().delete(self.job_name)
