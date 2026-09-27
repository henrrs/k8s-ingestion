"""Airflow facade for one driver-coordinated adaptive ingestion run."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re
from typing import Any

from airflow.providers.cncf.kubernetes.operators.job import (
    JOB_NAME_PREFIX,
    KubernetesJobOperator,
)
from airflow.sdk.exceptions import AirflowSkipException
from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException

from company_airflow.ingestion import build_ingestion_configuration
from company_airflow.job import JOB_PROFILES


def adaptive_job_name(dag_id: str, task_id: str, run_id: str, map_index: int) -> str:
    """Return the stable Kubernetes identity of one logical Airflow run."""
    identity = json.dumps(
        [dag_id, task_id, run_id, map_index], separators=(",", ":")
    )
    digest = hashlib.sha256(identity.encode()).hexdigest()[:16]
    stem = re.sub(r"[^a-z0-9-]", "-", task_id.lower()).strip("-")
    stem = stem[:31] or "ingestion"
    return f"adaptive-{stem}-{digest}"


def _secret_env(values: list[dict[str, str]]) -> list[k8s.V1EnvVar]:
    result = []
    for item in values:
        if not isinstance(item, dict) or set(item) != {"name", "secret", "key"}:
            raise ValueError(
                "Each secret_env entry must contain only name, secret and key"
            )
        if not all(isinstance(item[key], str) and item[key]
                   for key in ("name", "secret", "key")):
            raise ValueError(
                "secret_env name, secret and key must be non-empty strings"
            )
        result.append(
            k8s.V1EnvVar(
                name=item["name"],
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=item["secret"], key=item["key"]
                    )
                ),
            )
        )
    return result


class AdaptiveIngestionOperator(KubernetesJobOperator):
    """Submit one reusable ingestion image as a durable Kubernetes Driver Job.

    DAG authors describe the source, destination and execution policy.  This
    operator owns only the Kubernetes control-plane integration; source
    discovery, adaptive planning and worker creation remain inside the driver.
    """

    template_fields = tuple(
        dict.fromkeys((*KubernetesJobOperator.template_fields,
                       "configuration", "execution_profile", "enabled"))
    )
    template_fields_renderers = {
        **KubernetesJobOperator.template_fields_renderers,
        "configuration": "json",
    }
    ui_color = "#DDF4FF"

    def __init__(
        self,
        *,
        source: dict[str, Any],
        destination: dict[str, Any],
        execution: dict[str, Any] | None = None,
        storage: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        validation: dict[str, Any] | None = None,
        image: str = "company-dlt-ingestion:0.6.0",
        compute_profile: str = "small",
        namespace: str = "spark-lab",
        driver_service_account: str = "ingestion-driver",
        worker_service_account: str = "ingestion-worker",
        credential_mode: str = "kubernetes_secret",
        secret_env: list[dict[str, str]] | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int = 7200,
        kubernetes_conn_id: str = "kubernetes_default",
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        config_file: str | None = None,
        poll_interval: float = 5,
        deferrable: bool = True,
        enabled: bool | str = True,
        **kwargs,
    ) -> None:
        if credential_mode not in {"kubernetes_secret", "workload_identity"}:
            raise ValueError(
                "credential_mode must be 'kubernetes_secret' or "
                "'workload_identity'"
            )
        if compute_profile not in JOB_PROFILES and "{{" not in compute_profile:
            raise ValueError(
                f"Unknown compute profile {compute_profile!r}; "
                f"choose {list(JOB_PROFILES)}"
            )

        resolved_secret_env = secret_env
        if credential_mode == "workload_identity" and secret_env is None:
            resolved_secret_env = []
        configuration = build_ingestion_configuration(
            source=source,
            destination=destination,
            execution=execution,
            storage=storage,
            metrics=metrics,
            validation=validation,
            image=image,
            compute_profile=compute_profile,
            namespace=namespace,
            worker_service_account=worker_service_account,
            secret_env=resolved_secret_env,
            env=env,
            timeout_seconds=timeout_seconds,
            workload_identity=credential_mode == "workload_identity",
        )
        orchestration = configuration["_orchestration"]
        driver_env = [
            k8s.V1EnvVar(name="COMPANY_JOB_CONFIG", value="{}"),
            *(
                k8s.V1EnvVar(name=key, value=str(value))
                for key, value in orchestration["worker_env"].items()
            ),
            *_secret_env(orchestration["secret_env"]),
        ]
        labels = {
            "app.kubernetes.io/managed-by": "company-airflow",
            "company-role": "ingestion-driver",
        }
        if credential_mode == "workload_identity":
            labels["azure.workload.identity/use"] = "true"

        driver_resources = JOB_PROFILES["small"]
        self.configuration = configuration
        self.execution_profile = compute_profile
        self.enabled = enabled
        self.credential_mode = credential_mode
        self._logical_job_name: str | None = None
        self._configuration_hash: str | None = None

        super().__init__(
            name="adaptive-ingestion",
            random_name_suffix=False,
            namespace=namespace,
            image=image,
            arguments=["driver"],
            env_vars=driver_env,
            labels=labels,
            service_account_name=driver_service_account,
            kubernetes_conn_id=kubernetes_conn_id,
            in_cluster=in_cluster,
            cluster_context=cluster_context,
            config_file=config_file,
            container_resources=k8s.V1ResourceRequirements(
                requests=driver_resources, limits=driver_resources
            ),
            container_security_context=k8s.V1SecurityContext(
                allow_privilege_escalation=False,
                capabilities=k8s.V1Capabilities(drop=["ALL"]),
            ),
            backoff_limit=1,
            active_deadline_seconds=timeout_seconds,
            ttl_seconds_after_finished=86400,
            parallelism=1,
            completions=1,
            wait_until_job_complete=True,
            deferrable=deferrable,
            job_poll_interval=poll_interval,
            get_logs=True,
            do_xcom_push=False,
            log_events_on_failure=True,
            durable=True,
            on_finish_action="keep_pod",
            **kwargs,
        )

    @staticmethod
    def _context_identity(context) -> tuple[str, str, str, int]:
        ti = context["ti"]
        map_index = getattr(ti, "map_index", -1)
        return (
            ti.dag_id,
            ti.task_id,
            context["run_id"],
            -1 if map_index is None else int(map_index),
        )

    def _prepare_run(self, context) -> None:
        dag_id, task_id, airflow_run_id, map_index = self._context_identity(context)
        base_name = adaptive_job_name(dag_id, task_id, airflow_run_id, map_index)
        # KubernetesJobOperator unconditionally adds ``job-`` after reconciling
        # the Job. The engine's run_id must equal that final object name because
        # the driver reads its parent Job UID to create the worker ownerReference.
        name = f"{JOB_NAME_PREFIX}{base_name}"
        if self.execution_profile not in JOB_PROFILES:
            raise ValueError(
                f"Unknown compute profile {self.execution_profile!r}; "
                f"choose {list(JOB_PROFILES)}"
            )

        config = deepcopy(self.configuration)
        config["run_id"] = name
        config["profile"] = self.execution_profile
        config["_orchestration"]["worker_resources"] = deepcopy(
            JOB_PROFILES[self.execution_profile]
        )
        payload = json.dumps(config, separators=(",", ":"), sort_keys=True)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        for item in self.env_vars:
            if item.name == "COMPANY_JOB_CONFIG":
                item.value = payload
                break
        else:  # defensive: constructor always creates this variable
            raise RuntimeError("COMPANY_JOB_CONFIG environment variable is missing")

        self.name = base_name
        self.labels["company-run"] = name
        self.annotations["company.io/config-hash"] = digest
        self._logical_job_name = name
        self._configuration_hash = digest

    def build_job_request_obj(self, context=None):
        if context is None:
            raise ValueError("AdaptiveIngestionOperator requires Airflow context")
        self._prepare_run(context)
        return super().build_job_request_obj(context)

    def create_job(self, job_request_obj):
        """Create the Driver Job or safely reattach an Airflow retry."""
        try:
            return super().create_job(job_request_obj)
        except ApiException as error:
            if error.status != 409:
                raise
            existing = self.job_client.read_namespaced_job(
                name=job_request_obj.metadata.name,
                namespace=job_request_obj.metadata.namespace,
            )
            annotations = existing.metadata.annotations or {}
            if annotations.get("company.io/config-hash") != self._configuration_hash:
                raise RuntimeError(
                    f"Kubernetes Job {job_request_obj.metadata.name} exists "
                    "with another ingestion configuration"
                ) from error
            self.log.info(
                "Reattaching to existing Kubernetes Job %s",
                job_request_obj.metadata.name,
            )
            return existing

    def execute(self, context):
        if str(self.enabled).lower() in {"false", "0", "none"}:
            raise AirflowSkipException("Ingestion was not selected")
        result = super().execute(context)
        if not self.deferrable:
            return self._result_contract()
        return result

    def execute_complete(self, context, event, **kwargs):
        super().execute_complete(context, event, **kwargs)
        job = event.get("job") or {}
        metadata = job.get("metadata") or {}
        self._logical_job_name = metadata.get("name") or self._logical_job_name
        return self._result_contract()

    def _result_contract(self) -> dict[str, Any]:
        name = self._logical_job_name or self.name
        control_root = self.configuration.get("execution", {}).get(
            "control_uri", "s3://ingestion-control/runs"
        ).rstrip("/")
        return {
            "job": name,
            "namespace": self.namespace,
            "profile": self.execution_profile,
            "result_uri": f"{control_root}/{name}/result.json",
        }
