"""One Airflow task for a driver-coordinated distributed ingestion run."""

from copy import deepcopy

from company_airflow.job import JOB_PROFILES
from company_airflow.ingestion import build_ingestion_configuration
from company_airflow.operators.enterprise_k8s import EnterpriseK8sOperator


class EnterpriseIngestionOperator(EnterpriseK8sOperator):
    """Submit one Driver Job; the driver plans and owns all worker Jobs."""

    ui_color = "#DDF4FF"

    def __init__(
        self,
        *,
        source,
        destination,
        execution=None,
        storage=None,
        metrics=None,
        validation=None,
        image="company-dlt-ingestion:0.6.0",
        compute_profile="small",
        namespace="spark-lab",
        driver_service_account="ingestion-driver",
        worker_service_account="ingestion-worker",
        secret_env=None,
        env=None,
        timeout_seconds=7200,
        **kwargs,
    ):
        if compute_profile not in JOB_PROFILES and "{{" not in str(compute_profile):
            raise ValueError(
                f"Unknown compute profile {compute_profile!r}; "
                f"choose {list(JOB_PROFILES)}"
            )
        configuration = build_ingestion_configuration(
            source=source, destination=destination, execution=execution,
            storage=storage, metrics=metrics, validation=validation, image=image,
            compute_profile=compute_profile, namespace=namespace,
            worker_service_account=worker_service_account,
            secret_env=secret_env, env=env, timeout_seconds=timeout_seconds,
        )
        secrets = configuration["_orchestration"]["secret_env"]
        worker_env = configuration["_orchestration"]["worker_env"]
        super().__init__(
            image=image,
            configuration=configuration,
            compute_profile="small",
            execution_profile=compute_profile,
            arguments=["driver"],
            env=worker_env,
            secret_env=secrets,
            namespace=namespace,
            timeout_seconds=timeout_seconds,
            result_marker="COMPANY_INGESTION_RESULT=",
            service_account_name=driver_service_account,
            backoff_limit=1,
            **kwargs,
        )

    def execute(self, context):
        if self.execution_profile not in JOB_PROFILES:
            raise ValueError(
                f"Unknown compute profile {self.execution_profile!r}; "
                f"choose {list(JOB_PROFILES)}"
            )
        self.configuration["_orchestration"]["worker_resources"] = deepcopy(
            JOB_PROFILES[self.execution_profile]
        )
        return super().execute(context)
