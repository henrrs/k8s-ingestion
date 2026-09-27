"""Pure construction of the driver/worker execution contract."""

from copy import deepcopy

from company_airflow.job import JOB_PROFILES


DEFAULT_SECRET_ENV = [
    {"name": "SQLSERVER_USER", "secret": "sqlserver-credentials", "key": "username"},
    {"name": "SQLSERVER_PASSWORD", "secret": "sqlserver-credentials", "key": "password"},
    {"name": "AWS_ACCESS_KEY_ID", "secret": "seaweedfs-credentials", "key": "access_key"},
    {"name": "AWS_SECRET_ACCESS_KEY", "secret": "seaweedfs-credentials", "key": "secret_key"},
]


def build_ingestion_configuration(
    *, source, destination, execution, storage, metrics, validation, image,
    compute_profile, namespace, worker_service_account, secret_env, env,
    timeout_seconds, workload_identity=False,
):
    # ``None`` preserves the local-lab defaults.  An explicit empty list is a
    # meaningful production configuration: credentials are resolved by the
    # runtime through workload identity instead of Kubernetes Secrets.
    secrets = deepcopy(DEFAULT_SECRET_ENV if secret_env is None else secret_env)
    worker_env = {"AWS_REGION": "us-east-1", "DLT_TELEMETRY": "false"}
    worker_env.update(env or {})
    return {
        "source": deepcopy(source),
        "destination": deepcopy(destination),
        "execution": deepcopy(execution or {}),
        "storage": deepcopy(storage or {}),
        "metrics": deepcopy(metrics or {}),
        "validation": deepcopy(validation or {}),
        "_orchestration": {
            "image": image,
            "namespace": namespace,
            "worker_service_account": worker_service_account,
            "worker_resources": deepcopy(JOB_PROFILES.get(compute_profile, {})),
            "worker_env": worker_env,
            "secret_env": secrets,
            "timeout_seconds": timeout_seconds,
            "workload_identity": bool(workload_identity),
        },
    }
