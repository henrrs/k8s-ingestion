"""Build native Apache SparkApplication objects from an execution request."""
from copy import deepcopy
import hashlib
import json
import re

PROFILES = {
    "small": {"driver_cores": 1, "driver_memory": "1g", "executors": 2,
              "executor_cores": 1, "executor_memory": "1g"},
    "medium": {"driver_cores": 1, "driver_memory": "1g", "executors": 2,
               "executor_cores": 2, "executor_memory": "2g"},
}


def execution_name(dag_id: str, task_id: str, run_id: str, try_number: int) -> str:
    identity = json.dumps([dag_id, task_id, run_id, try_number], separators=(",", ":"))
    digest = hashlib.sha256(identity.encode()).hexdigest()[:16]
    stem = re.sub("[^a-z0-9-]", "-", task_id.lower()).strip("-")[:35] or "job"
    return f"company-{stem}-{digest}"


def secret_env(name: str, secret: str, key: str) -> dict:
    return {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}


def build_application(*, name: str, namespace: str, runtime: str, spark_version: str,
                      wheel_url: str, parameters: dict, compute_profile: str,
                      wheel_sha256: str | None = None, spark_conf: dict | None = None,
                      entrypoint: str = "company_ingestion.entrypoint:main") -> dict:
    if compute_profile not in PROFILES:
        raise ValueError(f"Unknown compute profile {compute_profile!r}; choose {list(PROFILES)}")
    resources = PROFILES[compute_profile]
    config = deepcopy(parameters)
    config["run_id"] = name
    config["profile"] = compute_profile
    config.setdefault("planner", {})["task_slots"] = resources["executors"] * resources["executor_cores"]
    env = [
        {"name": "COMPANY_EXECUTION_CONFIG", "value": json.dumps(config, separators=(",", ":"))},
        {"name": "COMPANY_WHEEL_URL", "value": wheel_url},
        {"name": "COMPANY_ENTRYPOINT", "value": entrypoint},
        secret_env("SQLSERVER_USER", "sqlserver-credentials", "username"),
        secret_env("SQLSERVER_PASSWORD", "sqlserver-credentials", "password"),
        secret_env("AWS_ACCESS_KEY_ID", "seaweedfs-credentials", "access_key"),
        secret_env("AWS_SECRET_ACCESS_KEY", "seaweedfs-credentials", "secret_key"),
        {"name": "AWS_REGION", "value": "us-east-1"},
    ]
    if wheel_sha256:
        env.append({"name": "COMPANY_WHEEL_SHA256", "value": wheel_sha256})
    conf = {
        "spark.app.name": name,
        "spark.kubernetes.namespace": namespace,
        "spark.kubernetes.container.image": runtime,
        "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
        "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
        "spark.driver.cores": str(resources["driver_cores"]),
        "spark.driver.memory": resources["driver_memory"],
        "spark.driver.memoryOverhead": "512m",
        "spark.kubernetes.driver.request.cores": str(resources["driver_cores"]),
        "spark.kubernetes.driver.limit.cores": str(resources["driver_cores"]),
        "spark.executor.instances": str(resources["executors"]),
        "spark.executor.cores": str(resources["executor_cores"]),
        "spark.executor.memory": resources["executor_memory"],
        "spark.executor.memoryOverhead": "512m",
        "spark.kubernetes.executor.request.cores": str(resources["executor_cores"]),
        "spark.kubernetes.executor.limit.cores": str(resources["executor_cores"]),
        "spark.dynamicAllocation.enabled": "false",
        "spark.kubernetes.executor.deleteOnTermination": "true",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
        "spark.hadoop.fs.s3a.endpoint": "http://seaweedfs:8333",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "s3a://spark-events/",
        "spark.eventLog.rolling.enabled": "true",
        "spark.eventLog.rolling.maxFileSize": "128m",
        "spark.ui.prometheus.enabled": "true",
        "spark.executor.processTreeMetrics.enabled": "true",
        "spark.kubernetes.driver.label.company-run": name,
        "spark.kubernetes.executor.label.company-run": name,
        "spark.kubernetes.driver.label.company-profile": compute_profile,
        "spark.kubernetes.executor.label.company-profile": compute_profile,
        "spark.kubernetes.driver.podTemplateContainerName": "spark-kubernetes-driver",
        "spark.kubernetes.executor.podTemplateContainerName": "spark-kubernetes-executor",
    }
    conf.update(spark_conf or {})
    conf = {key: str(value) for key, value in conf.items()}
    driver_template = {"spec": {"containers": [{"name": "spark-kubernetes-driver", "env": env}]}}
    executor_env = [value for value in env if value["name"] in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"}]
    executor_template = {"spec": {"containers": [{"name": "spark-kubernetes-executor", "env": executor_env}]}}
    return {
        "apiVersion": "spark.apache.org/v1", "kind": "SparkApplication",
        "metadata": {"name": name, "namespace": namespace,
                     "labels": {"app.kubernetes.io/managed-by": "company-airflow", "company-profile": compute_profile}},
        "spec": {
            "pyFiles": "local:///opt/company/launcher.py",
            "runtimeVersions": {"sparkVersion": spark_version},
            "sparkConf": conf,
            "driverSpec": {"podTemplateSpec": driver_template},
            "executorSpec": {"podTemplateSpec": executor_template},
            "applicationTolerations": {
                "restartConfig": {"restartPolicy": "Never"},
                "resourceRetainPolicy": "Always",
                "resourceRetainDurationMillis": 900000,
                "ttlAfterStopMillis": 86400000,
                "instanceConfig": {"minExecutors": 1, "initExecutors": resources["executors"], "maxExecutors": resources["executors"]},
                "applicationTimeoutConfig": {"driverStartTimeoutMillis": 600000,
                                             "executorStartTimeoutMillis": 600000},
            },
        },
    }
