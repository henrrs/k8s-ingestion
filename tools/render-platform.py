"""Render the local laboratory manifests. Secrets are generated separately."""
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
NS = "spark-lab"
objects = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}]

def add(kind, name, **fields):
    objects.append({"apiVersion": "apps/v1" if kind == "Deployment" else "v1", "kind": kind,
                    "metadata": {"name": name, "namespace": NS}, **fields})

def secret(name, secret, key):
    return {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}

def volume(name, size):
    add("PersistentVolumeClaim", name, spec={"accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": size}}})

def service(name, port, target=None):
    add("Service", name, spec={"selector": {"app": name}, "ports": [{"port": port, "targetPort": target or port}]})

def deployment(name, image, port, *, command=None, args=None, env=None, mounts=None,
               volumes=None, memory="512Mi", cpu="100m", limit_memory="1Gi", sa=None,
               security=None, readiness=None):
    container = {"name": name, "image": image, "imagePullPolicy": "IfNotPresent",
                 "ports": [{"containerPort": port}], "resources": {
                     "requests": {"cpu": cpu, "memory": memory},
                     "limits": {"memory": limit_memory}}, "env": env or []}
    for key, value in (("command", command), ("args", args), ("volumeMounts", mounts), ("readinessProbe", readiness)):
        if value is not None: container[key] = value
    spec = {"containers": [container], "volumes": volumes or []}
    if sa: spec["serviceAccountName"] = sa
    if security: spec["securityContext"] = security
    add("Deployment", name, spec={"replicas": 1, "strategy": {"type": "Recreate"},
        "selector": {"matchLabels": {"app": name}},
        "template": {"metadata": {"labels": {"app": name}}, "spec": spec}})
    service(name, port)

# The production artifact is still the versioned Airflow image. For the local
# lab, mount the same checked-in provider/DAG source as a ConfigMap as well. This
# keeps iterative `kubectl apply` changes reproducible without transferring the
# 2.3 GiB Airflow base image on every edit.
live_files = {}
live_items = []
for base, target in [
    (ROOT / "orchestration/airflow/provider/src/company_airflow", "company_airflow"),
    (ROOT / "orchestration/airflow/dags", "dags"),
]:
    for path in sorted(base.rglob("*.py")):
        relative = path.relative_to(base)
        key = (target + "__" + str(relative)).replace("/", "__")
        live_files[key] = path.read_text()
        live_items.append({"key": key, "path": f"{target}/{relative}"})
add("ConfigMap", "airflow-live-code", data=live_files)

volume("sqlserver-data", "3Gi")
deployment("sqlserver", "mcr.microsoft.com/mssql/server:2025-latest@sha256:2b5b581621126574f3d1f75e78d3eebe8d05aedb59ad0cfdf9aa42cb0634d726", 1433,
    env=[{"name": "ACCEPT_EULA", "value": "Y"}, {"name": "MSSQL_PID", "value": "Developer"},
         {"name": "MSSQL_MEMORY_LIMIT_MB", "value": "3072"}, secret("MSSQL_SA_PASSWORD", "sqlserver-admin", "password")],
    mounts=[{"name": "data", "mountPath": "/var/opt/mssql"}], volumes=[{"name": "data", "persistentVolumeClaim": {"claimName": "sqlserver-data"}}],
    memory="3Gi", cpu="500m", limit_memory="4Gi", security={"fsGroup": 10001},
    readiness={"tcpSocket": {"port": 1433}, "initialDelaySeconds": 20, "periodSeconds": 5})

volume("seaweedfs-data", "3Gi")
deployment("seaweedfs", "chrislusf/seaweedfs:4.47", 8333,
    args=["server", "-dir=/data", "-ip=seaweedfs", "-ip.bind=0.0.0.0", "-master.volumeSizeLimitMB=128",
          "-volume.max=256", "-s3", "-s3.port=8333", "-s3.config=/etc/seaweedfs/s3.json"],
    mounts=[{"name": "data", "mountPath": "/data"}, {"name": "auth", "mountPath": "/etc/seaweedfs", "readOnly": True}],
    volumes=[{"name": "data", "persistentVolumeClaim": {"claimName": "seaweedfs-data"}},
             {"name": "auth", "secret": {"secretName": "seaweedfs-auth"}}],
    # Multipart Delta writes can buffer several chunks concurrently in the
    # all-in-one S3/filer/volume process. One GiB OOMs on the wide fixture.
    memory="512Mi", limit_memory="2Gi", readiness={"tcpSocket": {"port": 8333}, "periodSeconds": 5})
# The all-in-one server advertises this Service to its S3 gateway and filer.
# Expose internal volume/master/filer ports as well as the public S3 endpoint.
objects[-1]["spec"]["ports"] = [
    {"name": name, "port": port, "targetPort": port}
    for name, port in [("s3", 8333), ("volume", 8080), ("master", 9333),
                       ("filer", 8888), ("master-grpc", 19333),
                       ("volume-grpc", 18080), ("filer-grpc", 18888)]
]

deployment("artifacts", "python:3.12.12-slim", 8080,
    command=["python", "-m", "http.server", "8080", "--directory", "/artifacts"],
    mounts=[{"name": "wheel", "mountPath": "/artifacts", "readOnly": True}],
    volumes=[{"name": "wheel", "configMap": {"name": "ingestion-artifacts"}}], memory="32Mi", limit_memory="128Mi")

volume("airflow-data", "1Gi")
deployment("airflow", "company-airflow:0.1.0", 8080,
    command=["/bin/bash", "-ec"],
    args=["cp /opt/airflow/auth/passwords.json /opt/airflow/data/passwords.json; exec airflow standalone"],
    env=[{"name": "AIRFLOW__CORE__LOAD_EXAMPLES", "value": "false"},
         {"name": "AIRFLOW__CORE__EXECUTOR", "value": "LocalExecutor"},
         {"name": "AIRFLOW__CORE__PARALLELISM", "value": "2"},
         {"name": "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", "value": "false"},
         {"name": "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS", "value": "admin:admin"},
         {"name": "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE", "value": "/opt/airflow/data/passwords.json"},
         {"name": "AIRFLOW__CORE__DAGS_FOLDER", "value": "/opt/company-live/dags"},
         {"name": "PYTHONPATH", "value": "/opt/company-live"},
         {"name": "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "value": "sqlite:////opt/airflow/data/airflow.db"},
         {"name": "AIRFLOW__LOGGING__BASE_LOG_FOLDER", "value": "/opt/airflow/data/logs"},
         {"name": "AIRFLOW__API__BASE_URL", "value": "http://localhost:8080"},
         {"name": "AIRFLOW__CORE__EXECUTION_API_SERVER_URL", "value": "http://localhost:8080/execution/"},
         secret("AIRFLOW__API_AUTH__JWT_SECRET", "airflow-auth", "jwt-secret"),
         {"name": "COMPANY_WHEEL_SHA256", "valueFrom": {"configMapKeyRef": {"name": "ingestion-release", "key": "sha256"}}}],
    mounts=[{"name": "data", "mountPath": "/opt/airflow/data"},
            {"name": "auth", "mountPath": "/opt/airflow/auth", "readOnly": True},
            {"name": "live-code", "mountPath": "/opt/company-live", "readOnly": True}],
    volumes=[{"name": "data", "persistentVolumeClaim": {"claimName": "airflow-data"}},
             {"name": "auth", "secret": {"secretName": "airflow-auth", "items": [{"key": "passwords.json", "path": "passwords.json"}]}},
             {"name": "live-code", "configMap": {"name": "airflow-live-code", "items": live_items}}],
    memory="1Gi", cpu="500m", limit_memory="2500Mi", sa="airflow", security={"fsGroup": 0},
    readiness={"httpGet": {"path": "/api/v2/monitor/health", "port": 8080}, "initialDelaySeconds": 30, "periodSeconds": 10})

for name in ["airflow", "ingestion-driver", "ingestion-worker"]:
    add("ServiceAccount", name)
objects.extend([
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "spark-cleanup", "namespace": NS},
     "rules": [{"apiGroups": [""], "resources": ["pods", "services", "configmaps", "persistentvolumeclaims"], "verbs": ["deletecollection"]}]},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "spark-cleanup", "namespace": NS},
     "subjects": [{"kind": "ServiceAccount", "name": "spark", "namespace": NS}],
     "roleRef": {"kind": "Role", "name": "spark-cleanup", "apiGroup": "rbac.authorization.k8s.io"}},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "airflow-spark", "namespace": NS},
     "rules": [{"apiGroups": ["spark.apache.org"], "resources": ["sparkapplications", "sparkapplications/status"], "verbs": ["get", "list", "watch", "create", "delete"]},
               {"apiGroups": ["batch"], "resources": ["jobs", "jobs/status"], "verbs": ["get", "list", "watch", "create", "delete"]},
               {"apiGroups": [""], "resources": ["pods", "pods/log"], "verbs": ["get", "list", "watch"]}]},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "airflow-spark", "namespace": NS},
     "subjects": [{"kind": "ServiceAccount", "name": "airflow", "namespace": NS}],
     "roleRef": {"kind": "Role", "name": "airflow-spark", "apiGroup": "rbac.authorization.k8s.io"}},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "ingestion-driver", "namespace": NS},
     "rules": [{"apiGroups": ["batch"], "resources": ["jobs", "jobs/status"],
                "verbs": ["get", "list", "watch", "create", "delete"]},
               {"apiGroups": [""], "resources": ["pods"],
                "verbs": ["get", "list", "watch"]}]},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "ingestion-driver", "namespace": NS},
     "subjects": [{"kind": "ServiceAccount", "name": "ingestion-driver", "namespace": NS}],
     "roleRef": {"kind": "Role", "name": "ingestion-driver", "apiGroup": "rbac.authorization.k8s.io"}},
])

out = ROOT / "infrastructure/local/kubernetes/platform.yaml"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(yaml.safe_dump_all(objects, sort_keys=False).replace("value: Y\n", "value: 'Y'\n"))
print(out)
