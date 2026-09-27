"""Render the minimal Kubernetes platform required by the Oracle E2E."""

from pathlib import Path
import yaml


ROOT = Path(__file__).resolve().parents[1]
NS = "spark-lab"


def metadata(name):
    return {"name": name, "namespace": NS}


def secret_env(name, secret_name, key):
    return {
        "name": name,
        "valueFrom": {"secretKeyRef": {"name": secret_name, "key": key}},
    }


objects = [
    {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}},
]


def pvc(name, size):
    objects.append({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": metadata(name),
        "spec": {"accessModes": ["ReadWriteOnce"],
                 "resources": {"requests": {"storage": size}}},
    })


def service(name, ports):
    objects.append({
        "apiVersion": "v1", "kind": "Service", "metadata": metadata(name),
        "spec": {"selector": {"app": name}, "ports": ports},
    })


pvc("seaweedfs-data", "3Gi")
objects.append({
    "apiVersion": "apps/v1", "kind": "Deployment",
    "metadata": metadata("seaweedfs"),
    "spec": {
        "replicas": 1, "strategy": {"type": "Recreate"},
        "selector": {"matchLabels": {"app": "seaweedfs"}},
        "template": {
            "metadata": {"labels": {"app": "seaweedfs"}},
            "spec": {"containers": [{
                "name": "seaweedfs", "image": "chrislusf/seaweedfs:4.47",
                "args": [
                    "server", "-dir=/data", "-ip=seaweedfs", "-ip.bind=0.0.0.0",
                    "-master.volumeSizeLimitMB=128", "-volume.max=256", "-s3",
                    "-s3.port=8333", "-s3.config=/etc/seaweedfs/s3.json",
                ],
                "ports": [{"containerPort": port} for port in
                          (8333, 8080, 9333, 8888, 19333, 18080, 18888)],
                "resources": {
                    "requests": {"cpu": "100m", "memory": "512Mi"},
                    "limits": {"memory": "2Gi"},
                },
                "readinessProbe": {"tcpSocket": {"port": 8333}, "periodSeconds": 5},
                "volumeMounts": [
                    {"name": "data", "mountPath": "/data"},
                    {"name": "auth", "mountPath": "/etc/seaweedfs", "readOnly": True},
                ],
            }], "volumes": [
                {"name": "data", "persistentVolumeClaim": {"claimName": "seaweedfs-data"}},
                {"name": "auth", "secret": {"secretName": "seaweedfs-auth"}},
            ]},
        },
    },
})
service("seaweedfs", [
    {"name": name, "port": port, "targetPort": port}
    for name, port in (("s3", 8333), ("volume", 8080), ("master", 9333),
                       ("filer", 8888), ("master-grpc", 19333),
                       ("volume-grpc", 18080), ("filer-grpc", 18888))
])

objects.append({
    "apiVersion": "apps/v1", "kind": "StatefulSet",
    "metadata": metadata("oracle"),
    "spec": {
        "serviceName": "oracle", "replicas": 1,
        "selector": {"matchLabels": {"app": "oracle"}},
        "template": {
            "metadata": {"labels": {"app": "oracle"}},
            "spec": {"securityContext": {"fsGroup": 54321}, "containers": [{
                "name": "oracle",
                "image": "gvenzl/oracle-free:23.26.3-slim-faststart@sha256:f5ff19033860d662c821cb04eb10483fa94f14f78eae252d054291ea07028093",
                "ports": [{"name": "listener", "containerPort": 1521}],
                "env": [
                    secret_env("ORACLE_PASSWORD", "oracle-admin", "password"),
                    secret_env("APP_USER", "oracle-credentials", "username"),
                    secret_env("APP_USER_PASSWORD", "oracle-credentials", "password"),
                ],
                "resources": {
                    "requests": {"cpu": "500m", "memory": "2Gi"},
                    "limits": {"cpu": "2", "memory": "4Gi"},
                },
                "readinessProbe": {
                    "exec": {"command": ["/bin/bash", "-c", "/opt/oracle/healthcheck.sh"]},
                    "initialDelaySeconds": 20, "periodSeconds": 10,
                    "timeoutSeconds": 5, "failureThreshold": 60,
                },
            }]},
        },
    },
})
service("oracle", [{"name": "listener", "port": 1521, "targetPort": 1521}])

for name in ("ingestion-driver", "ingestion-worker"):
    objects.append({
        "apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata(name),
    })
objects.extend([
    {
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
        "metadata": metadata("ingestion-driver"),
        "rules": [
            {"apiGroups": ["batch"], "resources": ["jobs", "jobs/status"],
             "verbs": ["get", "list", "watch", "create", "delete"]},
            {"apiGroups": [""], "resources": ["pods"],
             "verbs": ["get", "list", "watch"]},
        ],
    },
    {
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
        "metadata": metadata("ingestion-driver"),
        "subjects": [{"kind": "ServiceAccount", "name": "ingestion-driver",
                      "namespace": NS}],
        "roleRef": {"kind": "Role", "name": "ingestion-driver",
                    "apiGroup": "rbac.authorization.k8s.io"},
    },
])

pvc("pushgateway-data", "256Mi")
objects.append({
    "apiVersion": "apps/v1", "kind": "Deployment",
    "metadata": metadata("pushgateway"),
    "spec": {
        "replicas": 1, "selector": {"matchLabels": {"app": "pushgateway"}},
        "template": {"metadata": {"labels": {"app": "pushgateway"}}, "spec": {
            "containers": [{
                "name": "pushgateway", "image": "prom/pushgateway:v1.11.3",
                "args": ["--persistence.file=/data/metrics"],
                "ports": [{"containerPort": 9091}],
                "resources": {"requests": {"cpu": "50m", "memory": "32Mi"},
                              "limits": {"memory": "256Mi"}},
                "volumeMounts": [{"name": "data", "mountPath": "/data"}],
            }], "volumes": [{"name": "data", "persistentVolumeClaim": {
                "claimName": "pushgateway-data"}}],
        }},
    },
})
service("pushgateway", [{"name": "http", "port": 9091, "targetPort": 9091}])

output = ROOT / "infrastructure/local/oracle/lab.yaml"
output.write_text(yaml.safe_dump_all(objects, sort_keys=False))
print(output)
