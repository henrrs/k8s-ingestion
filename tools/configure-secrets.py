"""Create local credentials once, then apply Secrets without exposing values."""
import json
import os
from pathlib import Path
import secrets
import subprocess

root = Path(__file__).resolve().parents[1]
path = root / ".local/credentials.json"
path.parent.mkdir(exist_ok=True)
credential_keys = [
    "sqlserver_sa", "sqlserver_ingestion", "oracle_admin",
    "oracle_ingestion", "s3_secret", "airflow", "grafana", "jwt",
]
if not path.exists():
    values = {key: "Lab-" + secrets.token_hex(16) for key in credential_keys}
    values["s3_access"] = "company-local"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f: json.dump(values, f, indent=2)
values = json.loads(path.read_text())
changed = False
for key in credential_keys:
    if key not in values:
        values[key] = "Lab-" + secrets.token_hex(16)
        changed = True
if "s3_access" not in values:
    values["s3_access"] = "company-local"
    changed = True
if changed:
    path.write_text(json.dumps(values, indent=2) + "\n")
    path.chmod(0o600)
mapping = {
    "sqlserver-admin": {"password": values["sqlserver_sa"]},
    "sqlserver-credentials": {"username": "ingestion", "password": values["sqlserver_ingestion"]},
    "oracle-admin": {"password": values["oracle_admin"]},
    "oracle-credentials": {"username": "benchmark", "password": values["oracle_ingestion"]},
    "seaweedfs-credentials": {"access_key": values["s3_access"], "secret_key": values["s3_secret"]},
    "seaweedfs-auth": {"s3.json": json.dumps({"identities": [{"name": "company-local", "credentials": [{"accessKey": values["s3_access"], "secretKey": values["s3_secret"]}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]})},
    "airflow-auth": {"passwords.json": json.dumps({"admin": values["airflow"]}), "jwt-secret": values["jwt"]},
    "grafana-credentials": {"password": values["grafana"]},
}
kubectl = str(root / ".tools/bin/kubectl")
subprocess.run([kubectl, "--context", "company-spark", "create", "namespace", "spark-lab"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
for name, data in mapping.items():
    body = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": "spark-lab"}, "type": "Opaque", "stringData": data}
    subprocess.run([kubectl, "--context", "company-spark", "apply", "-f", "-"], input=json.dumps(body).encode(), check=True, stdout=subprocess.DEVNULL)
print("Local credentials saved in .local/credentials.json (mode 0600); Kubernetes Secrets configured.")
