import base64
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib

root = Path(__file__).resolve().parents[1]
project = tomllib.loads((root / "engines/spark/job/pyproject.toml").read_text())
version = project["project"]["version"]
wheel = root / f"dist/company_ingestion-{version}-py3-none-any.whl"
data = wheel.read_bytes()
digest = hashlib.sha256(data).hexdigest()
kubectl = [str(root / ".tools/bin/kubectl"), "--context", "company-spark", "-n", "spark-lab"]
for body in [
    {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "ingestion-artifacts", "namespace": "spark-lab"}, "binaryData": {wheel.name: base64.b64encode(data).decode()}},
    {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "ingestion-release", "namespace": "spark-lab"}, "data": {"sha256": digest}},
]:
    subprocess.run(kubectl + ["apply", "-f", "-"], input=json.dumps(body).encode(), check=True)
subprocess.run(kubectl + ["rollout", "restart", "deployment/artifacts", "deployment/airflow"], check=True)
print(f"Published {wheel.name}: sha256={digest}")
