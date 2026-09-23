#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .local/logs
python3 tools/install-tools.py
export PATH="$PWD/.tools/bin:$PATH"
if [ ! -x .venv/bin/python ]; then python3 -m venv .venv; fi
.venv/bin/pip install -r requirements-dev.txt
minikube start -p company-spark --driver=docker --container-runtime=containerd \
  --kubernetes-version=v1.36.4 --cpus=10 --memory=22000 --disk-size=25g --addons=metrics-server
minikube addons enable dashboard -p company-spark
python3 tools/configure-secrets.py
python3 tools/fetch-jars.py
.venv/bin/python -m build --wheel --outdir dist libraries/ingestion-core
docker build -f engines/spark/Dockerfile -t company-spark-runtime:0.1.0 .
# Stream directly, avoiding a second archive/cache on the host.
docker save company-spark-runtime:0.1.0 | docker exec -i company-spark ctr -n k8s.io images import -
docker build -f engines/dlt/Dockerfile -t company-dlt-ingestion:0.6.0 .
docker save company-dlt-ingestion:0.6.0 | docker exec -i company-spark ctr -n k8s.io images import -
.venv/bin/python -m build --wheel --outdir dist engines/spark/job
.venv/bin/python -m build --wheel --outdir dist engines/dlt
docker build -f orchestration/airflow/Dockerfile -t company-airflow:0.1.0 .
docker save company-airflow:0.1.0 | docker exec -i company-spark ctr -n k8s.io images import -
.venv/bin/python tools/render-platform.py
.venv/bin/python tools/render-observability.py
helm repo add apache-spark https://apache.github.io/spark-kubernetes-operator --force-update
helm upgrade --install spark-operator apache-spark/spark-kubernetes-operator --version 1.8.0 \
  --kube-context company-spark --namespace spark-operator --create-namespace -f infrastructure/local/kubernetes/spark-operator-values.yaml
kubectl --context company-spark apply -f infrastructure/local/kubernetes/platform.yaml
python3 tools/publish-artifacts.py
kubectl --context company-spark -n spark-lab rollout status deployment/seaweedfs --timeout=300s
python3 tools/port-forwards.py
.venv/bin/python tools/initialize-storage.py
kubectl --context company-spark apply -f infrastructure/local/kubernetes/observability/manifests.yaml
python3 tools/generate-seed.py
python3 tools/seed-sqlserver.py
kubectl --context company-spark -n spark-lab rollout status deployment/airflow --timeout=300s
kubectl --context company-spark -n spark-lab rollout status deployment/grafana --timeout=300s
python3 tools/port-forwards.py
echo 'Lab ready. Airflow http://localhost:8080 — Grafana http://localhost:3000/d/company-spark'
