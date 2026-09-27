#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

rows="${ROWS:-1000000}"
workers="${WORKERS:-4}"
cluster="company-spark"
namespace="spark-lab"

# Minikube deliberately rejects the Docker driver as root. This also covers a
# user who entered `sudo -s` before running the laboratory: resume as the owner
# of the checkout and use docker as the primary group for this process.
if [ "$(id -u)" -eq 0 ]; then
  repo_owner="$(stat -c '%U' "$repo_root")"
  if [ "$repo_owner" = "root" ]; then
    echo "The repository is owned by root; run this bootstrap as a regular user." >&2
    exit 1
  fi
  repo_home="$(getent passwd "$repo_owner" | cut -d: -f6)"
  exec runuser -u "$repo_owner" -g docker -- env \
    HOME="$repo_home" ROWS="$rows" WORKERS="$workers" \
    bash "$repo_root/tools/bootstrap-oracle-e2e.sh"
fi

# A process started before setup-host.sh does not inherit the newly added
# docker group. Re-enter only this bootstrap under that group, so automation
# can continue without a logout or a privileged Docker daemon invocation.
if command -v docker >/dev/null 2>&1 \
  && ! docker info >/dev/null 2>&1 \
  && getent group docker | cut -d: -f4 | tr ',' '\n' | grep -Fxq "$USER"; then
  exec sg docker -c \
    "cd '$repo_root' && ROWS='$rows' WORKERS='$workers' bash tools/bootstrap-oracle-e2e.sh"
fi

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "Docker is not installed or the current user cannot access it." >&2
  echo "Run: bash tools/setup-host.sh" >&2
  echo "Then open a new terminal and run this script again." >&2
  exit 1
fi

python3 tools/install-tools.py
export PATH="$repo_root/.tools/bin:$PATH"

if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
fi
if ! .venv/bin/python -m pip --version >/dev/null 2>&1; then
  .venv/bin/python -m ensurepip --upgrade
fi
.venv/bin/python -m pip install -q -r requirements-dev.txt

if ! minikube status -p "$cluster" >/dev/null 2>&1; then
  minikube start -p "$cluster" --driver=docker --container-runtime=containerd \
    --kubernetes-version=v1.36.4 --cpus=10 --memory=22000 --disk-size=30g \
    --addons=metrics-server
fi

python3 tools/configure-secrets.py
.venv/bin/python tools/render-oracle-lab.py

echo "Building the Oracle-enabled ingestion image..."
docker build -f engines/dlt/Dockerfile -t company-dlt-ingestion:0.6.0 .
minikube image load -p "$cluster" company-dlt-ingestion:0.6.0

kubectl --context "$cluster" apply -f infrastructure/local/oracle/lab.yaml
kubectl --context "$cluster" -n "$namespace" rollout status deployment/seaweedfs --timeout=600s
kubectl --context "$cluster" -n "$namespace" rollout status statefulset/oracle --timeout=1200s
kubectl --context "$cluster" -n "$namespace" rollout status deployment/pushgateway --timeout=300s
# The faststart image carries its prepared database in the image filesystem.
# Older manifests mounted this path from a PVC, which hides those files. Remove
# that now-unused laboratory volume after the updated pod has rolled out.
kubectl --context "$cluster" -n "$namespace" delete pvc oracle-data \
  --ignore-not-found --wait=false

port_log="$repo_root/.local/logs/oracle-e2e-seaweed-port-forward.log"
mkdir -p "$(dirname "$port_log")"
kubectl --context "$cluster" -n "$namespace" port-forward \
  service/seaweedfs 8333:8333 >"$port_log" 2>&1 &
port_pid=$!
trap 'kill "$port_pid" 2>/dev/null || true' EXIT
.venv/bin/python - <<'PY'
import socket, time
for _ in range(120):
    try:
        with socket.create_connection(("127.0.0.1", 8333), timeout=1):
            break
    except OSError:
        time.sleep(1)
else:
    raise SystemExit("SeaweedFS port-forward did not become ready")
PY
.venv/bin/python tools/initialize-storage.py

python3 tools/seed-oracle.py --rows "$rows"
python3 tools/run-oracle-e2e.py --workers "$workers"

echo "Oracle E2E completed successfully."
