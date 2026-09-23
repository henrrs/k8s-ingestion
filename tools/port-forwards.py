"""Start loopback-only UI tunnels; rerun after restarting cluster services."""
import json
import os
from pathlib import Path
import socket
import subprocess
import time

root = Path(__file__).resolve().parents[1]
services = {
    # label: (namespace, local port, service port, Kubernetes Service)
    "airflow": ("spark-lab", 8080, 8080, "airflow"),
    "grafana": ("spark-lab", 3000, 3000, "grafana"),
    "prometheus": ("spark-lab", 9090, 9090, "prometheus"),
    "pushgateway": ("spark-lab", 9091, 9091, "pushgateway"),
    "spark-history": ("spark-lab", 18080, 18080, "spark-history"),
    "seaweedfs-s3": ("spark-lab", 8333, 8333, "seaweedfs"),
    "seaweedfs-filer": ("spark-lab", 8888, 8888, "seaweedfs"),
    "sqlserver": ("spark-lab", 1433, 1433, "sqlserver"),
    "kubernetes-dashboard": ("kubernetes-dashboard", 8001, 80, "kubernetes-dashboard"),
}
pids = {}
for label, (namespace, port, service_port, service) in services.items():
    with socket.socket() as sock:
        if sock.connect_ex(("127.0.0.1", port)) == 0:
            print(f"Port {port} already listening")
            continue
    with (root / f".local/logs/port-{port}.log").open("a") as log:
        process = subprocess.Popen([str(root / ".tools/bin/kubectl"), "--context", "company-spark", "-n", namespace,
                                    "port-forward", f"service/{service}", f"{port}:{service_port}", "--address", "127.0.0.1"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        pids[label] = process.pid

# Keep localhost:4040 attached to the newest live Spark driver. Unlike the
# fixed services above, driver pods are ephemeral and change for every task.
watcher_pid_path = root / ".local/spark-ui-watcher.pid"
watcher_running = False
if watcher_pid_path.exists():
    try:
        os.kill(int(watcher_pid_path.read_text()), 0)
        watcher_running = True
    except (ValueError, ProcessLookupError, PermissionError):
        pass
if not watcher_running:
    with (root / ".local/logs/spark-ui-watcher.log").open("a") as log:
        watcher = subprocess.Popen(
            ["python3", str(root / "tools/spark-ui.py"), "--watch"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
        )
    watcher_pid_path.write_text(str(watcher.pid))
    pids["spark-ui-watcher"] = watcher.pid
(root / ".local/port-forward-pids.json").write_text(json.dumps(pids))
time.sleep(1)
for label, (_, port, _, _) in services.items():
    endpoint = f"localhost:{port} (TDS/TCP)" if label == "sqlserver" else f"http://localhost:{port}"
    print(f"{label}: {endpoint}")
