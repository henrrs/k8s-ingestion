"""Expose the newest running Spark driver's native Web UI on localhost:4040."""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import time

root = Path(__file__).resolve().parents[1]
kubectl = str(root / ".tools/bin/kubectl")


def port_open(port):
    with socket.socket() as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def running_driver(application=None):
    result = subprocess.run(
        [kubectl, "--context", "company-spark", "-n", "spark-lab", "get", "pods",
         "-l", "spark-role=driver", "-o", "json"],
        check=True, capture_output=True, text=True,
    )
    candidates = []
    for pod in json.loads(result.stdout)["items"]:
        labels = pod["metadata"].get("labels", {})
        if pod.get("status", {}).get("phase") != "Running":
            continue
        if application and labels.get("company-run") != application:
            continue
        candidates.append(pod)
    if not candidates:
        return None
    return max(candidates, key=lambda pod: pod["metadata"]["creationTimestamp"])["metadata"]["name"]


def wait_for_ui(pod, attempts=60):
    # Kubernetes marks the container Running before SparkContext starts Jetty.
    for _ in range(attempts):
        probe = subprocess.run(
            [kubectl, "--context", "company-spark", "-n", "spark-lab", "exec", pod,
             "--", "python3", "-c",
             "import socket; s=socket.create_connection(('127.0.0.1',4040),.5); s.close()"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if probe.returncode == 0:
            return True
        time.sleep(0.5)
    return False


def start_forward(pod):
    log_path = root / ".local/logs/port-4040.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = log_path.open("a")
    process = subprocess.Popen(
        [kubectl, "--context", "company-spark", "-n", "spark-lab", "port-forward",
         f"pod/{pod}", "4040:4040", "--address", "127.0.0.1"],
        stdin=subprocess.DEVNULL, stdout=log, stderr=log,
    )
    log.close()
    return process, log_path


def expose_once(application, wait):
    deadline = time.monotonic() + wait
    pod = running_driver(application)
    while pod is None and time.monotonic() < deadline:
        time.sleep(2)
        pod = running_driver(application)
    if pod is None:
        raise SystemExit("No running Spark driver found. Trigger the Airflow DAG and run this command while its task is active.")
    if port_open(4040):
        raise SystemExit("localhost:4040 is already in use; stop the previous tunnel before creating another one.")
    if not wait_for_ui(pod):
        raise SystemExit(f"Spark driver {pod} is running, but its Web UI did not become ready on port 4040.")
    process, log_path = start_forward(pod)
    for _ in range(20):
        if port_open(4040):
            print(f"Spark live UI for {pod}: http://localhost:4040")
            print("The URL remains available only while this driver is running.")
            return
        if process.poll() is not None:
            raise SystemExit(f"Could not expose the Spark UI; see {log_path}")
        time.sleep(0.25)
    process.terminate()
    raise SystemExit(f"Timed out exposing the Spark UI; see {log_path}")


def watch():
    current_pod = None
    forward = None
    while True:
        try:
            pod = running_driver()
        except subprocess.CalledProcessError:
            time.sleep(2)
            continue
        if forward is not None and forward.poll() is not None:
            forward = None
            current_pod = None
        if pod != current_pod:
            if forward is not None:
                forward.terminate()
                try:
                    forward.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    forward.kill()
                forward = None
            current_pod = None
            if pod and not port_open(4040) and wait_for_ui(pod):
                forward, _ = start_forward(pod)
                current_pod = pod
                print(f"Following {pod} at http://localhost:4040", flush=True)
        time.sleep(2)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--application", help="Company Spark application name; newest running driver is used when omitted")
parser.add_argument("--wait", type=int, default=0, help="seconds to wait for a running driver")
parser.add_argument("--watch", action="store_true", help="follow each new driver continuously")
args = parser.parse_args()
if args.watch:
    watch()
else:
    expose_once(args.application, args.wait)
