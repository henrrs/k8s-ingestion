"""Submit an Oracle Driver Job directly and persist its benchmark result."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]
KUBECTL = ROOT / ".tools/bin/kubectl"
BASE = [str(KUBECTL), "--context", "company-spark", "-n", "spark-lab"]


def secret_env(name, secret, key):
    return {"name": name, "secret": secret, "key": key}


def job_manifest(run_id, workers, timeout_seconds):
    secrets = [
        secret_env("ORACLE_USER", "oracle-credentials", "username"),
        secret_env("ORACLE_PASSWORD", "oracle-credentials", "password"),
        secret_env("AWS_ACCESS_KEY_ID", "seaweedfs-credentials", "access_key"),
        secret_env("AWS_SECRET_ACCESS_KEY", "seaweedfs-credentials", "secret_key"),
    ]
    config = {
        "run_id": run_id,
        "profile": f"oracle-{workers}-workers",
        "source": {
            "type": "oracle", "host": "oracle", "port": 1521,
            "service_name": "FREEPDB1", "database": "FREEPDB1",
            "schema": "BENCHMARK", "table": "WIDE",
            "query_timeout_seconds": 1800,
        },
        "destination": {
            "format": "delta", "uri": "s3://lakehouse/oracle/bronze/wide",
        },
        "execution": {
            "max_workers": workers,
            "max_source_connections": workers,
            "max_chunks": 64,
            "extract_backend": "oracle_arrow",
            "target_chunk_bytes": "auto",
            "fetch_size": "auto",
            "publication_mode": "auto",
            "chunk_retries": 2,
        },
        "storage": {"type": "s3", "endpoint_url": "http://seaweedfs:8333"},
        "metrics": {
            "output_uri": "s3://metrics/runs-dlt-distributed",
            "pushgateway": "http://pushgateway:9091",
        },
        "validation": {"require_source_row_match": True},
        "_orchestration": {
            "image": "company-dlt-ingestion:0.6.0",
            "namespace": "spark-lab",
            "worker_service_account": "ingestion-worker",
            "worker_resources": {"cpu": "1", "memory": "2Gi"},
            "worker_env": {"AWS_REGION": "us-east-1", "DLT_TELEMETRY": "false"},
            "secret_env": secrets,
            "timeout_seconds": timeout_seconds,
        },
    }
    env = [
        {"name": "COMPANY_JOB_CONFIG", "value": json.dumps(config, separators=(",", ":"))},
        {"name": "AWS_REGION", "value": "us-east-1"},
        {"name": "DLT_TELEMETRY", "value": "false"},
    ]
    for item in secrets:
        env.append({
            "name": item["name"],
            "valueFrom": {"secretKeyRef": {
                "name": item["secret"], "key": item["key"],
            }},
        })
    labels = {
        "app.kubernetes.io/managed-by": "oracle-e2e-tool",
        "company-run": run_id,
        "company-role": "ingestion-driver",
    }
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": run_id, "namespace": "spark-lab", "labels": labels},
        "spec": {
            "backoffLimit": 1,
            "activeDeadlineSeconds": timeout_seconds,
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "serviceAccountName": "ingestion-driver",
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "driver", "image": "company-dlt-ingestion:0.6.0",
                        "imagePullPolicy": "IfNotPresent", "args": ["driver"],
                        "env": env,
                        "resources": {
                            "requests": {"cpu": "250m", "memory": "512Mi"},
                            "limits": {"cpu": "1", "memory": "1Gi"},
                        },
                        "securityContext": {
                            "allowPrivilegeEscalation": False,
                            "capabilities": {"drop": ["ALL"]},
                        },
                    }],
                },
            },
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.workers <= 0:
        raise ValueError("--workers must be positive")
    generated = datetime.now(timezone.utc).strftime("oracle-wide-%Y%m%d%H%M%S")
    run_id = args.run_id or generated
    if not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", run_id):
        raise ValueError("--run-id must be a DNS-compatible Kubernetes name")

    manifest = job_manifest(run_id, args.workers, args.timeout_seconds)
    subprocess.run(
        BASE + ["apply", "-f", "-"],
        input=json.dumps(manifest), text=True, check=True,
    )
    print(f"Oracle E2E run: {run_id}", flush=True)
    logs = subprocess.run(
        BASE + ["logs", "-f", f"job/{run_id}", "--pod-running-timeout=600s"],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    print(logs.stdout, end="", flush=True)
    completed = subprocess.run(
        BASE + ["wait", "--for=condition=complete", f"job/{run_id}",
                f"--timeout={args.timeout_seconds}s"],
    )
    marker = "COMPANY_INGESTION_RESULT="
    result_lines = [line for line in logs.stdout.splitlines() if marker in line]
    if completed.returncode or not result_lines:
        subprocess.run(BASE + ["describe", "job", run_id], check=False)
        raise RuntimeError(f"Oracle E2E run failed: {run_id}")
    result = json.loads(result_lines[-1].split(marker, 1)[1])
    output = ROOT / ".local/benchmarks" / f"{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        f"Result: rows={result['rows']} seconds="
        f"{result['durations_seconds']['engine_total']:.3f} "
        f"throughput={result['throughput_rows_per_second']:.0f} rows/s "
        f"output={result['output_bytes']} bytes",
        flush=True,
    )
    print(f"Saved: {output}", flush=True)


if __name__ == "__main__":
    main()
