#!/usr/bin/env python3
"""Submit JDBC and mssql-python/mapInArrow Spark benchmarks without Airflow."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "orchestration/airflow/provider/src"))

from company_airflow.application import build_application  # noqa: E402
from company_airflow.status import application_result  # noqa: E402


KUBECTL = [
    str(ROOT / ".tools/bin/kubectl"), "--context", "company-spark",
    "-n", "spark-lab",
]


def kubectl(*args, input_value=None, check=True):
    return subprocess.run(
        KUBECTL + list(args), input=input_value, text=True,
        capture_output=True, check=check,
    )


def application(name, mode, args, wheel_sha):
    spark_conf = {}
    if args.s3a_upload_profile == "disk-multipart":
        spark_conf.update({
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
            "spark.hadoop.fs.s3a.multipart.size": str(64 * 1024 * 1024),
            "spark.hadoop.fs.s3a.multipart.threshold": str(64 * 1024 * 1024),
            "spark.hadoop.fs.s3a.threads.max": "4",
            "spark.hadoop.fs.s3a.connection.maximum": "16",
        })
    if args.executor_memory is not None:
        spark_conf["spark.executor.memory"] = args.executor_memory
    if args.executor_memory_overhead is not None:
        spark_conf["spark.executor.memoryOverhead"] = args.executor_memory_overhead
    body = build_application(
        name=name,
        namespace="spark-lab",
        runtime="company-spark-runtime:0.4.0",
        spark_version="4.2.0",
        wheel_url="http://artifacts:8080/company_ingestion-0.10.0-py3-none-any.whl",
        wheel_sha256=wheel_sha,
        compute_profile=args.profile,
        parameters={
            "source": {
                "type": "sqlserver",
                "host": "sqlserver",
                "database": "Benchmark",
                "schema": "dbo",
                "table": args.table,
                "trust_server_certificate": True,
                "read_mode": mode,
            },
            "destination": {
                "format": "delta",
                "uri": f"s3a://lakehouse/spark-reader-benchmark/{mode}/{args.table}",
                "compression": args.compression,
                "write_mode": args.write_mode,
            },
            "planner": {
                "max_connections": args.max_connections,
                "target_partition_bytes": args.target_partition_bytes,
                "max_partition_oversubscription": args.partition_oversubscription,
            },
            "benchmark": {
                "isolate_io_phases": args.isolate_io_phases,
                "writer_matrix": args.writer_matrix,
            },
            "metrics": {
                "pushgateway": "http://pushgateway:9091",
                "output_uri": "s3a://metrics/runs",
            },
        },
        spark_conf=spark_conf,
    )
    if args.output_partitions is not None or args.target_file_mib is not None:
        env = body["spec"]["driverSpec"]["podTemplateSpec"]["spec"]["containers"][0]["env"]
        config_env = next(item for item in env if item["name"] == "COMPANY_EXECUTION_CONFIG")
        execution_config = json.loads(config_env["value"])
        if args.output_partitions is not None:
            execution_config["destination"]["output_partitions"] = args.output_partitions
        if args.target_file_mib is not None:
            execution_config["destination"]["target_file_bytes"] = args.target_file_mib * 1024 * 1024
        config_env["value"] = json.dumps(execution_config, separators=(",", ":"))
    if args.fetch_batch_mib is not None:
        planner = json.loads(
            next(
                item["value"]
                for item in body["spec"]["driverSpec"]["podTemplateSpec"]["spec"]["containers"][0]["env"]
                if item["name"] == "COMPANY_EXECUTION_CONFIG"
            )
        )["planner"]
        planner["target_fetch_batch_bytes"] = args.fetch_batch_mib * 1024 * 1024
        env = body["spec"]["driverSpec"]["podTemplateSpec"]["spec"]["containers"][0]["env"]
        config_env = next(item for item in env if item["name"] == "COMPANY_EXECUTION_CONFIG")
        execution_config = json.loads(config_env["value"])
        execution_config["planner"] = planner
        config_env["value"] = json.dumps(execution_config, separators=(",", ":"))
    if args.task_slots is not None:
        env = body["spec"]["driverSpec"]["podTemplateSpec"]["spec"]["containers"][0]["env"]
        config_env = next(item for item in env if item["name"] == "COMPANY_EXECUTION_CONFIG")
        execution_config = json.loads(config_env["value"])
        execution_config["planner"]["task_slots"] = args.task_slots
        config_env["value"] = json.dumps(execution_config, separators=(",", ":"))
    body["metadata"]["labels"]["app.kubernetes.io/managed-by"] = (
        "direct-spark-benchmark"
    )
    return body


def driver_logs(name):
    pods = kubectl(
        "get", "pods", "-l", f"company-run={name},spark-role=driver",
        "-o", "json", check=False,
    )
    if pods.returncode:
        return pods.stderr
    documents = json.loads(pods.stdout).get("items", [])
    output = []
    for pod in documents:
        pod_name = pod["metadata"]["name"]
        logs = kubectl(
            "logs", pod_name, "-c", "spark-kubernetes-driver", check=False,
        )
        output.append(logs.stdout or logs.stderr)
    return "\n".join(output)


def wait_for_result(name, timeout):
    deadline = time.monotonic() + timeout
    last_state = None
    while time.monotonic() < deadline:
        response = kubectl("get", "sparkapplication", name, "-o", "json")
        document = json.loads(response.stdout)
        state = (
            (document.get("status") or {}).get("currentState") or {}
        ).get("currentStateSummary", "Submitted")
        if state != last_state:
            print(f"[{name}] {state}", flush=True)
            last_state = state
        terminal = application_result(document)
        if not terminal:
            pods = kubectl(
                "get", "pods", "-l", f"company-run={name},spark-role=driver",
                "-o", "json", check=False,
            )
            if pods.returncode == 0:
                phases = [
                    item.get("status", {}).get("phase")
                    for item in json.loads(pods.stdout).get("items", [])
                ]
                if "Succeeded" in phases:
                    logs = driver_logs(name)
                    for line in logs.splitlines():
                        if "COMPANY_INGESTION_RESULT=" in line:
                            return json.loads(
                                line.split("COMPANY_INGESTION_RESULT=", 1)[1]
                            )
        if terminal:
            logs = driver_logs(name)
            summary = None
            for line in logs.splitlines():
                if "COMPANY_INGESTION_RESULT=" in line:
                    summary = json.loads(
                        line.split("COMPANY_INGESTION_RESULT=", 1)[1]
                    )
            if terminal["status"] != "success":
                tail = "\n".join(logs.splitlines()[-120:])
                raise RuntimeError(
                    f"{name} ended as {terminal['state']}: "
                    f"{terminal.get('message', '')}\n{tail}"
                )
            if summary is None:
                raise RuntimeError(f"{name} succeeded without a result summary")
            return summary
        time.sleep(5)
    raise TimeoutError(f"Timed out after {timeout}s waiting for {name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="wide", choices=["small", "medium", "wide"])
    parser.add_argument("--profile", default="small", choices=["small", "medium"])
    parser.add_argument(
        "--task-slots", type=int,
        help="Override planner task slots while retaining the selected executor memory profile.",
    )
    parser.add_argument("--order", default="jdbc-first", choices=["jdbc-first", "arrow-first"])
    parser.add_argument(
        "--reader", default="both", choices=["both", "jdbc", "mssql_arrow"],
        help="Run both readers or isolate one reader while debugging.",
    )
    parser.add_argument("--isolate-io-phases", action="store_true")
    parser.add_argument("--writer-matrix", action="store_true")
    parser.add_argument(
        "--write-mode", choices=["spark", "pyarrow"], default="spark",
    )
    parser.add_argument(
        "--compression", choices=["snappy", "zstd", "uncompressed"],
        default="zstd",
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--output-partitions", type=int)
    output_group.add_argument("--target-file-mib", type=int)
    parser.add_argument(
        "--s3a-upload-profile", choices=["default", "disk-multipart"],
        default="default",
    )
    parser.add_argument("--executor-memory")
    parser.add_argument("--executor-memory-overhead")
    parser.add_argument("--max-connections", type=int, default=8)
    parser.add_argument("--target-partition-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument(
        "--partition-oversubscription", type=int, choices=[1, 2], default=1,
        help="Maximum planned partitions per available Spark task slot.",
    )
    parser.add_argument(
        "--fetch-batch-mib", type=int, choices=[16, 32, 64], default=None,
        help="Override the runtime's adaptive JDBC/Arrow fetch batch target.",
    )
    parser.add_argument("--timeout", type=int, default=2400)
    args = parser.parse_args()
    if args.writer_matrix:
        args.isolate_io_phases = True

    release = kubectl(
        "get", "configmap", "ingestion-release",
        "-o", "jsonpath={.data.sha256}",
    )
    wheel_sha = release.stdout.strip()
    if not wheel_sha:
        raise RuntimeError("ingestion-release ConfigMap has no wheel SHA256")

    if args.reader == "both":
        modes = (
            ["jdbc", "mssql_arrow"] if args.order == "jdbc-first"
            else ["mssql_arrow", "jdbc"]
        )
    else:
        modes = [args.reader]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    output = ROOT / ".local/benchmarks"
    output.mkdir(parents=True, exist_ok=True)
    results = {}
    failures = []
    for mode in modes:
        short_mode = "arrow" if mode == "mssql_arrow" else mode
        name = f"spark-{short_mode}-{args.table}-{timestamp}"
        body = application(name, mode, args, wheel_sha)
        submitted = kubectl("apply", "-f", "-", input_value=json.dumps(body))
        print(submitted.stdout.strip(), flush=True)
        try:
            result = wait_for_result(name, args.timeout)
            logs = driver_logs(name)
            result["runtime_diagnostics"] = {
                "lost_executor_ids": sorted(set(re.findall(
                    r"Lost executor (\d+)", logs
                ))),
                "oom_detected": "OOM is detected" in logs,
                "fetch_failed_events": logs.count("FetchFailed("),
                "s3a_upload_profile": args.s3a_upload_profile,
            }
            results[mode] = result
            path = output / f"{name}.json"
            path.write_text(json.dumps(result, indent=2, default=str) + "\n")
            print(
                f"{mode}: {result['rows']:,} rows; "
                f"{result['durations_seconds']['read_write']:.3f}s; "
                f"{result['throughput_rows_per_second']:.1f} rows/s; {path}",
                flush=True,
            )
        except Exception as error:
            failures.append((mode, error))
            print(f"{mode}: FAILED: {error}", file=sys.stderr, flush=True)

    if len(results) == 2:
        jdbc = results["jdbc"]
        arrow = results["mssql_arrow"]
        ratio = (
            arrow["throughput_rows_per_second"]
            / jdbc["throughput_rows_per_second"]
        )
        print(
            "COMPARISON=" + json.dumps({
                "table": args.table,
                "profile": args.profile,
                "isolate_io_phases": args.isolate_io_phases,
                "jdbc_seconds": jdbc["durations_seconds"]["read_write"],
                "arrow_seconds": arrow["durations_seconds"]["read_write"],
                "jdbc_rows_per_second": jdbc["throughput_rows_per_second"],
                "arrow_rows_per_second": arrow["throughput_rows_per_second"],
                "arrow_over_jdbc": ratio,
            }, indent=2),
            flush=True,
        )
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
