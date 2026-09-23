"""Small dltHub SQL Server → Delta ingestion workload for comparison with Spark."""
from datetime import datetime, timezone
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request

import boto3
import dlt
from deltalake import DeltaTable
from dlt.common.configuration.specs import AwsCredentials
from dlt.sources.sql_database import sql_database
import sqlalchemy as sa


def required(config, path):
    value = config
    for key in path.split("."):
        value = value[key]
    return value


def validate_identifier(value, name):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$#@]*", value):
        raise ValueError(f"{name} is not a safe SQL Server identifier")
    return value


def s3_client(endpoint):
    return boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def storage_options(endpoint):
    return {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        "allow_http": "true",
        # Every benchmark run owns a unique table path. This local-only setting
        # avoids requiring DynamoDB while preserving single-writer semantics.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def object_sizes(client, uri):
    parsed = urllib.parse.urlparse(uri)
    prefix = parsed.path.lstrip("/").rstrip("/") + "/"
    paginator = client.get_paginator("list_objects_v2")
    objects = [item for page in paginator.paginate(Bucket=parsed.netloc, Prefix=prefix)
               for item in page.get("Contents", [])]
    data = [item for item in objects if "/_delta_log/" not in "/" + item["Key"]]
    return sum(item["Size"] for item in data), len([item for item in data if item["Key"].endswith(".parquet")])


def write_summary(client, result):
    client.put_object(
        Bucket="metrics", Key=f"runs-dlt/{result['run_id']}.json",
        Body=(json.dumps(result, indent=2, default=str) + "\n").encode(),
        ContentType="application/json",
    )


def escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def push_metrics(result, endpoint):
    if not endpoint:
        return
    labels = {"engine": "dlt", "table": result["table"], "profile": result["profile"],
              "run_id": result["run_id"], "strategy": result["plan"]["strategy"]}
    def line(name, value, extra=None):
        pairs = ",".join(f'{key}="{escape(val)}"' for key, val in {**labels, **(extra or {})}.items())
        return f"company_ingestion_{name}{{{pairs}}} {float(value)}\n"
    body = "".join(line(name, result[name]) for name in [
        "rows", "output_bytes", "source_estimated_bytes", "partitions", "column_count",
        "throughput_rows_per_second", "throughput_output_bytes_per_second", "success"])
    body += "".join(line("duration_seconds", seconds, {"phase": phase})
                    for phase, seconds in result["durations_seconds"].items())
    body += line("plan_info", 1, {"column": "none", "reason": result["plan"]["rationale"]})
    uri = endpoint.rstrip("/") + "/metrics/job/company_ingestion/engine/dlt/run_id/" + urllib.parse.quote(result["run_id"], safe="")
    request = urllib.request.Request(uri, data=body.encode(), method="PUT",
                                     headers={"Content-Type": "text/plain; version=0.0.4"})
    with urllib.request.urlopen(request, timeout=15) as response:
        response.read()


def main():
    started = time.perf_counter()
    config = json.loads(os.environ["COMPANY_JOB_CONFIG"])
    run_id, profile = required(config, "run_id"), required(config, "profile")
    source = required(config, "source")
    if required(config, "source.type") != "sqlserver":
        raise ValueError("source.type must be sqlserver")
    if required(config, "destination.format") != "delta":
        raise ValueError("destination.format must be delta")
    table = validate_identifier(required(config, "source.table"), "source.table")
    schema = validate_identifier(required(config, "source.schema"), "source.schema")
    database = required(config, "source.database")
    chunk_size = int(config.get("extract", {}).get("chunk_size", 50000))
    if config.get("extract", {}).get("backend", "pyarrow") != "pyarrow":
        raise ValueError("extract.backend must be pyarrow")
    if chunk_size < 1:
        raise ValueError("extract.chunk_size must be positive")
    endpoint = config.get("storage", {}).get("endpoint_url", "http://seaweedfs:8333")
    run_uri = required(config, "destination.uri").rstrip("/") + "/" + run_id
    result = {
        "engine": "dlt", "engine_version": dlt.__version__, "run_id": run_id,
        "profile": profile, "table": table, "database": database, "schema": schema,
        "started_at": datetime.now(timezone.utc).isoformat(), "destination": run_uri,
        "source_bytes_definition": "SQL Server allocated used pages; estimate, not network bytes",
        "output_bytes_definition": "Active Delta data files produced by dlt/delta-rs, excluding transaction log and dlt state",
        "throughput_definition": "Validated source rows / dlt extract-normalize-load duration",
        "success": 0,
    }
    client = s3_client(endpoint)
    try:
        planning_started = time.perf_counter()
        url = sa.URL.create(
            "mssql+pymssql", username=os.environ["SQLSERVER_USER"],
            password=os.environ["SQLSERVER_PASSWORD"], host=required(source, "host"),
            port=int(source.get("port", 1433)), database=database,
        )
        engine = sa.create_engine(url, pool_pre_ping=True)
        qualified = f"[{schema}].[{table}]"
        with engine.connect() as connection:
            stats = connection.execute(sa.text("""
                SELECT COALESCE(SUM(CASE WHEN p.index_id IN (0,1) THEN p.row_count ELSE 0 END),0) AS rows,
                       COALESCE(SUM(p.used_page_count),0) * 8192 AS bytes
                FROM sys.dm_db_partition_stats AS p
                WHERE p.object_id=OBJECT_ID(:qualified)
            """), {"qualified": qualified}).mappings().one()
            metadata = sa.MetaData()
            reflected = sa.Table(table, metadata, schema=schema, autoload_with=connection)
        rows_expected, source_bytes = int(stats["rows"]), int(stats["bytes"])
        planning_seconds = time.perf_counter() - planning_started
        batches = max(1, math.ceil(rows_expected / chunk_size))
        result["plan"] = {
            "strategy": "dlt_pyarrow_chunks", "partitions": 1,
            "requested_partitions": 1, "chunk_size": chunk_size,
            "estimated_batches": batches,
            "rationale": "dlt sql_database PyArrow backend streams bounded row batches in one Kubernetes process; no distributed JDBC partitions.",
        }
        print("COMPANY_DLT_PLAN=" + json.dumps(result["plan"]), flush=True)

        credentials = AwsCredentials(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.getenv("AWS_REGION", "us-east-1"), endpoint_url=endpoint,
        )
        destination = dlt.destinations.filesystem(
            bucket_url=run_uri, credentials=credentials,
            deltalake_storage_options=storage_options(endpoint),
        )
        pipeline = dlt.pipeline(
            pipeline_name=re.sub(r"[^A-Za-z0-9_]", "_", run_id),
            destination=destination, dataset_name="data",
        )
        data = sql_database(
            credentials=engine, schema=schema, table_names=[table],
            chunk_size=chunk_size, backend="pyarrow", reflection_level="full_with_precision",
        ).with_resources(table)
        load_started = time.perf_counter()
        load_info = pipeline.run(data, write_disposition="replace", table_format="delta")
        load_seconds = time.perf_counter() - load_started

        verify_started = time.perf_counter()
        with pipeline.destination_client() as destination_client:
            table_uri = destination_client.get_open_table_location("delta", table)
        delta = DeltaTable(table_uri, storage_options=storage_options(endpoint))
        rows = delta.to_pyarrow_dataset().count_rows()
        if rows != rows_expected:
            raise RuntimeError(f"Delta readback mismatch: {rows} != {rows_expected}")
        output_bytes, files = object_sizes(client, table_uri)
        verify_seconds = time.perf_counter() - verify_started
        result.update({
            "rows": rows, "readback_rows": rows, "output_bytes": output_bytes,
            "files": files, "source_estimated_bytes": source_bytes,
            "source_estimated_rows": rows_expected, "partitions": 1,
            "column_count": len(reflected.columns), "load_ids": list(load_info.loads_ids),
            "throughput_rows_per_second": rows / max(load_seconds, 1e-9),
            "throughput_output_bytes_per_second": output_bytes / max(load_seconds, 1e-9),
            "durations_seconds": {"planning": planning_seconds, "read_write": load_seconds,
                                  "readback": verify_seconds,
                                  "engine_total": time.perf_counter() - started},
            "success": 1,
        })
        write_summary(client, result)
        try:
            push_metrics(result, config.get("metrics", {}).get("pushgateway"))
        except Exception as error:
            print(f"Observability push unavailable: {type(error).__name__}", flush=True)
        print("COMPANY_JOB_RESULT=" + json.dumps(result, default=str), flush=True)
    except Exception as error:
        result.update({"error_type": type(error).__name__, "elapsed_seconds": time.perf_counter() - started})
        try:
            client.put_object(Bucket="metrics", Key=f"runs-dlt/{run_id}.failed.json",
                              Body=(json.dumps(result, indent=2) + "\n").encode())
        except Exception:
            pass
        raise


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] in {"driver", "worker"}:
        from company_dlt_ingestion.bootstrap.entrypoint import main as distributed_main
        distributed_main(sys.argv[1])
    else:
        main()
