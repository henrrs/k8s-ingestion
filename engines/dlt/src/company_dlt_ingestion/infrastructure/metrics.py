"""Prometheus publication for the distributed dlt run."""

import urllib.parse
import urllib.request


def escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _send(endpoint, grouping, body):
    if not endpoint:
        return
    uri = endpoint.rstrip("/") + grouping
    request = urllib.request.Request(
        uri,
        data=body.encode(),
        method="PUT",
        headers={"Content-Type": "text/plain; version=0.0.4"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        response.read()


def push_progress(config, plan, active, succeeded, failed):
    endpoint = config.metrics.get("pushgateway")
    labels = (
        f'engine="dlt-distributed",table="{escape(config.source["table"])}",'
        f'profile="{escape(config.profile)}",run_id="{escape(config.run_id)}",'
        f'strategy="{escape(plan["strategy"])}"'
    )
    body = "".join(
        [
            f"company_ingestion_chunks_total{{{labels}}} {plan['chunk_count']}\n",
            f"company_ingestion_chunks_running{{{labels}}} {active}\n",
            f"company_ingestion_chunks_succeeded{{{labels}}} {succeeded}\n",
            f"company_ingestion_chunks_failed{{{labels}}} {failed}\n",
        ]
    )
    grouping = (
        "/metrics/job/company_ingestion_progress/engine/dlt-distributed/run_id/"
        + urllib.parse.quote(config.run_id, safe="")
    )
    _send(endpoint, grouping, body)


def push_chunk(config, manifest):
    endpoint = config.metrics.get("pushgateway")
    labels = (
        f'engine="dlt-distributed",table="{escape(config.source["table"])}",'
        f'profile="{escape(config.profile)}",run_id="{escape(config.run_id)}",'
        f'chunk_id="{manifest["index"]}",attempt="{escape(manifest["pod_uid"])}"'
    )
    body = "".join(
        [
            f"company_ingestion_chunk_rows{{{labels}}} {manifest['rows']}\n",
            f"company_ingestion_chunk_bytes{{{labels}}} {manifest['bytes']}\n",
            f"company_ingestion_chunk_duration_seconds{{{labels}}} {manifest['duration_seconds']}\n",
            f"company_ingestion_chunk_success{{{labels}}} 1\n",
        ]
    )
    body += "".join(
        f'company_ingestion_chunk_phase_duration_seconds{{{labels},phase="{escape(phase)}"}} {seconds}\n'
        for phase, seconds in manifest.get("durations_seconds", {}).items()
    )
    body += (
        f"company_ingestion_chunk_cpu_seconds{{{labels}}} "
        f"{manifest.get('cpu_seconds', 0)}\n"
    )
    grouping = (
        "/metrics/job/company_ingestion_chunk/engine/dlt-distributed/run_id/"
        + urllib.parse.quote(config.run_id, safe="")
        + "/chunk_id/"
        + str(manifest["index"])
    )
    _send(endpoint, grouping, body)


def push(result, endpoint):
    if not endpoint:
        return
    labels = {
        "engine": "dlt-distributed",
        "table": result["table"],
        "profile": result["profile"],
        "run_id": result["run_id"],
        "strategy": result["plan"]["strategy"],
    }

    def line(name, value, extra=None):
        all_labels = {**labels, **(extra or {})}
        pairs = ",".join(
            f'{key}="{escape(label)}"' for key, label in all_labels.items()
        )
        return f"company_ingestion_{name}{{{pairs}}} {float(value)}\n"

    values = {
        "rows": result["rows"],
        "output_bytes": result["output_bytes"],
        "staging_data_bytes": result["staging_data_bytes"],
        "publication_data_bytes_written": result[
            "publication_data_bytes_written"
        ],
        "data_write_amplification_ratio": result[
            "data_write_amplification_ratio"
        ],
        "source_estimated_bytes": result["source_estimated_bytes"],
        "partitions": result["plan"]["parallelism"],
        "chunks_total": result["plan"]["chunk_count"],
        "column_count": result["column_count"],
        "throughput_rows_per_second": result["throughput_rows_per_second"],
        "throughput_output_bytes_per_second": result[
            "throughput_output_bytes_per_second"
        ],
        "success": result["success"],
        "retries_total": result["retries"],
        "target_chunk_bytes": result["plan"]["target_chunk_bytes"],
        "fetch_size": result["plan"]["fetch_size"],
    }
    body = "".join(line(name, value) for name, value in values.items())
    body += "".join(
        line("duration_seconds", value, {"phase": phase})
        for phase, value in result["durations_seconds"].items()
    )
    body += line(
        "plan_info",
        1,
        {
            "column": result["plan"].get("column") or "none",
            "reason": result["plan"]["rationale"],
            "chunk_size_mode": result["plan"]["chunk_size_mode"],
            "fetch_size_mode": result["plan"]["fetch_size_mode"],
            "publication_mode": result["publication_mode"],
            "extract_backend": result.get("extract_backend", "unknown"),
        },
    )
    grouping = (
        "/metrics/job/company_ingestion/engine/dlt-distributed/run_id/"
        + urllib.parse.quote(result["run_id"], safe="")
    )
    _send(endpoint, grouping, body)
