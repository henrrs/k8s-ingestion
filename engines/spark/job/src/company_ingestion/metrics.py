"""Durable run summaries and Prometheus gauges; byte definitions are explicit."""
import json
import urllib.parse
import urllib.request

def write_json(spark, uri, value):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(path, True)
    try:
        stream.write(bytearray((json.dumps(value, indent=2, default=str) + "\n").encode()))
    finally:
        stream.close()

def escaped(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

def exposition(result):
    labels = {"engine": "spark", "table": result["table"], "profile": result["profile"],
              "run_id": result["run_id"], "strategy": result["plan"]["strategy"]}
    def line(name, value, extra=None):
        pairs = ",".join(f'{k}="{escaped(v)}"' for k, v in {**labels, **(extra or {})}.items())
        return f"company_ingestion_{name}{{{pairs}}} {float(value)}\n"
    text = ""
    for metric in ["rows", "output_bytes", "source_estimated_bytes", "partitions", "column_count", "throughput_rows_per_second", "throughput_output_bytes_per_second", "success"]:
        text += line(metric, result[metric])
    for phase, seconds in result["durations_seconds"].items():
        text += line("duration_seconds", seconds, {"phase": phase})
    text += line("plan_info", 1, {"column": result["plan"]["column"] or "none", "reason": result["plan"]["rationale"]})
    return text

def push(result, endpoint):
    if endpoint:
        uri = endpoint.rstrip("/") + "/metrics/job/company_ingestion/run_id/" + urllib.parse.quote(result["run_id"], safe="")
        request = urllib.request.Request(uri, data=exposition(result).encode(), method="PUT", headers={"Content-Type": "text/plain; version=0.0.4"})
        with urllib.request.urlopen(request, timeout=15) as response:
            response.read()
