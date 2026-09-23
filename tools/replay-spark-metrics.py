"""Republish durable Spark summaries with the current Prometheus labels."""
import json
from pathlib import Path
import urllib.parse
import urllib.request

import boto3

from company_ingestion.metrics import exposition


root = Path(__file__).resolve().parents[1]
credentials = json.loads((root / ".local/credentials.json").read_text())
s3 = boto3.client(
    "s3", endpoint_url="http://127.0.0.1:8333",
    aws_access_key_id=credentials["s3_access"],
    aws_secret_access_key=credentials["s3_secret"], region_name="us-east-1",
)

count = 0
for item in s3.list_objects_v2(Bucket="metrics", Prefix="runs/").get("Contents", []):
    if not item["Key"].endswith(".json") or item["Key"].endswith(".failed.json"):
        continue
    result = json.loads(s3.get_object(Bucket="metrics", Key=item["Key"])["Body"].read())
    if result.get("run_id") == "company-smoke":
        continue
    run_id = urllib.parse.quote(result["run_id"], safe="")
    uri = f"http://127.0.0.1:9091/metrics/job/company_ingestion/engine/spark/run_id/{run_id}"
    request = urllib.request.Request(
        uri, data=exposition(result).encode(), method="PUT",
        headers={"Content-Type": "text/plain; version=0.0.4"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        response.read()
    count += 1
print(f"Republished {count} Spark summaries")
