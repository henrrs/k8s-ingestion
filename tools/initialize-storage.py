import json
from pathlib import Path
import boto3
from botocore.config import Config

root = Path(__file__).resolve().parents[1]
creds = json.loads((root / ".local/credentials.json").read_text())
client = boto3.client("s3", endpoint_url="http://127.0.0.1:8333", aws_access_key_id=creds["s3_access"],
                      aws_secret_access_key=creds["s3_secret"], region_name="us-east-1",
                      config=Config(s3={"addressing_style": "path"}))
existing = {b["Name"] for b in client.list_buckets()["Buckets"]}
for bucket in ["lakehouse", "metrics", "spark-events", "ingestion-control"]:
    if bucket not in existing: client.create_bucket(Bucket=bucket)
    print(f"Storage ready: {bucket}")
