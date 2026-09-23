"""S3-compatible control and artifact store."""

import json
import os
from urllib.parse import urlparse

import boto3
from botocore.config import Config

from ...core.serialization import canonical_json, content_hash


def parse_uri(uri):
    parsed = urlparse(uri.replace("s3a://", "s3://", 1))
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected an s3:// URI, got {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


class S3ObjectStore:
    name = "s3"

    def __init__(self, endpoint_url):
        self.endpoint_url = endpoint_url
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}),
        )

    def exists(self, uri):
        bucket, key = parse_uri(uri)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except self.client.exceptions.ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def read_json(self, uri):
        bucket, key = parse_uri(uri)
        response = self.client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read())

    def write_json(self, uri, value):
        bucket, key = parse_uri(uri)
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=canonical_json(value) + b"\n",
            ContentType="application/json",
        )

    def write_immutable_json(self, uri, value):
        if self.exists(uri):
            existing = self.read_json(uri)
            if content_hash(existing) != content_hash(value):
                raise RuntimeError(
                    f"Immutable object has different content: {uri}"
                )
            return existing
        self.write_json(uri, value)
        return value

    def list(self, uri):
        bucket, prefix = parse_uri(uri)
        prefix = prefix.rstrip("/") + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        return [
            {"bucket": bucket, "key": item["Key"], "size": item["Size"]}
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
            for item in page.get("Contents", [])
        ]


def delta_storage_options(endpoint_url):
    return {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        "allow_http": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
