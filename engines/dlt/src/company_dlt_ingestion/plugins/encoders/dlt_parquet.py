"""dltHub adapter that converts source batches into Parquet artifacts."""

import os
import re
import time

from ...core.contracts import ChunkEncoding


class DltParquetEncoder:
    name = "dlt_parquet"

    def __init__(self, endpoint_url):
        self.endpoint_url = endpoint_url

    def encode(
        self, *, resource_name, batch_supplier, attempt_uri, pipeline_name,
        store,
    ):
        import dlt
        from dlt.common.configuration.specs import AwsCredentials

        rows = 0

        @dlt.resource(name=resource_name, write_disposition="append")
        def resource():
            nonlocal rows
            for batch in batch_supplier():
                rows += int(
                    batch.num_rows if hasattr(batch, "num_rows") else len(batch)
                )
                yield batch

        credentials = AwsCredentials(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            endpoint_url=self.endpoint_url,
        )
        setup_started = time.perf_counter()
        destination = dlt.destinations.filesystem(
            bucket_url=attempt_uri, credentials=credentials
        )
        pipeline = dlt.pipeline(
            pipeline_name=re.sub(r"[^A-Za-z0-9_]", "_", pipeline_name),
            destination=destination,
            dataset_name="payload",
        )
        setup = time.perf_counter() - setup_started
        run_started = time.perf_counter()
        load_info = pipeline.run(
            resource(), write_disposition="append",
            loader_file_format="parquet",
        )
        pipeline_run = time.perf_counter() - run_started
        listing_started = time.perf_counter()
        objects = [
            item for item in store.list(attempt_uri)
            if item["key"].endswith(".parquet")
            and "/_dlt_" not in "/" + item["key"]
        ]
        listing = time.perf_counter() - listing_started
        if rows and not objects:
            raise RuntimeError(
                "dlt completed without producing a Parquet data file"
            )
        return ChunkEncoding(
            rows=rows,
            files=objects,
            load_ids=list(load_info.loads_ids),
            durations={
                "pipeline_setup": setup,
                "pipeline_run": pipeline_run,
                "object_listing": listing,
            },
        )
