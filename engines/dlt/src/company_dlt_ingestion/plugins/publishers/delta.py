"""Delta Lake publisher for already encoded Parquet chunk artifacts."""

import json
import os
import time

from ..stores.s3 import delta_storage_options, parse_uri


def normalized_batches(files, filesystem, schema, batch_size):
    import pyarrow as pa
    import pyarrow.parquet as pq

    for item in files:
        path = f"{item['bucket']}/{item['key']}"
        parquet = pq.ParquetFile(path, filesystem=filesystem)
        for batch in parquet.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            arrays = []
            for field in schema:
                if field.name not in table.column_names:
                    arrays.append(pa.nulls(table.num_rows, type=field.type))
                    continue
                column = table[field.name]
                if not column.type.equals(field.type):
                    column = column.cast(field.type)
                arrays.append(column)
            normalized = pa.Table.from_arrays(arrays, schema=schema)
            yield from normalized.to_batches(max_chunksize=batch_size)


def relative_delta_path(final_uri, item):
    bucket, prefix = parse_uri(final_uri)
    prefix = prefix.rstrip("/") + "/"
    if item["bucket"] != bucket or not item["key"].startswith(prefix):
        return None
    return item["key"][len(prefix):]


def schemas_are_zero_copy_compatible(schemas, unified):
    expected = {field.name: field.type for field in unified}
    return all(
        all(expected.get(field.name) == field.type for field in schema)
        for schema in schemas
    )


def active_delta_objects(table, objects):
    active = set()
    for uri in table.file_uris():
        bucket, key = parse_uri(uri)
        active.add((bucket, key))
    return [
        item for item in objects
        if (item["bucket"], item["key"]) in active
    ]


class DeltaPublisher:
    name = "delta"

    def __init__(self, endpoint_url):
        self.endpoint_url = endpoint_url

    def publish(self, config, store, plan, manifests):
        import pyarrow as pa
        import pyarrow.parquet as pq
        import s3fs
        from deltalake import DeltaTable, write_deltalake

        expected_rows = sum(item["rows"] for item in manifests)
        options = delta_storage_options(self.endpoint_url)
        try:
            existing = DeltaTable(
                config.final_uri, storage_options=options
            )
        except Exception:
            existing = None
        if existing is not None:
            actual = existing.to_pyarrow_dataset().count_rows()
            if actual != expected_rows:
                raise RuntimeError(
                    f"Existing Delta table has {actual} rows; expected "
                    f"{expected_rows}"
                )
            data = active_delta_objects(
                existing, store.list(config.final_uri)
            )
            return {
                "rows": actual,
                "output_bytes": sum(item["size"] for item in data),
                "files": len(data),
                "delta_version": existing.version(),
                "resumed": True,
                "publication_mode": "existing",
            }

        files = [
            file for manifest in manifests for file in manifest["files"]
        ]
        if expected_rows and not files:
            raise RuntimeError(
                "No selected Parquet files are available for publication"
            )
        filesystem = s3fs.S3FileSystem(
            key=os.environ["AWS_ACCESS_KEY_ID"],
            secret=os.environ["AWS_SECRET_ACCESS_KEY"],
            endpoint_url=self.endpoint_url,
            client_kwargs={
                "region_name": os.getenv("AWS_REGION", "us-east-1")
            },
            config_kwargs={"s3": {"addressing_style": "path"}},
        )
        schemas = [
            pq.read_schema(
                f"{item['bucket']}/{item['key']}", filesystem=filesystem
            )
            for item in files
        ]
        if not schemas:
            raise RuntimeError("A completely empty snapshot is not supported yet")
        schema = pa.unify_schemas(schemas, promote_options="permissive")
        relative_paths = [
            relative_delta_path(config.final_uri, item) for item in files
        ]
        zero_copy = (
            all(relative_paths)
            and schemas_are_zero_copy_compatible(schemas, schema)
            and config.execution.get("publication_mode", "auto") != "rewrite"
        )
        if zero_copy:
            from deltalake import Schema
            from deltalake.transaction import (
                AddAction,
                create_table_with_add_actions,
            )

            actions = []
            for item, path in zip(files, relative_paths):
                parquet = pq.ParquetFile(
                    f"{item['bucket']}/{item['key']}",
                    filesystem=filesystem,
                )
                actions.append(
                    AddAction(
                        path=path,
                        size=int(item["size"]),
                        partition_values={},
                        modification_time=int(time.time() * 1000),
                        data_change=True,
                        stats=json.dumps(
                            {"numRecords": parquet.metadata.num_rows}
                        ),
                    )
                )
            create_table_with_add_actions(
                config.final_uri,
                Schema.from_arrow(schema),
                actions,
                mode="error",
                storage_options=options,
            )
            publication_mode = "zero_copy"
        else:
            batches = normalized_batches(
                files, filesystem, schema, int(plan["fetch_size"])
            )
            reader = pa.RecordBatchReader.from_batches(schema, batches)
            write_deltalake(
                config.final_uri,
                reader,
                mode="error",
                storage_options=options,
                target_file_size=int(
                    config.execution.get(
                        "target_file_bytes", 256 * 1024 * 1024
                    )
                ),
            )
            publication_mode = "rewrite"

        delta = DeltaTable(config.final_uri, storage_options=options)
        actual_rows = delta.to_pyarrow_dataset().count_rows()
        if actual_rows != expected_rows:
            raise RuntimeError(
                f"Delta readback mismatch: {actual_rows} != {expected_rows}"
            )
        require_source_match = config.raw.get("validation", {}).get(
            "require_source_row_match", True
        )
        if require_source_match and actual_rows != int(plan["estimated_rows"]):
            raise RuntimeError(
                "Snapshot row count differs from the source boundary estimate: "
                f"{actual_rows} != {plan['estimated_rows']}"
            )
        data = active_delta_objects(delta, store.list(config.final_uri))
        return {
            "rows": actual_rows,
            "output_bytes": sum(item["size"] for item in data),
            "files": len(data),
            "delta_version": delta.version(),
            "resumed": False,
            "publication_mode": publication_mode,
        }
