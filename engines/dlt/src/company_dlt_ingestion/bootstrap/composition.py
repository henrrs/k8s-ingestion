"""Resolve one job configuration into isolated runtime components."""

from dataclasses import dataclass

from .registry import COMPONENTS
from ..plugins.encoders.dlt_parquet import DltParquetEncoder
from ..plugins.publishers.delta import DeltaPublisher
from ..plugins.sources.oracle import OracleSourceAdapter
from ..plugins.sources.sqlserver import SqlServerSourceAdapter
from ..plugins.stores.s3 import S3ObjectStore


def register_builtin_components():
    defaults = (
        ("sources", "sqlserver", SqlServerSourceAdapter),
        ("sources", "oracle", OracleSourceAdapter),
        ("encoders", "dlt_parquet", DltParquetEncoder),
        ("stores", "s3", S3ObjectStore),
        ("publishers", "delta", DeltaPublisher),
    )
    for category, name, factory in defaults:
        values = getattr(COMPONENTS, category)
        if name not in values:
            COMPONENTS.register(category, name, factory)


@dataclass
class RuntimeComponents:
    source: object
    encoder: object
    store: object
    publisher: object


def assemble(config):
    register_builtin_components()
    source_name = config.source["type"]
    encoder_name = config.execution.get("encoder", "dlt_parquet")
    store_name = config.storage.get("type", "s3")
    publisher_name = config.destination.get("format", "delta")
    return RuntimeComponents(
        source=COMPONENTS.create("sources", source_name, config.source),
        encoder=COMPONENTS.create(
            "encoders", encoder_name, config.endpoint_url
        ),
        store=COMPONENTS.create("stores", store_name, config.endpoint_url),
        publisher=COMPONENTS.create(
            "publishers", publisher_name, config.endpoint_url
        ),
    )
