import pytest

from company_dlt_ingestion.bootstrap.composition import register_builtin_components
from company_dlt_ingestion.bootstrap.registry import COMPONENTS, ComponentRegistry
from company_dlt_ingestion.application.config import DistributedConfig


def test_builtin_component_matrix_is_explicit():
    register_builtin_components()
    assert set(COMPONENTS.sources) == {"sqlserver", "oracle"}
    assert set(COMPONENTS.encoders) == {"dlt_parquet"}
    assert set(COMPONENTS.stores) == {"s3"}
    assert set(COMPONENTS.publishers) == {"delta"}


def test_registry_rejects_duplicate_and_explains_available_components():
    registry = ComponentRegistry()
    registry.register("sources", "first", lambda: object())
    with pytest.raises(ValueError, match="already registered"):
        registry.register("sources", "first", lambda: object())
    with pytest.raises(ValueError, match="available: first"):
        registry.create("sources", "missing")


def test_generic_config_does_not_embed_vendor_selection_rules():
    value = {
        "run_id": "plugin-contract-test",
        "profile": "small",
        "source": {"type": "future-source"},
        "destination": {"format": "future-format", "uri": "s3://target"},
        "_orchestration": {
            "image": "engine:1", "namespace": "jobs",
            "worker_service_account": "worker",
        },
    }
    config = DistributedConfig(value)
    assert config.source["type"] == "future-source"
    assert config.destination["format"] == "future-format"
