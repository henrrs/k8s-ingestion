"""Planning use case independent from the source technology."""

from datetime import datetime, timezone

from ..core.serialization import content_hash
from .state import plan_uri


def build_plan(config, store, source):
    uri = plan_uri(config)
    if store.exists(uri):
        return store.read_json(uri)
    metadata, read_plan = source.partitioner().plan(config.planner_config)
    value = read_plan.to_dict()
    value.update(
        {
            "version": 1,
            "run_id": config.run_id,
            "source": {
                "type": config.source["type"],
                "database": config.source["database"],
                "schema": config.source["schema"],
                "table": config.source["table"],
            },
            "column_count": len(metadata.columns),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    value["plan_hash"] = content_hash(value)
    store.write_immutable_json(uri, value)
    return value
