"""Durable URIs, checkpoints and worker assignment rules."""

from datetime import datetime, timezone
import re


def plan_uri(config):
    return f"{config.control_uri}/plan.json"


def request_uri(config):
    return f"{config.control_uri}/request.json"


def selected_uri(config):
    return f"{config.control_uri}/state/selected-chunks.json"


def result_uri(config):
    return f"{config.control_uri}/result.json"


def attempt_manifest_uri(config, index, pod_uid):
    return (
        f"{config.control_uri}/chunks/{index:05d}/attempts/"
        f"{pod_uid}/manifest.json"
    )


def assigned_chunks(plan, worker_index):
    parallelism = int(plan["parallelism"])
    if worker_index < 0 or worker_index >= parallelism:
        raise ValueError(
            f"Worker index {worker_index} is outside 0..{parallelism - 1}"
        )
    return [
        chunk for chunk in plan["chunks"]
        if int(chunk["index"]) % parallelism == worker_index
    ]


def chunk_manifests(config, store, index):
    root = f"{config.control_uri}/chunks/{index:05d}/attempts"
    candidates = []
    for item in store.list(root):
        if not item["key"].endswith("/manifest.json"):
            continue
        manifest = store.read_json(f"s3://{item['bucket']}/{item['key']}")
        if (
            manifest.get("run_id") != config.run_id
            or int(manifest.get("index", -1)) != index
            or not isinstance(manifest.get("files"), list)
        ):
            continue
        if all(
            store.exists(f"s3://{file['bucket']}/{file['key']}")
            for file in manifest["files"]
        ):
            candidates.append(manifest)
    return sorted(
        candidates,
        key=lambda item: (item.get("completed_at", ""), item["pod_uid"]),
    )


def completed_chunk_count(config, store, plan):
    completed = set()
    pattern = re.compile(r"/chunks/(\d{5})/attempts/[^/]+/manifest\.json$")
    for item in store.list(f"{config.control_uri}/chunks"):
        match = pattern.search("/" + item["key"])
        if match:
            completed.add(int(match.group(1)))
    return len(completed)


def select_manifests(config, store, plan):
    uri = selected_uri(config)
    if store.exists(uri):
        return store.read_json(uri)["chunks"]
    selected = []
    missing = []
    for index in range(plan["chunk_count"]):
        valid = chunk_manifests(config, store, index)
        if valid:
            selected.append(valid[0])
        else:
            missing.append(index)
    if missing:
        raise RuntimeError(f"No valid successful manifest for chunks: {missing}")
    value = {
        "version": 1,
        "run_id": config.run_id,
        "chunks": selected,
        "selected_at": datetime.now(timezone.utc).isoformat(),
    }
    store.write_immutable_json(uri, value)
    return selected
