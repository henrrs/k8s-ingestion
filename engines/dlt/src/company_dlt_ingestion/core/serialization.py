"""Deterministic serialization shared by application and storage adapters."""

import hashlib
import json


def canonical_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()


def content_hash(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()
