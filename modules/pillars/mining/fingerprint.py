"""Deterministic identity of captured data (independent of technical write times)."""

import hashlib
import json
from .serialization import to_json_value


def content_hash(value):
    digest = hashlib.sha256()
    for chunk in json.JSONEncoder(
        sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=to_json_value
    ).iterencode(value):
        digest.update(chunk.encode("utf-8"))
    return digest.hexdigest()
