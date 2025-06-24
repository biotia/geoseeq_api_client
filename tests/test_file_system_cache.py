import json
from pathlib import Path

import pytest

from geoseeq import file_system_cache
from geoseeq.file_system_cache import FileSystemCache


def test_cache_blob_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(file_system_cache, "USE_GEOSEEQ_CACHE", True)
    monkeypatch.setattr(file_system_cache, "GEOSEEQ_CACHE_DIR", str(tmp_path))
    cache = FileSystemCache(timeout=60)
    obj = "object-id"
    blob = {"a": 1}
    cache.cache_blob(obj, blob)
    assert cache.get_cached_blob(obj) == blob
    cache.clear_blob(obj)
    assert cache.get_cached_blob(obj) is None
