import os
import tempfile
from pathlib import Path

import pytest

from geoseeq.result.utils import check_json_serialization, md5_checksum
from geoseeq.remote_object import RemoteObjectError


def test_check_json_serialization_passes():
    data = {"a": [1, 2, {"b": 3}]}
    check_json_serialization(data)  # should not raise


def test_check_json_serialization_fails():
    with pytest.raises(RemoteObjectError):
        check_json_serialization({"a": (1, 2)})


def test_md5_checksum(tmp_path):
    file_path = tmp_path / "file.txt"
    content = b"hello world"
    file_path.write_bytes(content)
    assert md5_checksum(str(file_path)) == md5_checksum(str(file_path))
