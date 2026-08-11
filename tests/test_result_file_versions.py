"""Unit tests for pinning a ResultFile to a historical version.

A ResultFile can be pinned to a specific past version by either
`version_replicate` (opaque id) or `version_index` (integer). On fetch,
its `stored_data` is overwritten with that version's stored_data so the
normal download path serves the pinned version.
"""

import pytest
from unittest.mock import MagicMock

from geoseeq.remote_object import RemoteObjectError
from geoseeq.result.result_file import SampleResultFile


CURRENT_BLOB = {
    "uuid": "field-uuid-1",
    "name": "R1",
    "stored_data": {"url": "https://example.com/current"},
    "created_at": "2026-06-17T00:00:00Z",
    "updated_at": "2026-06-17T00:00:00Z",
    "pipeline_run": None,
}

VERSIONS_BLOB = {
    "versioned_fields": [
        {
            "id": "vrf-2",
            "index": 1,
            "version_replicate": "aaaaaaaaaaaa",
            "stored_data": {"url": "https://example.com/v1"},
            "name": "R1",
        },
        {
            "id": "vrf-1",
            "index": 0,
            "version_replicate": "bbbbbbbbbbbb",
            "stored_data": {"url": "https://example.com/v0"},
            "name": "R1",
        },
    ],
}


def _make_file(**version_kwargs):
    """Build a SampleResultFile whose knex returns the current then versions blob."""
    knex = MagicMock()
    knex.get.side_effect = [dict(CURRENT_BLOB), dict(VERSIONS_BLOB)]
    parent = MagicMock()
    parent.nested_url.return_value = "sample_ars/parent-uuid"
    return SampleResultFile(knex, parent, "R1", **version_kwargs)


def test_pin_by_version_replicate():
    """Pinning by version_replicate overwrites stored_data with that version."""
    result_file = _make_file(version_replicate="bbbbbbbbbbbb")
    result_file._get()
    assert result_file.stored_data == {"url": "https://example.com/v0"}


def test_pin_by_version_index():
    """Pinning by version_index overwrites stored_data with that indexed version."""
    result_file = _make_file(version_index=1)
    result_file._get()
    assert result_file.stored_data == {"url": "https://example.com/v1"}


def test_no_pin_keeps_current_version():
    """Without a pin, stored_data stays the current version and versions aren't fetched."""
    knex = MagicMock()
    knex.get.side_effect = [dict(CURRENT_BLOB)]
    parent = MagicMock()
    result_file = SampleResultFile(knex, parent, "R1")
    result_file._get()
    assert result_file.stored_data == {"url": "https://example.com/current"}
    assert knex.get.call_count == 1


def test_missing_version_raises():
    """A pin that matches no version raises a clear error."""
    result_file = _make_file(version_replicate="does-not-exist")
    with pytest.raises(RemoteObjectError):
        result_file._get()


def test_both_pins_raises_value_error():
    """Passing both version_replicate and version_index is rejected."""
    with pytest.raises(ValueError):
        SampleResultFile(
            MagicMock(), MagicMock(), "R1",
            version_replicate="aaaaaaaaaaaa", version_index=0,
        )


def test_project_result_file_threads_version_params():
    """ProjectResultFolder.result_file() forwards version params to ProjectResultFile."""
    from geoseeq.result.result_file import ProjectResultFile
    knex = MagicMock()
    knex.get.side_effect = [dict(CURRENT_BLOB), dict(VERSIONS_BLOB)]
    parent = MagicMock()
    rf = ProjectResultFile(knex, parent, "R1", version_replicate="bbbbbbbbbbbb")
    rf._get()
    assert rf.stored_data == {"url": "https://example.com/v0"}
