"""Unit tests for threading a version pin through the result-file id-constructors.

The id-constructor path (project_result_file_from_id -> project_result_file_from_name)
must forward version_replicate/version_index into the folder's result_file(...) call
and then .get() the returned file so the pin takes effect. This is the path Biotia-DX
uses to resolve result files by absolute name during DB downloads.

A real (unauthenticated) Knex is used as the knex arg because the @with_knex
decorator only passes through genuine Knex instances; the folder resolver is
patched so no network call is made.
"""

from unittest.mock import MagicMock, patch

import pytest

from geoseeq.knex import Knex
from geoseeq.id_constructors import from_names, from_ids
from geoseeq.result.result_file import ProjectResultFile


NAME = "My Org/My Project/My Result Folder/My Result File"
SAMPLE_NAME = "My Org/My Project/My Sample/My Result Folder/My Result File"


def _folder_returning(r_file):
    """Build a mock project result folder whose result_file() returns r_file."""
    r_folder = MagicMock()
    r_folder.result_file.return_value = r_file
    return r_folder


def test_project_result_file_from_name_forwards_pin():
    """project_result_file_from_name forwards the pin to result_file() and .get()s it."""
    r_file = MagicMock()
    r_folder = _folder_returning(r_file)
    with patch.object(from_names, "project_result_folder_from_name", return_value=r_folder):
        out = from_names.project_result_file_from_name(Knex(), NAME, version_replicate="abc")
    r_folder.result_file.assert_called_once_with(
        "My Result File", version_replicate="abc", version_index=None
    )
    r_file.get.assert_called_once()
    assert out is r_file


def test_project_result_file_from_id_forwards_pin_via_name_path():
    """A pinned id resolves through the name path and forwards the pin to result_file()."""
    r_file = MagicMock()
    r_folder = _folder_returning(r_file)
    with patch.object(from_names, "project_result_folder_from_name", return_value=r_folder):
        out = from_ids.project_result_file_from_id(Knex(), NAME, version_replicate="abc")
    r_folder.result_file.assert_called_once_with(
        "My Result File", version_replicate="abc", version_index=None
    )
    r_file.get.assert_called_once()
    assert out is r_file


def test_project_result_file_from_id_uuid_pin_raises():
    """Pinning a version by UUID/GRN is unsupported and raises NotImplementedError."""
    uuid = "d5e5f5a5-1111-2222-3333-444455556666"
    with pytest.raises(NotImplementedError):
        from_ids.project_result_file_from_id(Knex(), uuid, version_replicate="abc")


def test_project_result_file_from_id_no_pin_uses_generic_dispatch():
    """Without a pin, resolution stays on the unchanged _generic_from_id dispatch."""
    r_file = MagicMock()
    r_folder = _folder_returning(r_file)
    with patch.object(from_names, "project_result_folder_from_name", return_value=r_folder):
        from_ids.project_result_file_from_id(Knex(), NAME)
    # No pin means version params default to None on the folder call.
    r_folder.result_file.assert_called_once_with(
        "My Result File", version_replicate=None, version_index=None
    )


def test_both_pins_propagate_value_error():
    """Passing both version_replicate and version_index raises at the ResultFile layer."""
    real_knex = Knex()

    class _RealFolder:
        """Minimal folder whose result_file builds a real ProjectResultFile."""

        knex = real_knex

        def result_file(self, field_name, version_replicate=None, version_index=None):
            return ProjectResultFile(
                real_knex, MagicMock(), field_name,
                version_replicate=version_replicate, version_index=version_index,
            )

    with patch.object(from_names, "project_result_folder_from_name", return_value=_RealFolder()):
        with pytest.raises(ValueError):
            from_ids.project_result_file_from_id(
                real_knex, NAME, version_replicate="abc", version_index=0
            )


# ---------------------------------------------------------------------------
# Sample twin — sample_result_file_from_name / sample_result_file_from_id
# ---------------------------------------------------------------------------


def test_sample_result_file_from_name_forwards_pin():
    """sample_result_file_from_name forwards the pin to result_file() and .get()s it."""
    r_file = MagicMock()
    r_folder = _folder_returning(r_file)
    with patch.object(from_names, "sample_result_folder_from_name", return_value=r_folder):
        out = from_names.sample_result_file_from_name(
            Knex(), SAMPLE_NAME, version_replicate="xyz"
        )
    r_folder.result_file.assert_called_once_with(
        "My Result File", version_replicate="xyz", version_index=None
    )
    r_file.get.assert_called_once()
    assert out is r_file


def test_sample_result_file_from_id_forwards_pin_via_name_path():
    """A pinned sample id resolves through the name path and forwards the pin."""
    r_file = MagicMock()
    r_folder = _folder_returning(r_file)
    with patch.object(from_names, "sample_result_folder_from_name", return_value=r_folder):
        out = from_ids.sample_result_file_from_id(
            Knex(), SAMPLE_NAME, version_replicate="xyz"
        )
    r_folder.result_file.assert_called_once_with(
        "My Result File", version_replicate="xyz", version_index=None
    )
    r_file.get.assert_called_once()
    assert out is r_file


def test_sample_result_file_from_id_uuid_pin_raises():
    """Pinning a sample result file version by UUID/GRN raises NotImplementedError."""
    uuid = "d5e5f5a5-1111-2222-3333-444455556666"
    with pytest.raises(NotImplementedError):
        from_ids.sample_result_file_from_id(Knex(), uuid, version_replicate="xyz")


def test_sample_result_file_from_id_no_pin_uses_generic_dispatch():
    """Without a pin, sample resolution stays on the unchanged _generic_from_id dispatch."""
    r_file = MagicMock()
    r_folder = _folder_returning(r_file)
    with patch.object(from_names, "sample_result_folder_from_name", return_value=r_folder):
        from_ids.sample_result_file_from_id(Knex(), SAMPLE_NAME)
    r_folder.result_file.assert_called_once_with(
        "My Result File", version_replicate=None, version_index=None
    )
