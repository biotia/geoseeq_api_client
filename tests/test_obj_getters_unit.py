"""Unit tests for _get_or_create_folder in obj_getters.py.

Exercises the create=False reraise path and the create=True+yes path
added/refactored by the deterministic-replicate PR. No network calls;
the folder object is fully mocked.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from geoseeq.cli.shared_params.obj_getters import _get_or_create_folder
from geoseeq.knex import GeoseeqNotFoundError
from geoseeq.result.result_folder import SampleResultFolder


def _mock_folder(*, get_raises=False, module_name="mod::v1"):
    """Return a mock folder with _resolve_replicate stubbed out."""
    folder = MagicMock(spec=SampleResultFolder)
    folder.module_name = module_name
    folder._resolve_replicate = MagicMock()
    if get_raises:
        folder.get.side_effect = GeoseeqNotFoundError("not found")
    else:
        folder.get.return_value = folder
    return folder


def test_get_or_create_folder_get_succeeds_returns_folder():
    """Happy path: folder already exists, get() returns it, no create called."""
    folder = _mock_folder()
    result = _get_or_create_folder(folder, "mod::v1", yes=True, create=True)
    folder._resolve_replicate.assert_called_once()
    folder.get.assert_called_once()
    folder.create.assert_not_called()
    assert result is folder.get.return_value


def test_get_or_create_folder_create_false_reraises_not_found():
    """create=False: GeoseeqNotFoundError from get() propagates to the caller."""
    folder = _mock_folder(get_raises=True)
    with pytest.raises(GeoseeqNotFoundError):
        _get_or_create_folder(folder, "mod::v1", yes=True, create=False)
    folder._resolve_replicate.assert_called_once()
    folder.create.assert_not_called()


def test_get_or_create_folder_create_true_yes_creates_missing_folder():
    """create=True + yes=True: missing folder is created without a confirm prompt."""
    folder = _mock_folder(get_raises=True)
    folder.create.return_value = MagicMock(name="created_folder")

    result = _get_or_create_folder(folder, "mod::v1", yes=True, create=True)

    folder._resolve_replicate.assert_called_once()
    folder.create.assert_called_once()
    assert result is folder.create.return_value


def test_project_folder_adopts_single_existing():
    """ProjectResultFolder adopt-single: a lone sibling's replicate is inherited."""
    from geoseeq.result.result_folder import DEFAULT_REPLICATE, ProjectResultFolder

    existing_replicate = "legacy-abc123"

    class _FakeExisting:
        module_name = "short_read::single_end"
        replicate = existing_replicate

    class _FakeGrp:
        name = "G"
        inherited_url_options = {}

        def get_result_folders(self):
            return [_FakeExisting()]

        def pre_hash(self):
            return "grp"

        def idem(self):
            return self

        def nested_url(self):
            return "sample_groups/G"

    folder = ProjectResultFolder(
        MagicMock(name="knex"), _FakeGrp(), "short_read::single_end", replicate=None
    )
    folder._resolve_replicate()
    assert folder.replicate == existing_replicate
