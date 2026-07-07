"""Unit tests for deterministic result-folder replicate resolution.

Mirrors the style of ``test_upload_reads_unit.py`` but exercises the
library/model-level resolution added to ``ResultFolder`` (generalizing PR
#75's reads-only ``_resolve_read_replicate`` to all result-folder
get-or-create).

The tests mock ``parent.get_result_folders()`` and never hit the network.
"""

import logging
from unittest.mock import MagicMock

import pytest

from geoseeq.result.result_folder import (
    DEFAULT_REPLICATE,
    ProjectResultFolder,
    SampleResultFolder,
)


MODULE = "short_read::single_end"


class _FakeExistingFolder:
    """Stand-in for a folder returned by ``parent.get_result_folders()``."""

    def __init__(self, module_name, replicate, updated_at=None):
        self.module_name = module_name
        self.replicate = replicate
        self.updated_at = updated_at


class _FakeParent:
    """In-memory stand-in for a Sample or Project (the folder's parent)."""

    def __init__(self, name="P", folders=None):
        self.name = name
        self._folders = list(folders or [])
        self.inherited_url_options = {}

    def get_result_folders(self):
        return list(self._folders)

    def pre_hash(self):
        return "parent"

    def idem(self):
        return self

    def nested_url(self):
        return "parents/P"


def _sample_folder(parent, module_name=MODULE, replicate=None):
    return SampleResultFolder(MagicMock(name="knex"), parent, module_name, replicate=replicate)


def _project_folder(parent, module_name=MODULE, replicate=None):
    return ProjectResultFolder(MagicMock(name="knex"), parent, module_name, replicate=replicate)


# ---------------------------------------------------------------------------
# _resolve_replicate: the 0 / 1 / many / explicit rules
# ---------------------------------------------------------------------------


def test_resolve_replicate_no_existing_returns_default_sample():
    """A sample folder with no existing sibling for the module gets '1'."""
    folder = _sample_folder(_FakeParent())
    folder._resolve_replicate()
    assert folder.replicate == DEFAULT_REPLICATE


def test_resolve_replicate_no_existing_returns_default_project():
    """A project folder with no existing sibling for the module gets '1'."""
    folder = _project_folder(_FakeParent())
    folder._resolve_replicate()
    assert folder.replicate == DEFAULT_REPLICATE


def test_resolve_replicate_adopts_single_existing():
    """Exactly one existing folder (even a random replicate) is adopted."""
    existing = _FakeExistingFolder(MODULE, replicate="a1b2c3d4e5f6")
    folder = _sample_folder(_FakeParent(folders=[existing]))
    folder._resolve_replicate()
    assert folder.replicate == "a1b2c3d4e5f6"


def test_resolve_replicate_ignores_other_modules():
    """Existing folders for a different module_name do not count."""
    other = _FakeExistingFolder("long_read::nanopore", replicate="zzz")
    folder = _sample_folder(_FakeParent(folders=[other]))
    folder._resolve_replicate()
    assert folder.replicate == DEFAULT_REPLICATE


def test_resolve_replicate_multiple_picks_most_recent_and_warns(caplog):
    """Ambiguous legacy state: newest folder wins and a warning is logged."""
    older = _FakeExistingFolder(MODULE, replicate="old", updated_at="2020-01-01")
    newer = _FakeExistingFolder(MODULE, replicate="new", updated_at="2024-06-01")
    folder = _sample_folder(_FakeParent(folders=[older, newer]))
    with caplog.at_level(logging.WARNING, logger="geoseeq_api"):
        folder._resolve_replicate()
    assert folder.replicate == "new"
    assert "2 'short_read::single_end' result folders" in caplog.text
    assert "explicit replicate" in caplog.text


def test_resolve_replicate_explicit_is_unchanged_and_skips_lookup():
    """An explicit replicate is respected verbatim; no parent lookup happens."""
    parent = _FakeParent(folders=[_FakeExistingFolder(MODULE, replicate="rand")])
    parent.get_result_folders = MagicMock(side_effect=AssertionError("must not be called"))
    folder = _sample_folder(parent, replicate="lane2")
    folder._resolve_replicate()
    assert folder.replicate == "lane2"
    parent.get_result_folders.assert_not_called()


# ---------------------------------------------------------------------------
# idem() / create() delegate to resolution first
# ---------------------------------------------------------------------------


def test_idem_resolves_then_reuses_existing_folder_no_sibling():
    """A second get-or-create for the same module reuses the adopted folder.

    The parent already carries a folder for the module under replicate '1'
    (as the first create would have produced). ``idem()`` must resolve to
    that replicate and route through ``get`` — never mint a random sibling.
    """
    existing = _FakeExistingFolder(MODULE, replicate="1")
    folder = _sample_folder(_FakeParent(folders=[existing]))
    folder.get = MagicMock(name="get")  # idem's get path succeeds -> no create
    folder.create = MagicMock(name="create")

    folder.idem()

    assert folder.replicate == "1"  # adopted, not a fresh random replicate
    folder.get.assert_called_once()
    folder.create.assert_not_called()


def test_create_resolves_new_folder_to_default():
    """create() on a brand-new folder resolves to '1' before POSTing."""
    folder = _sample_folder(_FakeParent())
    folder._create = MagicMock(name="_create")  # avoid the real POST

    folder.create()

    assert folder.replicate == DEFAULT_REPLICATE
    folder._create.assert_called_once()
