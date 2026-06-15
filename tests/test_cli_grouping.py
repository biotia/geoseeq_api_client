"""Public-API surface check for `geoseeq.cli._grouping`.

Behavior of the grouping helpers is covered indirectly by the upload-reads
CLI tests (`tests/test_upload_cli.py`). This module only asserts that the
shared helper imports cleanly and exposes the documented public callables,
so refactors that break the shared surface fail fast.
"""
import inspect

import geoseeq.cli._grouping as grouping
from geoseeq.cli._grouping import get_regex, group_files


def test_module_exports_public_helpers():
    """The module exposes `get_regex` and `group_files` as top-level names."""
    assert hasattr(grouping, "get_regex")
    assert hasattr(grouping, "group_files")


def test_get_regex_is_callable():
    """`get_regex` is a callable function (not e.g. a module attribute)."""
    assert callable(get_regex)
    assert inspect.isfunction(get_regex)


def test_group_files_is_callable():
    """`group_files` is a callable function."""
    assert callable(group_files)
    assert inspect.isfunction(group_files)


def test_get_regex_signature():
    """`get_regex` has the documented (knex, filepaths, module_name, lib, regex) shape."""
    params = list(inspect.signature(get_regex).parameters)
    assert params == ["knex", "filepaths", "module_name", "lib", "regex"]


def test_group_files_signature():
    """`group_files` has the documented parameter list."""
    params = list(inspect.signature(group_files).parameters)
    expected = ["knex", "filepaths", "module_name", "regex", "yes", "name_map"]
    assert params == expected


def test_upload_reads_uses_shared_helpers():
    """The upload-reads module imports the shared helpers (no duplicated copies)."""
    from geoseeq.cli.upload import upload_reads

    # Sanity: the names are bound in the module and refer to the shared ones.
    assert upload_reads.get_regex is get_regex
    assert upload_reads.group_files is group_files
