"""Tests for :class:`geoseeq.result.bioinfo.SampleBioInfoFolder.read_file`.

Covers the read_id normalization absorbed from PR #44: all three input
forms (bare, seq-type-prefixed, fully-prefixed) must produce the same
canonical ``<module>::<read>::<lane>`` file name, with no
double-prefixing of the seq_type.
"""
from unittest.mock import MagicMock

import pytest

from geoseeq.result.bioinfo import SampleBioInfoFolder


def _make_folder(module_name):
    """Return a stub SampleBioInfoFolder whose ``result_file`` records its argument."""
    folder = SampleBioInfoFolder.__new__(SampleBioInfoFolder)
    folder.module_name = module_name
    folder.result_file = MagicMock(side_effect=lambda name: ("file", name))
    return folder


@pytest.mark.parametrize(
    "read_id",
    [
        "read_1::lane_1",                              # bare
        "paired_end::read_1::lane_1",                  # seq-type-prefixed
        "short_read::paired_end::read_1::lane_1",      # fully prefixed
    ],
)
def test_read_file_normalizes_all_three_input_forms(read_id):
    """All three input shapes produce the canonical fully-prefixed file name."""
    folder = _make_folder("short_read::paired_end")
    folder.read_file(read_id)
    folder.result_file.assert_called_once_with(
        "short_read::paired_end::read_1::lane_1"
    )


def test_read_file_default_arg_is_bare_form():
    """The default ``read_id`` is the bare form and resolves to canonical."""
    folder = _make_folder("short_read::paired_end")
    folder.read_file()
    folder.result_file.assert_called_once_with(
        "short_read::paired_end::read_1::lane_1"
    )


def test_read_file_single_end_seq_type_prefix():
    """Seq-type prefix normalization works for single_end (no paired-end check)."""
    folder = _make_folder("short_read::single_end")
    folder.read_file("single_end::read_1::lane_2")
    folder.result_file.assert_called_once_with(
        "short_read::single_end::read_1::lane_2"
    )


def test_read_file_nanopore_seq_type_prefix():
    """Seq-type prefix normalization works for long_read::nanopore too."""
    folder = _make_folder("long_read::nanopore")
    folder.read_file("nanopore::read_1::lane_1")
    folder.result_file.assert_called_once_with(
        "long_read::nanopore::read_1::lane_1"
    )


def test_read_file_no_double_prefix_with_seq_type_input():
    """Regression: seq-type-prefixed input must NOT produce a doubled prefix."""
    folder = _make_folder("short_read::paired_end")
    folder.read_file("paired_end::read_2::lane_1")
    called_with = folder.result_file.call_args[0][0]
    # The bug being fixed would have produced
    # "short_read::paired_end::paired_end::read_2::lane_1" here.
    assert called_with == "short_read::paired_end::read_2::lane_1"
    assert called_with.count("paired_end") == 1


def test_read_1_and_read_2_helpers_unchanged():
    """Existing ``read_1``/``read_2`` helpers still produce canonical names."""
    folder = _make_folder("short_read::paired_end")
    folder.read_1("lane_3")
    folder.result_file.assert_called_once_with(
        "short_read::paired_end::read_1::lane_3"
    )
    folder.result_file.reset_mock()
    folder.read_2("lane_3")
    folder.result_file.assert_called_once_with(
        "short_read::paired_end::read_2::lane_3"
    )
