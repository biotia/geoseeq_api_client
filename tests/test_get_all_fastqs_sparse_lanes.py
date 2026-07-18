"""get_all_fastqs must tolerate the server's sparse lane arrays.

The all-fastqs endpoint returns a lane-indexed array sized to the highest lane
number, with empty placeholders ([None, None] paired / None single) for lanes
that have no reads. A sample whose reads sit on lane 3 (not lane 1) therefore
yields leading empty lanes. The consumer previously grabbed file_grns[0]
unconditionally and handed a None grn to _grn_to_file -> None.split(":") crash.
"""
from unittest.mock import MagicMock

from geoseeq.sample import Sample


def _sample(blob):
    knex = MagicMock()
    knex.get.return_value = blob
    s = Sample(knex, MagicMock(), "s1")
    s.uuid = "sample-uuid"
    s._grn_to_file = MagicMock(side_effect=lambda grn: f"FILE({grn})")
    return s


def test_paired_end_reads_on_lane_3_skip_empty_lanes():
    blob = {"short_read::paired_end": {"raw::raw_reads": [[None, None], [None, None], ["grn:r1", "grn:r2"]]}}
    s = _sample(blob)
    files = s.get_all_fastqs()
    assert files["short_read::paired_end"]["raw::raw_reads"] == [["FILE(grn:r1)", "FILE(grn:r2)"]]
    # never resolves a None grn (the regression)
    assert all(c.args[0] is not None for c in s._grn_to_file.call_args_list)


def test_single_end_skips_none_lanes():
    blob = {"short_read::single_end": {"raw::raw_reads": [None, None, "grn:r1"]}}
    s = _sample(blob)
    files = s.get_all_fastqs()
    assert files["short_read::single_end"]["raw::raw_reads"] == ["FILE(grn:r1)"]


def test_paired_end_lane_1_unchanged():
    blob = {"short_read::paired_end": {"raw::raw_reads": [["grn:r1", "grn:r2"]]}}
    s = _sample(blob)
    files = s.get_all_fastqs()
    assert files["short_read::paired_end"]["raw::raw_reads"] == [["FILE(grn:r1)", "FILE(grn:r2)"]]


def test_paired_end_malformed_short_pair_skipped():
    # a length-1 "pair" must not IndexError; it's skipped
    blob = {"short_read::paired_end": {"raw::raw_reads": [["grn:r1"], ["grn:r1", "grn:r2"]]}}
    s = _sample(blob)
    files = s.get_all_fastqs()
    assert files["short_read::paired_end"]["raw::raw_reads"] == [["FILE(grn:r1)", "FILE(grn:r2)"]]


def test_paired_end_multiple_real_lanes_all_kept():
    blob = {"short_read::paired_end": {"raw::raw_reads": [["grn:l1r1", "grn:l1r2"], ["grn:l2r1", "grn:l2r2"]]}}
    s = _sample(blob)
    files = s.get_all_fastqs()
    assert files["short_read::paired_end"]["raw::raw_reads"] == [
        ["FILE(grn:l1r1)", "FILE(grn:l1r2)"],
        ["FILE(grn:l2r1)", "FILE(grn:l2r2)"],
    ]
