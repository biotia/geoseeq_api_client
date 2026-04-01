"""Unit tests for ``geoseeq s3 ls``."""

import sys
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.s3 import _format_ls_row, cli_s3
from geoseeq.knex import GeoseeqNotFoundError


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_entry(key, size, last_modified="2026-03-28T12:00:00Z"):
    """Return a minimal staged-file entry dict."""
    return {"key": key, "size": size, "last_modified": last_modified}


# ---------------------------------------------------------------------------
# _format_ls_row unit tests
# ---------------------------------------------------------------------------


class TestFormatLsRow:
    """Tests for the pure formatting helper."""

    def test_basic_row(self):
        """Timestamp, size, and key appear in correct positions."""
        entry = _make_entry("staging/abc/file.fastq.gz", 12345678)
        row = _format_ls_row(8, entry)
        assert "2026-03-28 12:00:00" in row
        assert "12345678" in row
        assert "staging/abc/file.fastq.gz" in row

    def test_size_right_aligned(self):
        """Smaller size is padded to match size_width."""
        entry = _make_entry("staging/abc/tiny.bam", 999)
        row = _format_ls_row(8, entry)
        # '999' right-padded to width 8 → '     999'
        assert "     999" in row

    def test_timestamp_iso_truncated_to_seconds(self):
        """Sub-second or timezone suffixes are stripped."""
        entry = _make_entry("k", 1, last_modified="2026-03-28T11:30:45.123456Z")
        row = _format_ls_row(1, entry)
        assert "2026-03-28 11:30:45" in row
        assert "." not in row.split("  ")[0]

    def test_missing_last_modified_uses_spaces(self):
        """Missing timestamp field produces a 19-space placeholder."""
        entry = {"key": "k", "size": 0}
        row = _format_ls_row(1, entry)
        assert row.startswith(" " * 19)

    def test_missing_size_defaults_to_zero(self):
        """Missing size defaults to 0."""
        entry = {"key": "k", "last_modified": "2026-01-01T00:00:00Z"}
        row = _format_ls_row(1, entry)
        assert "0" in row


# ---------------------------------------------------------------------------
# CLI integration tests (mocked HTTP)
# ---------------------------------------------------------------------------


@pytest.fixture
def runner():
    """Click test runner."""
    return CliRunner(mix_stderr=False)


def _invoke_ls(runner, project_id, extra_args=None, api_response=None, api_exc=None):
    """Invoke ``s3 ls`` with a mocked knex.

    Either ``api_response`` (list) or ``api_exc`` (exception) must be given to
    control what the mocked GET call returns.
    """
    extra_args = extra_args or []

    mock_proj = MagicMock()
    mock_proj.uuid = "test-proj-uuid-1234"

    mock_knex = MagicMock()
    mock_knex.set_auth_required.return_value = mock_knex
    if api_exc is not None:
        mock_knex.get.side_effect = api_exc
    else:
        mock_knex.get.return_value = api_response

    with patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj), \
         patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex):
        result = runner.invoke(
            cli_s3,
            ["ls"] + extra_args + [project_id],
            catch_exceptions=False,
        )
    return result, mock_knex


class TestS3LsCommand:
    """Behaviour tests for ``geoseeq s3 ls``."""

    def test_happy_path_prints_rows(self, runner):
        """One row per entry is printed to stdout."""
        entries = [
            _make_entry("staging/abc/file1.fastq.gz", 12345678),
            _make_entry("staging/abc/file2.bam", 987654, "2026-03-28T11:30:00Z"),
        ]
        result, _ = _invoke_ls(runner, "My Org/My Project", api_response=entries)

        assert result.exit_code == 0
        assert "staging/abc/file1.fastq.gz" in result.output
        assert "staging/abc/file2.bam" in result.output
        assert "2026-03-28 12:00:00" in result.output
        assert "2026-03-28 11:30:00" in result.output

    def test_empty_staging_area_message(self, runner):
        """Empty list prints a human-friendly message instead of a blank table."""
        result, _ = _invoke_ls(runner, "My Org/My Project", api_response=[])

        assert result.exit_code == 0
        assert "No files in staging area." in result.output

    def test_all_users_flag_emits_warning(self, runner):
        """``--all-users`` warns on stderr and still lists files."""
        entries = [_make_entry("staging/abc/file.fastq.gz", 100)]
        result, _ = _invoke_ls(
            runner, "My Org/My Project",
            extra_args=["--all-users"],
            api_response=entries,
        )

        assert result.exit_code == 0
        assert "--all-users is not yet supported" in result.stderr
        # And the file still appears
        assert "staging/abc/file.fastq.gz" in result.output

    def test_api_error_exits_nonzero(self, runner):
        """An API error produces a non-zero exit code and error message on stderr."""
        result, _ = _invoke_ls(
            runner, "My Org/My Project",
            api_exc=GeoseeqNotFoundError("project not found"),
        )

        assert result.exit_code != 0
        assert "Error listing staged files" in result.stderr
        assert result.output.strip() == ""

    def test_correct_endpoint_called(self, runner):
        """The GET call targets the expected URL path."""
        entries = [_make_entry("k", 1)]
        _, mock_knex = _invoke_ls(runner, "My Org/My Project", api_response=entries)

        mock_knex.get.assert_called_once_with(
            "sample_groups/test-proj-uuid-1234/list_staged_files"
        )

    def test_size_column_consistent_width(self, runner):
        """The size column is right-aligned with consistent width across all rows."""
        entries = [
            _make_entry("big.fastq.gz", 123456789),
            _make_entry("small.bam", 1),
        ]
        result, _ = _invoke_ls(runner, "My Org/My Project", api_response=entries)

        assert result.exit_code == 0
        lines = [l for l in result.output.splitlines() if l.strip()]
        # Both lines should have the same total length up to the key separator
        prefixes = [line.rsplit("  ", 1)[0] for line in lines]
        assert all(len(p) == len(prefixes[0]) for p in prefixes)
