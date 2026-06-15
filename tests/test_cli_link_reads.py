"""Unit tests for ``geoseeq link reads``.

The S3 listing layer and the geoseeq object graph are both mocked so
the tests run hermetically. The flow under test is:

    S3Source.list_keys -> bulk_upload/validate_filenames ->
    bulk_upload/group_files -> per-sample idem + per-field link_s3

Tests cover: dry-run preview, commit happy path, idempotency, name
canonicalization (no double-prefixing), per-sample dedup
(perf-regression guard), default privacy level, and the
no-keys-found error path.
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.link import cli_link
from geoseeq.cli.link.link_reads import (
    _build_actions,
    _build_filename_index,
    _resolve_regex_project_uuid,
)

# Click 8.2 removed mix_stderr; on 8.2+ stderr is mixed into output and
# cannot be captured separately. Mirrors the convention in test_s3_register_cli.
_CAN_SEPARATE_STDERR = "mix_stderr" in inspect.signature(CliRunner.__init__).parameters
_RUNNER_KWARGS = {"mix_stderr": False} if _CAN_SEPARATE_STDERR else {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROJECT_UUID = "11111111-2222-3333-4444-555555555555"
_BUCKET = "test-bucket"
_PREFIX = "myproject/"
_ENDPOINT = "https://s3.us-east-005.backblazeb2.com"

# Two samples; sample_A is paired-end across two lanes (4 fields), sample_B
# is one lane. The sample_A shape is the perf-relevant case: naive looping
# would re-idem the sample/folder once per field.
_KEYS = [
    "myproject/sample_A_L001_R1_001.fastq.gz",
    "myproject/sample_A_L001_R2_001.fastq.gz",
    "myproject/sample_A_L002_R1_001.fastq.gz",
    "myproject/sample_A_L002_R2_001.fastq.gz",
    "myproject/sample_B_L001_R1_001.fastq.gz",
    "myproject/sample_B_L001_R2_001.fastq.gz",
]

# Server returns field names with the seq-type prefix already in place
# (e.g. "paired_end::read_1::lane_001"). The absorbed PR #44 fix is what
# lets us pass these straight to folder.read_file() without
# double-prefixing.
_GROUPS = [
    {
        "sample_name": "sample_A",
        "fields": {
            "paired_end::read_1::lane_001": "sample_A_L001_R1_001.fastq.gz",
            "paired_end::read_2::lane_001": "sample_A_L001_R2_001.fastq.gz",
            "paired_end::read_1::lane_002": "sample_A_L002_R1_001.fastq.gz",
            "paired_end::read_2::lane_002": "sample_A_L002_R2_001.fastq.gz",
        },
    },
    {
        "sample_name": "sample_B",
        "fields": {
            "paired_end::read_1::lane_001": "sample_B_L001_R1_001.fastq.gz",
            "paired_end::read_2::lane_001": "sample_B_L001_R2_001.fastq.gz",
        },
    },
]


def _validate_response(unmatched=None):
    return {"regex_used": r".*", "unmatched": unmatched or []}


def _make_knex(validate=None, groups=None):
    """Return a mock knex whose ``post`` routes the two grouping endpoints."""
    knex = MagicMock()
    validate = validate or _validate_response()
    groups = groups if groups is not None else _GROUPS

    def _post(endpoint, json):
        if endpoint == "bulk_upload/validate_filenames":
            return validate
        if endpoint == "bulk_upload/group_files":
            return groups
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    knex.post.side_effect = _post
    return knex


def _make_proj_mock():
    """Return a project mock that records sample/folder/file idem + link_s3 calls.

    Each ``proj.sample(name)`` returns the SAME ``Sample`` mock for the same
    name; same for folder and file lookups under it. This is what
    geoseeq's real ``.idem()`` semantics give us (get-or-create), so the
    perf-dedup assertion below is the legitimate call-count check.
    """
    proj = MagicMock()
    proj.uuid = _PROJECT_UUID
    sample_mocks: dict[str, MagicMock] = {}

    def _sample(name):
        if name not in sample_mocks:
            sample = MagicMock(name=f"sample[{name}]")
            sample.idem.return_value = sample

            folder_mocks: dict[str, MagicMock] = {}

            def _result_folder(module_name):
                if module_name not in folder_mocks:
                    folder = MagicMock(name=f"folder[{name}/{module_name}]")
                    folder.idem.return_value = folder
                    file_mocks: dict[str, MagicMock] = {}

                    def _read_file(field_name):
                        if field_name not in file_mocks:
                            rf = MagicMock(name=f"rf[{name}/{field_name}]")
                            rf.idem.return_value = rf
                            file_mocks[field_name] = rf
                        return file_mocks[field_name]

                    folder.read_file.side_effect = _read_file
                    folder_mocks[module_name] = folder
                return folder_mocks[module_name]

            sample.result_folder.side_effect = _result_folder
            sample_mocks[name] = sample
        return sample_mocks[name]

    proj.sample.side_effect = _sample
    proj._sample_mocks = sample_mocks  # exposed for assertions
    return proj


@pytest.fixture
def runner():
    """Click test runner with separated stdout/stderr where possible."""
    return CliRunner(**_RUNNER_KWARGS)


def _invoke(runner, args, knex=None, proj=None, keys=None,
            project_exc=None, source_factory=None):
    """Invoke ``geoseeq link reads`` with all I/O mocked. Returns (result, knex, proj)."""
    knex = knex if knex is not None else _make_knex()
    proj = proj if proj is not None else _make_proj_mock()
    keys = keys if keys is not None else _KEYS

    fake_source = MagicMock()
    fake_source.bucket = _BUCKET
    fake_source.prefix = _PREFIX
    fake_source.endpoint_url = _ENDPOINT
    fake_source.list_keys.return_value = iter(keys)
    fake_source.to_s3_uri.side_effect = lambda k: f"s3://{_BUCKET}/{k}"

    source_factory = source_factory or (lambda *a, **kw: fake_source)

    with patch("geoseeq.cli.link.link_reads.S3Source.from_uri",
               side_effect=source_factory), \
         patch("geoseeq.cli.link.link_reads.handle_project_id",
               return_value=proj, side_effect=project_exc), \
         patch("geoseeq.cli.shared_params.common_state.Knex",
               return_value=knex):
        result = runner.invoke(cli_link, args, catch_exceptions=False)
    return result, knex, proj


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestPureHelpers:
    """Tests for the small helpers extracted for testability."""

    def test_build_filename_index_strips_prefix(self):
        """Basenames map back to their full S3 keys."""
        idx = _build_filename_index(["a/b/foo.fastq.gz", "a/bar.fastq.gz"])
        assert idx == {"foo.fastq.gz": "a/b/foo.fastq.gz",
                       "bar.fastq.gz": "a/bar.fastq.gz"}

    def test_resolve_regex_uuid_prefers_target(self):
        """When the target project exists, its UUID is used."""
        proj = MagicMock()
        proj.uuid = "target-uuid"
        assert _resolve_regex_project_uuid(proj, "scratch-uuid") == "target-uuid"

    def test_resolve_regex_uuid_falls_back_to_scratch(self):
        """When the target has no UUID, fall back to the scratch UUID."""
        assert _resolve_regex_project_uuid(None, "scratch-uuid") == "scratch-uuid"

    def test_resolve_regex_uuid_returns_none_when_neither(self):
        """Neither target nor scratch available -> None (caller errors out)."""
        assert _resolve_regex_project_uuid(None, None) is None

    def test_build_actions_preserves_field_names(self):
        """Field names from group_files flow through unchanged into actions."""
        source = MagicMock()
        source.to_s3_uri.side_effect = lambda k: f"s3://b/{k}"
        idx = {"f.fastq.gz": "p/f.fastq.gz"}
        groups = [{"sample_name": "s", "fields": {"paired_end::read_1::lane_1": "f.fastq.gz"}}]
        actions = _build_actions(groups, idx, source)
        assert actions == [("s", "paired_end::read_1::lane_1", "s3://b/p/f.fastq.gz")]

    def test_build_actions_skips_unindexed_filenames(self):
        """A filename not in the basename index (e.g. unmatched) is skipped."""
        source = MagicMock()
        source.to_s3_uri.side_effect = lambda k: f"s3://b/{k}"
        groups = [{"sample_name": "s",
                   "fields": {"r1": "known.fastq.gz", "r2": "missing.fastq.gz"}}]
        actions = _build_actions(groups, {"known.fastq.gz": "known.fastq.gz"}, source)
        assert len(actions) == 1
        assert actions[0][1] == "r1"


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------


class TestDryRun:
    """Dry-run is the default; no writes happen."""

    def test_dry_run_is_default_and_writes_nothing(self, runner):
        """Without --commit, no sample/folder idem or link_s3 calls happen."""
        result, _, proj = _invoke(
            runner,
            ["reads", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
        )
        assert result.exit_code == 0, result.output
        assert proj.sample.call_count == 0

    def test_dry_run_preview_lists_actions(self, runner):
        """Dry-run prints the (sample, field, uri) preview tuples."""
        result, _, _ = _invoke(
            runner,
            ["reads", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
        )
        out = result.stderr if _CAN_SEPARATE_STDERR else result.output
        assert "WOULD LINK" in out
        assert "sample_A" in out
        assert "sample_B" in out
        assert f"s3://{_BUCKET}/myproject/sample_A_L001_R1_001.fastq.gz" in out

    def test_dry_run_errors_on_empty_listing(self, runner):
        """If S3 returns no matching keys, the command errors out."""
        result, _, _ = _invoke(
            runner,
            ["reads", "MyOrg/MyProject", f"s3://{_BUCKET}/empty/"],
            keys=[],
        )
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------


class TestCommit:
    """``--commit`` actually creates samples/folders and calls link_s3."""

    def test_commit_links_every_action(self, runner):
        """Every (sample, field) tuple results in exactly one link_s3 call."""
        result, _, proj = _invoke(
            runner,
            ["reads", "--commit", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
        )
        assert result.exit_code == 0, result.output
        # 4 fields for sample_A + 2 for sample_B = 6 total link_s3 calls.
        total_link_calls = 0
        for sample_name, group in zip(["sample_A", "sample_B"], _GROUPS):
            sample = proj._sample_mocks[sample_name]
            folder = sample.result_folder("short_read::paired_end")
            for field_name in group["fields"]:
                rf = folder.read_file(field_name)
                total_link_calls += rf.link_s3.call_count
        assert total_link_calls == 6

    def test_commit_link_s3_uses_endpoint_url(self, runner):
        """``link_s3`` is called with the source endpoint_url for non-AWS storage."""
        result, _, proj = _invoke(
            runner,
            ["reads", "--commit", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
        )
        assert result.exit_code == 0
        sample = proj._sample_mocks["sample_A"]
        folder = sample.result_folder("short_read::paired_end")
        rf = folder.read_file("paired_end::read_1::lane_001")
        rf.link_s3.assert_called_once()
        args, kwargs = rf.link_s3.call_args
        assert args[0].startswith(f"s3://{_BUCKET}/myproject/sample_A")
        assert kwargs.get("endpoint_url") == _ENDPOINT

    def test_commit_idempotent_field_names_pass_through(self, runner):
        """Field names with the seq-type prefix flow through unchanged.

        Regression guard against the double-prefix bug fixed by the
        absorbed PR #44 change: ``folder.read_file`` is called with the
        prefixed name from group_files, and the read_file logic is what
        normalizes — the command does not strip the prefix client-side.
        """
        result, _, proj = _invoke(
            runner,
            ["reads", "--commit", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
        )
        assert result.exit_code == 0
        sample = proj._sample_mocks["sample_A"]
        folder = sample.result_folder("short_read::paired_end")
        called_field_names = {
            c.args[0] for c in folder.read_file.call_args_list
        }
        # The prefixed names from group_files reach read_file as-is.
        assert "paired_end::read_1::lane_001" in called_field_names
        assert "paired_end::read_2::lane_002" in called_field_names

    def test_per_sample_dedup_call_count(self, runner):
        """Perf-regression guard: ``sample.idem`` runs once per sample, not per field.

        sample_A has 4 fields, sample_B has 2; naive per-file looping
        would call sample.idem and folder.idem 6 times each. The
        sample-grouped commit loop must call each exactly twice (once
        per sample).
        """
        result, _, proj = _invoke(
            runner,
            ["reads", "--commit", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
        )
        assert result.exit_code == 0

        for sample_name in ("sample_A", "sample_B"):
            sample = proj._sample_mocks[sample_name]
            assert sample.idem.call_count == 1, (
                f"{sample_name}.idem called "
                f"{sample.idem.call_count}x; expected 1 (per-sample dedup)"
            )
            folder = sample.result_folder("short_read::paired_end")
            assert folder.idem.call_count == 1, (
                f"{sample_name}.folder.idem called "
                f"{folder.idem.call_count}x; expected 1"
            )

    def test_idempotent_second_commit_re_idems(self, runner):
        """Re-running --commit calls idem() again — server-side a no-op."""
        # First run.
        proj = _make_proj_mock()
        _invoke(
            runner,
            ["reads", "--commit", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
            proj=proj,
        )
        sample_a_idem_after_first = proj._sample_mocks["sample_A"].idem.call_count

        # Same project mock, same input — second invocation does the same calls.
        _invoke(
            runner,
            ["reads", "--commit", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
            proj=proj,
        )
        sample_a_idem_after_second = proj._sample_mocks["sample_A"].idem.call_count

        # Each commit run does exactly one idem per sample, regardless
        # of whether the sample already existed -- idempotence comes
        # from idem() being server-side get-or-create, not from the
        # client suppressing the call.
        assert sample_a_idem_after_second == sample_a_idem_after_first + 1


# ---------------------------------------------------------------------------
# Privacy & arg validation
# ---------------------------------------------------------------------------


class TestPrivacyAndValidation:
    """Privacy default and CLI plumbing checks."""

    def test_privacy_defaults_to_private(self, runner):
        """``--privacy-level`` defaults to ``private`` (never public)."""
        with patch("geoseeq.cli.link.link_reads.handle_project_id") as mock_handle:
            mock_handle.return_value = _make_proj_mock()
            with patch("geoseeq.cli.link.link_reads.S3Source.from_uri") as mock_src, \
                 patch("geoseeq.cli.shared_params.common_state.Knex",
                       return_value=_make_knex()):
                fake_src = MagicMock()
                fake_src.list_keys.return_value = iter(_KEYS)
                fake_src.endpoint_url = _ENDPOINT
                fake_src.to_s3_uri.side_effect = lambda k: f"s3://b/{k}"
                mock_src.return_value = fake_src
                runner.invoke(
                    cli_link,
                    ["reads", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
                    catch_exceptions=False,
                )
            # handle_project_id was called with private=True
            assert mock_handle.call_args.kwargs.get("private") is True

    def test_help_lists_reads_subcommand(self, runner):
        """``geoseeq link --help`` mentions the reads subcommand."""
        result = runner.invoke(cli_link, ["--help"])
        assert result.exit_code == 0
        assert "reads" in result.output

    def test_reads_help_lists_required_options(self, runner):
        """``geoseeq link reads --help`` documents the key options."""
        result = runner.invoke(cli_link, ["reads", "--help"])
        assert result.exit_code == 0
        for opt in ["--endpoint-url", "--module-name", "--regex",
                    "--filter", "--privacy-level", "--dry-run", "--commit"]:
            assert opt in result.output


# ---------------------------------------------------------------------------
# Server interaction
# ---------------------------------------------------------------------------


class TestServerInteraction:
    """Validation/grouping calls match upload-reads' shape exactly."""

    def test_validate_called_with_seq_type_only(self, runner):
        """``validate_filenames`` gets ``sequence_type`` (not the full module)."""
        result, knex, _ = _invoke(
            runner,
            ["reads", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
        )
        assert result.exit_code == 0
        validate_calls = [c for c in knex.post.call_args_list
                          if c.args[0] == "bulk_upload/validate_filenames"]
        assert len(validate_calls) == 1
        payload = validate_calls[0].kwargs["json"]
        assert payload["sequence_type"] == "paired_end"
        assert payload["sample_group_id"] == _PROJECT_UUID

    def test_group_files_called_after_validate(self, runner):
        """``group_files`` is called after ``validate_filenames`` with the picked regex."""
        result, knex, _ = _invoke(
            runner,
            ["reads", "MyOrg/MyProject", f"s3://{_BUCKET}/{_PREFIX}"],
        )
        assert result.exit_code == 0
        endpoints = [c.args[0] for c in knex.post.call_args_list]
        assert endpoints == [
            "bulk_upload/validate_filenames",
            "bulk_upload/group_files",
        ]
