"""Unit tests for ``geoseeq s3 register``."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.s3 import _normalize_staged_path, cli_s3
from geoseeq.knex import GeoseeqGeneralError, GeoseeqNotFoundError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROJECT_UUID = "proj-uuid-abcd-1234"

_BASE_ARGS = [
    "register",
    "--sample", "my-sample",
    "--module", "kraken2",
    "--field", "report",
]

_SUCCESS_RESPONSE = {
    "uuid": "file-uuid-5678",
    "sample_name": "my-sample",
    "module_name": "kraken2",
    "field_name": "report",
    "s3_uri": "s3://geoseeq-v0-abc/staging/abc-uuid/foo.fastq.gz",
}


def _make_mock_knex(post_return=None, post_exc=None):
    """Return a mock knex whose post() either returns a value or raises."""
    mock_knex = MagicMock()
    mock_knex.set_auth_required.return_value = mock_knex
    if post_exc is not None:
        mock_knex.post.side_effect = post_exc
    else:
        mock_knex.post.return_value = post_return
    return mock_knex


def _invoke_register(runner, project_id, staged_path, extra_args=None,
                     post_return=None, post_exc=None, project_exc=None):
    """Invoke ``s3 register`` with mocked knex and project lookup.

    Returns (result, mock_knex).
    """
    extra_args = extra_args or []
    mock_proj = MagicMock()
    mock_proj.uuid = _PROJECT_UUID

    mock_knex = _make_mock_knex(post_return=post_return, post_exc=post_exc)

    project_side_effect = project_exc if project_exc is not None else None

    with patch("geoseeq.cli.s3.handle_project_id",
               return_value=mock_proj, side_effect=project_side_effect), \
         patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex):
        result = runner.invoke(
            cli_s3,
            _BASE_ARGS + extra_args + [project_id, staged_path],
            catch_exceptions=False,
        )
    return result, mock_knex


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def runner():
    """Click test runner with separated stdout/stderr."""
    return CliRunner(mix_stderr=False)


# ---------------------------------------------------------------------------
# Unit tests for _normalize_staged_path
# ---------------------------------------------------------------------------

class TestNormalizeStagedPath:
    """Tests for the pure path-normalization helper."""

    def test_bare_key_is_unchanged(self):
        """A key without a scheme prefix is returned as-is."""
        key = "staging/abc-uuid/foo.fastq.gz"
        assert _normalize_staged_path(key) == key

    def test_full_uri_is_stripped_to_key(self):
        """An s3:// URI has the scheme and bucket stripped."""
        uri = "s3://geoseeq-v0-abc123/staging/abc-uuid/foo.fastq.gz"
        assert _normalize_staged_path(uri) == "staging/abc-uuid/foo.fastq.gz"

    def test_full_uri_with_uuid_bucket(self):
        """Works with bucket names that contain UUIDs."""
        uri = "s3://geoseeq-v0-00000000-0000-0000-0000-000000000000/staging/x/y.bam"
        assert _normalize_staged_path(uri) == "staging/x/y.bam"

    def test_bare_key_at_root(self):
        """A bare key with no subdirectory is returned unchanged."""
        key = "myfile.fastq.gz"
        assert _normalize_staged_path(key) == key


# ---------------------------------------------------------------------------
# CLI integration tests (mocked HTTP)
# ---------------------------------------------------------------------------

class TestS3RegisterHappyPath:
    """Success-path behaviour tests for ``geoseeq s3 register``."""

    def test_success_bare_key_prints_summary(self, runner):
        """A bare key invocation prints all summary fields to stdout."""
        result, _ = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            post_return=_SUCCESS_RESPONSE,
        )

        assert result.exit_code == 0
        assert "Registered: staging/abc-uuid/foo.fastq.gz" in result.output
        assert "Sample:   my-sample" in result.output
        assert "Module:   kraken2" in result.output
        assert "Field:    report" in result.output
        assert "UUID:     file-uuid-5678" in result.output
        assert "URI:      s3://geoseeq-v0-abc/staging/abc-uuid/foo.fastq.gz" in result.output

    def test_success_full_uri_is_normalized(self, runner):
        """A full S3 URI is stripped to a bare key before the API call."""
        full_uri = "s3://geoseeq-v0-abc/staging/abc-uuid/foo.fastq.gz"
        result, mock_knex = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path=full_uri,
            post_return=_SUCCESS_RESPONSE,
        )

        assert result.exit_code == 0
        # The summary line uses the bare key, not the full URI
        assert "Registered: staging/abc-uuid/foo.fastq.gz" in result.output

        # The payload sent to the API contains the bare key
        call_kwargs = mock_knex.post.call_args
        payload = call_kwargs[1]["json"]
        assert payload["staged_path"] == "staging/abc-uuid/foo.fastq.gz"

    def test_file_size_included_when_provided(self, runner):
        """``--file-size`` is included in the API payload when given."""
        _, mock_knex = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            extra_args=["--file-size", "10240"],
            post_return=_SUCCESS_RESPONSE,
        )

        payload = mock_knex.post.call_args[1]["json"]
        assert payload["file_size"] == 10240

    def test_file_size_omitted_when_not_provided(self, runner):
        """``file_size`` is absent from the payload when ``--file-size`` is not given."""
        _, mock_knex = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            post_return=_SUCCESS_RESPONSE,
        )

        payload = mock_knex.post.call_args[1]["json"]
        assert "file_size" not in payload

    def test_replicate_defaults_to_one(self, runner):
        """``replicate`` defaults to 1 when ``--replicate`` is not given."""
        _, mock_knex = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            post_return=_SUCCESS_RESPONSE,
        )

        payload = mock_knex.post.call_args[1]["json"]
        assert payload["replicate"] == 1

    def test_replicate_custom_value(self, runner):
        """``--replicate 2`` is forwarded in the payload."""
        _, mock_knex = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            extra_args=["--replicate", "2"],
            post_return=_SUCCESS_RESPONSE,
        )

        payload = mock_knex.post.call_args[1]["json"]
        assert payload["replicate"] == 2

    def test_correct_endpoint_called(self, runner):
        """POST targets ``sample_groups/<uuid>/register_staged_file``."""
        _, mock_knex = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            post_return=_SUCCESS_RESPONSE,
        )

        mock_knex.post.assert_called_once()
        url_arg = mock_knex.post.call_args[0][0]
        assert url_arg == f"sample_groups/{_PROJECT_UUID}/register_staged_file"

    def test_payload_contains_all_required_fields(self, runner):
        """The POST payload includes staged_path, sample_name, module_name, field_name, replicate."""
        _, mock_knex = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            post_return=_SUCCESS_RESPONSE,
        )

        payload = mock_knex.post.call_args[1]["json"]
        assert payload["staged_path"] == "staging/abc-uuid/foo.fastq.gz"
        assert payload["sample_name"] == "my-sample"
        assert payload["module_name"] == "kraken2"
        assert payload["field_name"] == "report"
        assert "replicate" in payload


class TestS3RegisterErrorHandling:
    """Error and validation tests for ``geoseeq s3 register``."""

    def test_api_error_exits_nonzero(self, runner):
        """A GeoseeqGeneralError causes a non-zero exit code."""
        result, _ = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            post_exc=GeoseeqGeneralError("internal server error"),
        )

        assert result.exit_code != 0

    def test_api_error_message_on_stderr(self, runner):
        """The API error message is written to stderr."""
        result, _ = _invoke_register(
            runner,
            project_id="My Org/My Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            post_exc=GeoseeqGeneralError("something went wrong"),
        )

        assert "Error registering staged file" in result.stderr
        assert result.output.strip() == ""

    def test_project_not_found_exits_nonzero(self, runner):
        """A project-lookup failure causes a non-zero exit code."""
        result, _ = _invoke_register(
            runner,
            project_id="My Org/Missing Project",
            staged_path="staging/abc-uuid/foo.fastq.gz",
            project_exc=GeoseeqNotFoundError("project not found"),
        )

        assert result.exit_code != 0
        assert "Error looking up project" in result.stderr
        assert result.output.strip() == ""

    def test_missing_sample_option_fails(self, runner):
        """Omitting ``--sample`` produces a usage error."""
        result = runner.invoke(
            cli_s3,
            [
                "register",
                "--module", "kraken2",
                "--field", "report",
                "My Org/My Project",
                "staging/abc-uuid/foo.fastq.gz",
            ],
        )
        assert result.exit_code != 0

    def test_missing_module_option_fails(self, runner):
        """Omitting ``--module`` produces a usage error."""
        result = runner.invoke(
            cli_s3,
            [
                "register",
                "--sample", "my-sample",
                "--field", "report",
                "My Org/My Project",
                "staging/abc-uuid/foo.fastq.gz",
            ],
        )
        assert result.exit_code != 0

    def test_missing_field_option_fails(self, runner):
        """Omitting ``--field`` produces a usage error."""
        result = runner.invoke(
            cli_s3,
            [
                "register",
                "--sample", "my-sample",
                "--module", "kraken2",
                "My Org/My Project",
                "staging/abc-uuid/foo.fastq.gz",
            ],
        )
        assert result.exit_code != 0
