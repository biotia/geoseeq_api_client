"""Unit tests for the `geoseeq s3` CLI command group."""

import inspect
import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli import main
from geoseeq.knex import (
    GeoseeqForbiddenError,
    GeoseeqNotFoundError,
    GeoseeqOtherError,
)

# Click 8.2 removed the mix_stderr parameter (stderr is always separated).
_RUNNER_KWARGS = (
    {"mix_stderr": False}
    if "mix_stderr" in inspect.signature(CliRunner.__init__).parameters
    else {}
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def runner():
    """Return a Click test runner with separated stderr."""
    return CliRunner(**_RUNNER_KWARGS)


def _fake_creds(**overrides):
    """Return a minimal staging-credentials response dict."""
    base = {
        "project_name": "my-project",
        "bucket_name": "geoseeq-staging",
        "staging_prefix": "staging/user-uuid-1234/",
        "endpoint_url": "https://s3.wasabisys.com",
        "access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "region": "us-east-1",
        "inaccessible_sample_count": 0,
        "warnings": [],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_project_and_knex(creds_response, project_uuid="proj-uuid-1234"):
    """Return a context-manager stack that mocks handle_project_id and knex.post."""
    mock_proj = MagicMock()
    mock_proj.uuid = project_uuid

    mock_knex = MagicMock()
    mock_knex.set_auth_required.return_value = mock_knex
    mock_knex.post.return_value = creds_response

    return mock_proj, mock_knex


# ---------------------------------------------------------------------------
# Tests: output formats
# ---------------------------------------------------------------------------

class TestS3CredentialsOutputFormats:
    """Verify each --output-format produces the expected text on stdout."""

    def _run(self, runner, fmt, creds):
        mock_proj, mock_knex = _patch_project_and_knex(creds)
        with (
            patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "--output-format", fmt, "My Org/my-project"],
                catch_exceptions=False,
            )
        return result

    def test_env_format_default(self, runner):
        """Default format outputs shell export statements."""
        creds = _fake_creds()
        result = self._run(runner, "env", creds)
        assert result.exit_code == 0
        out = result.output
        assert "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE" in out
        assert "export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" in out
        assert "export AWS_DEFAULT_REGION=us-east-1" in out
        assert "# Project: my-project" in out
        assert "# Bucket: geoseeq-staging" in out
        assert "--endpoint-url https://s3.wasabisys.com" in out

    def test_json_format(self, runner):
        """JSON format outputs parseable raw JSON."""
        creds = _fake_creds()
        result = self._run(runner, "json", creds)
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["access_key_id"] == creds["access_key_id"]
        assert parsed["bucket_name"] == creds["bucket_name"]

    def test_ini_format(self, runner):
        """INI format outputs an AWS credentials file block."""
        creds = _fake_creds()
        result = self._run(runner, "ini", creds)
        assert result.exit_code == 0
        out = result.output
        assert "[geoseeq-my-project]" in out
        assert "aws_access_key_id = AKIAIOSFODNN7EXAMPLE" in out
        assert "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" in out
        assert "region = us-east-1" in out

    def test_env_format_is_default(self, runner):
        """Omitting --output-format defaults to env."""
        creds = _fake_creds()
        mock_proj, mock_knex = _patch_project_and_knex(creds)
        with (
            patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "My Org/my-project"],
                catch_exceptions=False,
            )
        assert result.exit_code == 0
        assert "export AWS_ACCESS_KEY_ID=" in result.output


# ---------------------------------------------------------------------------
# Tests: warnings
# ---------------------------------------------------------------------------

class TestS3CredentialsWarnings:
    """Verify that warnings go to stderr and do not pollute stdout."""

    def test_warnings_go_to_stderr(self, runner):
        """Warnings from inaccessible samples appear on stderr only."""
        creds = _fake_creds(
            inaccessible_sample_count=2,
            warnings=["Sample A is not accessible.", "Sample B is not accessible."],
        )
        mock_proj, mock_knex = _patch_project_and_knex(creds)
        with (
            patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "--output-format", "env", "My Org/my-project"],
                catch_exceptions=False,
            )
        assert result.exit_code == 0
        # Warnings must appear on stderr
        assert "Sample A is not accessible." in result.stderr
        assert "Sample B is not accessible." in result.stderr
        # Warnings must NOT pollute stdout (the output used for shell capture)
        assert "Sample A is not accessible." not in result.output
        assert "Sample B is not accessible." not in result.output

    def test_no_warnings_when_all_accessible(self, runner):
        """No warning output when inaccessible_sample_count is 0."""
        creds = _fake_creds()
        mock_proj, mock_knex = _patch_project_and_knex(creds)
        with (
            patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "My Org/my-project"],
                catch_exceptions=False,
            )
        assert result.exit_code == 0
        assert result.stderr == ""


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------

class TestS3CredentialsErrors:
    """Verify non-zero exit codes on API errors."""

    def test_nonzero_exit_on_project_not_found(self, runner):
        """Exit code 1 when the project cannot be found."""
        mock_knex = MagicMock()
        mock_knex.set_auth_required.return_value = mock_knex
        with (
            patch(
                "geoseeq.cli.s3.handle_project_id",
                side_effect=GeoseeqNotFoundError("not found"),
            ),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "My Org/missing-project"],
            )
        assert result.exit_code != 0

    def test_nonzero_exit_on_forbidden(self, runner):
        """Exit code 1 when the API returns 403 Forbidden."""
        mock_proj, mock_knex = _patch_project_and_knex({})
        mock_knex.post.side_effect = GeoseeqForbiddenError("forbidden")
        with (
            patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "My Org/my-project"],
            )
        assert result.exit_code != 0

    def test_nonzero_exit_on_other_api_error(self, runner):
        """Exit code 1 when the API returns another HTTP error."""
        mock_proj, mock_knex = _patch_project_and_knex({})
        mock_knex.post.side_effect = GeoseeqOtherError("bad request")
        with (
            patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "My Org/my-project"],
            )
        assert result.exit_code != 0

    def test_error_message_goes_to_stderr(self, runner):
        """API error messages are written to stderr, not stdout."""
        mock_knex = MagicMock()
        mock_knex.set_auth_required.return_value = mock_knex
        with (
            patch(
                "geoseeq.cli.s3.handle_project_id",
                side_effect=GeoseeqNotFoundError("project not found"),
            ),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "My Org/missing-project"],
            )
        assert result.exit_code != 0
        assert "Error" in result.stderr
        # stdout should be empty on error
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# Tests: API call
# ---------------------------------------------------------------------------

class TestS3CredentialsApiCall:
    """Verify the correct endpoint is called."""

    def test_posts_to_staging_credentials_endpoint(self, runner):
        """Command POSTs to sample_groups/<uuid>/staging_credentials."""
        creds = _fake_creds()
        mock_proj, mock_knex = _patch_project_and_knex(creds, project_uuid="abc-123")
        with (
            patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj),
            patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex),
        ):
            result = runner.invoke(
                main,
                ["s3", "credentials", "My Org/my-project"],
                catch_exceptions=False,
            )
        assert result.exit_code == 0
        mock_knex.post.assert_called_once_with("sample_groups/abc-123/staging_credentials")
