"""Unit tests for ``geoseeq s3 upload-and-register``."""

import os
import tempfile
from unittest.mock import MagicMock, call, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.s3 import cli_s3
from geoseeq.knex import GeoseeqGeneralError, GeoseeqNotFoundError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PROJECT_UUID = "proj-uuid-abcd-1234"
_BUCKET = "geoseeq-staging"
_STAGING_PREFIX = "staging/user-uuid-9999/"

_BASE_ARGS = [
    "upload-and-register",
    "--sample", "my-sample",
    "--module", "kraken2",
    "--field", "report",
]

_CREDS_RESPONSE = {
    "project_name": "my-project",
    "bucket_name": _BUCKET,
    "staging_prefix": _STAGING_PREFIX,
    "endpoint_url": "https://s3.wasabisys.com",
    "access_key_id": "AKIAIOSFODNN7EXAMPLE",
    "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "region": "us-east-1",
    "inaccessible_sample_count": 0,
    "warnings": [],
}

_REGISTER_RESPONSE = {
    "uuid": "file-uuid-5678",
    "sample_name": "my-sample",
    "module_name": "kraken2",
    "field_name": "report",
    "s3_uri": f"s3://{_BUCKET}/{_STAGING_PREFIX}reads.fastq.gz",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def runner():
    """Click test runner with separated stdout/stderr."""
    return CliRunner(mix_stderr=False)


@pytest.fixture
def local_file(tmp_path):
    """Create a small temporary file to represent the upload source."""
    f = tmp_path / "reads.fastq.gz"
    f.write_bytes(b"fake fastq content")
    return str(f)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_knex(creds=None, creds_exc=None, register_return=None, register_exc=None):
    """Build a mock knex whose post() dispatches by URL suffix."""
    mock_knex = MagicMock()
    mock_knex.set_auth_required.return_value = mock_knex

    def _post_dispatch(url, **kwargs):
        if url.endswith("staging_credentials"):
            if creds_exc is not None:
                raise creds_exc
            return creds if creds is not None else _CREDS_RESPONSE
        if url.endswith("register_staged_file"):
            if register_exc is not None:
                raise register_exc
            return register_return if register_return is not None else _REGISTER_RESPONSE
        raise AssertionError(f"Unexpected POST to {url}")

    mock_knex.post.side_effect = _post_dispatch
    return mock_knex


def _invoke(runner, local_file, extra_args=None, *, mock_knex, project_exc=None):
    """Invoke ``s3 upload-and-register`` with all external calls mocked.

    Returns (result, mock_s3_client, mock_upload, mock_s3_build).
    """
    extra_args = extra_args or []
    mock_proj = MagicMock()
    mock_proj.uuid = _PROJECT_UUID

    mock_s3_client = MagicMock()
    mock_s3_build = MagicMock(return_value=mock_s3_client)

    project_side_effect = project_exc if project_exc is not None else None

    with patch("geoseeq.cli.s3.handle_project_id",
               return_value=mock_proj, side_effect=project_side_effect), \
         patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex), \
         patch("geoseeq.cli.s3._build_s3_client", mock_s3_build), \
         patch("geoseeq.cli.s3._upload_file") as mock_upload:
        result = runner.invoke(
            cli_s3,
            _BASE_ARGS + extra_args + ["My Org/My Project", local_file],
            catch_exceptions=False,
        )
    return result, mock_s3_client, mock_upload, mock_s3_build


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestUploadAndRegisterHappyPath:
    """Success-path behaviour for ``geoseeq s3 upload-and-register``."""

    def test_exit_code_zero_on_success(self, runner, local_file):
        """Exit code is 0 when both upload and registration succeed."""
        mock_knex = _make_knex()
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)
        assert result.exit_code == 0

    def test_upload_file_called_with_correct_args(self, runner, local_file):
        """boto3 upload_file receives the local path, bucket, and staged key."""
        mock_knex = _make_knex()
        result, _client, mock_upload, mock_build = _invoke(
            runner, local_file, mock_knex=mock_knex
        )
        assert result.exit_code == 0

        # _upload_file(s3_client, local_path, bucket, key)
        mock_upload.assert_called_once()
        _s3, path_arg, bucket_arg, key_arg = mock_upload.call_args[0]
        assert path_arg == local_file
        assert bucket_arg == _BUCKET
        assert key_arg == _STAGING_PREFIX + "reads.fastq.gz"

    def test_staged_key_uses_staging_prefix_and_basename(self, runner, local_file):
        """The S3 key is ``{staging_prefix}{basename(local_file)}``."""
        mock_knex = _make_knex()
        _result, _client, mock_upload, _build = _invoke(
            runner, local_file, mock_knex=mock_knex
        )
        _, _, _, key_arg = mock_upload.call_args[0]
        expected_key = _STAGING_PREFIX + os.path.basename(local_file)
        assert key_arg == expected_key

    def test_register_endpoint_called_with_correct_payload(self, runner, local_file):
        """POST to register_staged_file contains all required fields."""
        mock_knex = _make_knex()
        _invoke(runner, local_file, mock_knex=mock_knex)

        register_call = None
        for c in mock_knex.post.call_args_list:
            if "register_staged_file" in c[0][0]:
                register_call = c
                break
        assert register_call is not None, "register_staged_file endpoint was not called"

        payload = register_call[1]["json"]
        assert payload["staged_path"] == _STAGING_PREFIX + "reads.fastq.gz"
        assert payload["sample_name"] == "my-sample"
        assert payload["module_name"] == "kraken2"
        assert payload["field_name"] == "report"
        assert payload["replicate"] == 1

    def test_replicate_option_forwarded_to_payload(self, runner, local_file):
        """``--replicate 3`` is included in the register payload."""
        mock_knex = _make_knex()
        _invoke(runner, local_file, extra_args=["--replicate", "3"], mock_knex=mock_knex)

        for c in mock_knex.post.call_args_list:
            if "register_staged_file" in c[0][0]:
                assert c[1]["json"]["replicate"] == 3
                return
        pytest.fail("register_staged_file was not called")

    def test_registration_summary_printed_on_success(self, runner, local_file):
        """The registration summary is written to stdout on success."""
        mock_knex = _make_knex()
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)

        assert "Registered:" in result.output
        assert "my-sample" in result.output
        assert "kraken2" in result.output
        assert "report" in result.output

    def test_progress_messages_on_stdout(self, runner, local_file):
        """Upload progress messages appear on stdout."""
        mock_knex = _make_knex()
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)

        assert "Uploading" in result.output
        assert "Upload complete." in result.output

    def test_upload_message_contains_filename(self, runner, local_file):
        """The uploading message mentions the file's basename."""
        mock_knex = _make_knex()
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)

        assert os.path.basename(local_file) in result.output

    def test_s3_client_built_with_creds(self, runner, local_file):
        """_build_s3_client is called with the credentials dict from the API."""
        mock_knex = _make_knex()
        mock_proj = MagicMock()
        mock_proj.uuid = _PROJECT_UUID
        mock_s3_client = MagicMock()
        mock_build = MagicMock(return_value=mock_s3_client)

        with patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj), \
             patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex), \
             patch("geoseeq.cli.s3._build_s3_client", mock_build), \
             patch("geoseeq.cli.s3._upload_file"):
            runner.invoke(
                cli_s3,
                _BASE_ARGS + ["My Org/My Project", local_file],
                catch_exceptions=False,
            )

        mock_build.assert_called_once_with(_CREDS_RESPONSE)


# ---------------------------------------------------------------------------
# Upload success + registration failure
# ---------------------------------------------------------------------------

class TestUploadSuccessRegistrationFailure:
    """When upload succeeds but registration fails the staged key is recoverable."""

    def test_nonzero_exit_on_registration_error(self, runner, local_file):
        """Exit code is non-zero when registration fails after a successful upload."""
        mock_knex = _make_knex(
            register_exc=GeoseeqGeneralError("internal server error")
        )
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)
        assert result.exit_code != 0

    def test_staged_key_printed_to_stdout_on_registration_failure(self, runner, local_file):
        """The staged S3 key is printed to stdout so the user can retry registration."""
        mock_knex = _make_knex(
            register_exc=GeoseeqGeneralError("registration failed")
        )
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)

        expected_key = _STAGING_PREFIX + os.path.basename(local_file)
        assert expected_key in result.output

    def test_registration_error_message_on_stderr(self, runner, local_file):
        """The registration error message appears on stderr."""
        mock_knex = _make_knex(
            register_exc=GeoseeqGeneralError("something went wrong")
        )
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)

        assert "Error registering staged file" in result.stderr

    def test_upload_still_completes_before_registration_failure(self, runner, local_file):
        """The upload progress messages appear even when registration fails."""
        mock_knex = _make_knex(
            register_exc=GeoseeqGeneralError("registration failed")
        )
        result, _client, mock_upload, _build = _invoke(
            runner, local_file, mock_knex=mock_knex
        )

        mock_upload.assert_called_once()
        assert "Upload complete." in result.output


# ---------------------------------------------------------------------------
# Credentials fetch failure
# ---------------------------------------------------------------------------

class TestCredentialsFetchFailure:
    """When the staging credentials call fails the command exits cleanly."""

    def test_nonzero_exit_on_credentials_error(self, runner, local_file):
        """Exit code is non-zero when the credentials endpoint returns an error."""
        mock_knex = _make_knex(creds_exc=GeoseeqGeneralError("forbidden"))
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)
        assert result.exit_code != 0

    def test_error_message_on_stderr_for_credentials_failure(self, runner, local_file):
        """The credentials error message is written to stderr."""
        mock_knex = _make_knex(creds_exc=GeoseeqGeneralError("forbidden"))
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)

        assert "Error fetching staging credentials" in result.stderr

    def test_no_upload_attempted_after_credentials_failure(self, runner, local_file):
        """upload_file is not called when credentials fetch fails."""
        mock_knex = _make_knex(creds_exc=GeoseeqGeneralError("forbidden"))
        _result, _client, mock_upload, _build = _invoke(
            runner, local_file, mock_knex=mock_knex
        )
        mock_upload.assert_not_called()

    def test_clean_stdout_on_credentials_failure(self, runner, local_file):
        """stdout is empty when credentials fetch fails."""
        mock_knex = _make_knex(creds_exc=GeoseeqGeneralError("forbidden"))
        result, *_ = _invoke(runner, local_file, mock_knex=mock_knex)
        assert result.output.strip() == ""


# ---------------------------------------------------------------------------
# Upload failure
# ---------------------------------------------------------------------------

class TestUploadFailure:
    """When the boto3 upload raises an exception the command exits cleanly."""

    def test_nonzero_exit_on_upload_error(self, runner, local_file):
        """Exit code is non-zero when the upload raises an exception."""
        mock_knex = _make_knex()
        mock_proj = MagicMock()
        mock_proj.uuid = _PROJECT_UUID

        with patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj), \
             patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex), \
             patch("geoseeq.cli.s3._build_s3_client", return_value=MagicMock()), \
             patch("geoseeq.cli.s3._upload_file", side_effect=Exception("connection reset")):
            result = runner.invoke(
                cli_s3,
                _BASE_ARGS + ["My Org/My Project", local_file],
                catch_exceptions=False,
            )
        assert result.exit_code != 0

    def test_error_message_on_stderr_for_upload_failure(self, runner, local_file):
        """The upload error message is written to stderr."""
        mock_knex = _make_knex()
        mock_proj = MagicMock()
        mock_proj.uuid = _PROJECT_UUID

        with patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj), \
             patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex), \
             patch("geoseeq.cli.s3._build_s3_client", return_value=MagicMock()), \
             patch("geoseeq.cli.s3._upload_file", side_effect=Exception("connection reset")):
            result = runner.invoke(
                cli_s3,
                _BASE_ARGS + ["My Org/My Project", local_file],
                catch_exceptions=False,
            )
        assert "Error uploading file" in result.stderr

    def test_register_not_called_on_upload_failure(self, runner, local_file):
        """register_staged_file is NOT called when the upload fails."""
        mock_knex = _make_knex()
        mock_proj = MagicMock()
        mock_proj.uuid = _PROJECT_UUID

        with patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj), \
             patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex), \
             patch("geoseeq.cli.s3._build_s3_client", return_value=MagicMock()), \
             patch("geoseeq.cli.s3._upload_file", side_effect=Exception("connection reset")):
            runner.invoke(
                cli_s3,
                _BASE_ARGS + ["My Org/My Project", local_file],
                catch_exceptions=False,
            )

        for c in mock_knex.post.call_args_list:
            assert "register_staged_file" not in c[0][0], (
                "register_staged_file was called despite upload failure"
            )

    def test_nonzero_exit_on_oserror(self, runner, local_file):
        """Exit code is non-zero when the upload raises an OSError."""
        mock_knex = _make_knex()
        mock_proj = MagicMock()
        mock_proj.uuid = _PROJECT_UUID

        with patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj), \
             patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex), \
             patch("geoseeq.cli.s3._build_s3_client", return_value=MagicMock()), \
             patch("geoseeq.cli.s3._upload_file", side_effect=OSError("disk full")):
            result = runner.invoke(
                cli_s3,
                _BASE_ARGS + ["My Org/My Project", local_file],
                catch_exceptions=False,
            )
        assert result.exit_code != 0
        assert "Error uploading file" in result.stderr


# ---------------------------------------------------------------------------
# Project lookup failure
# ---------------------------------------------------------------------------

class TestProjectLookupFailure:
    """When project lookup fails the command exits with a meaningful error."""

    def test_nonzero_exit_on_project_not_found(self, runner, local_file):
        """Exit code is non-zero when the project is not found."""
        mock_knex = _make_knex()
        result, *_ = _invoke(
            runner, local_file, mock_knex=mock_knex,
            project_exc=GeoseeqNotFoundError("project not found"),
        )
        assert result.exit_code != 0

    def test_error_message_on_stderr_for_project_not_found(self, runner, local_file):
        """The project lookup error is written to stderr."""
        mock_knex = _make_knex()
        result, *_ = _invoke(
            runner, local_file, mock_knex=mock_knex,
            project_exc=GeoseeqNotFoundError("project not found"),
        )
        assert "Error looking up project" in result.stderr

    def test_no_upload_attempted_after_project_failure(self, runner, local_file):
        """upload_file is not called when project lookup fails."""
        mock_knex = _make_knex()
        _result, _client, mock_upload, _build = _invoke(
            runner, local_file, mock_knex=mock_knex,
            project_exc=GeoseeqNotFoundError("project not found"),
        )
        mock_upload.assert_not_called()


# ---------------------------------------------------------------------------
# Missing required options
# ---------------------------------------------------------------------------

class TestMissingRequiredOptions:
    """Omitting required options should produce a usage error."""

    def _run_without(self, runner, local_file, omit_option):
        """Invoke with one required option (and its value) removed."""
        mock_knex = _make_knex()
        mock_proj = MagicMock()
        mock_proj.uuid = _PROJECT_UUID

        args = []
        i = 0
        while i < len(_BASE_ARGS):
            if _BASE_ARGS[i] == omit_option:
                i += 2  # skip the flag and its following value
            else:
                args.append(_BASE_ARGS[i])
                i += 1

        with patch("geoseeq.cli.s3.handle_project_id", return_value=mock_proj), \
             patch("geoseeq.cli.shared_params.common_state.Knex", return_value=mock_knex), \
             patch("geoseeq.cli.s3._build_s3_client", return_value=MagicMock()), \
             patch("geoseeq.cli.s3._upload_file"):
            result = runner.invoke(
                cli_s3,
                args + ["My Org/My Project", local_file],
            )
        return result

    def test_missing_sample_fails(self, runner, local_file):
        """Omitting ``--sample`` produces a usage error."""
        result = self._run_without(runner, local_file, "--sample")
        assert result.exit_code != 0

    def test_missing_module_fails(self, runner, local_file):
        """Omitting ``--module`` produces a usage error."""
        result = self._run_without(runner, local_file, "--module")
        assert result.exit_code != 0

    def test_missing_field_fails(self, runner, local_file):
        """Omitting ``--field`` produces a usage error."""
        result = self._run_without(runner, local_file, "--field")
        assert result.exit_code != 0
