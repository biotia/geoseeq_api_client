"""Unit tests for the `geoseeq repo log` CLI command."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.main import main


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_RESPONSE = {
    "count": 2,
    "results": [
        {
            "sha1": "abc123def456abc1",
            "message": "api: upload file",
            "author": "GeoSeeq API",
            "timestamp": "2026-05-20T14:32:00Z",
            "parent_sha1": "def456",
        },
        {
            "sha1": "def456abc789def2",
            "message": "api: create sample S1",
            "author": "GeoSeeq API",
            "timestamp": "2026-05-20T14:30:15Z",
            "parent_sha1": None,
        },
    ],
}

EMPTY_API_RESPONSE = {"count": 0, "results": []}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_mock_repo(tmp_path: Path) -> Path:
    """Create a minimal .geoseeq/config.json in tmp_path and return it."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()
    config = {
        "project_uuid": "test-uuid-1234",
        "server_url": "https://test.geoseeq.com",
        "auth_profile": "",
        "git_remote_url": "https://test.geoseeq.com/api/v1/projects/test-uuid-1234/git",
    }
    (geoseeq_dir / "config.json").write_text(json.dumps(config))
    return tmp_path


def _mock_http_response(data: dict) -> MagicMock:
    """Return a mock requests.Response with json() returning *data*."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


def _invoke_log(runner: CliRunner, extra_args: list[str], cwd: Path | None = None) -> object:
    """Invoke `geoseeq repo log` with optional extra args in the given working dir."""
    args = ["repo", "log"] + extra_args
    env = {"GEOSEEQ_API_TOKEN": "fake-token"}
    if cwd is not None:
        args = args + [str(cwd)]
    return runner.invoke(main, args, env=env, catch_exceptions=False)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_log_normal_output(tmp_path):
    """Normal output shows 10-char SHA, formatted date, and message per line."""
    make_mock_repo(tmp_path)
    runner = CliRunner()

    mock_resp = _mock_http_response(API_RESPONSE)
    mock_repo = MagicMock()
    mock_repo.config.project_uuid = "test-uuid-1234"
    mock_repo.config.server_url = "https://test.geoseeq.com"

    with patch("geoseeq.cli.repo.GeoSeeqRepo.find", return_value=mock_repo), patch(
        "requests.Session.get", return_value=mock_resp
    ):
        result = runner.invoke(
            main,
            ["repo", "log", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake-token"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) == 2

    # First entry
    assert lines[0].startswith("abc123def4")
    assert "2026-05-20 14:32:00" in lines[0]
    assert "api: upload file" in lines[0]

    # Second entry
    assert lines[1].startswith("def456abc7")
    assert "2026-05-20 14:30:15" in lines[1]
    assert "api: create sample S1" in lines[1]


def test_log_json_flag(tmp_path):
    """--json flag emits pretty-printed JSON of the API response."""
    make_mock_repo(tmp_path)
    runner = CliRunner()

    mock_resp = _mock_http_response(API_RESPONSE)
    mock_repo = MagicMock()
    mock_repo.config.project_uuid = "test-uuid-1234"
    mock_repo.config.server_url = "https://test.geoseeq.com"

    with patch("geoseeq.cli.repo.GeoSeeqRepo.find", return_value=mock_repo), patch(
        "requests.Session.get", return_value=mock_resp
    ):
        result = runner.invoke(
            main,
            ["repo", "log", "--json", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake-token"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["count"] == 2
    assert len(parsed["results"]) == 2
    assert parsed["results"][0]["sha1"] == "abc123def456abc1"


def test_log_empty_history(tmp_path):
    """When the server returns no history, a friendly message is shown."""
    make_mock_repo(tmp_path)
    runner = CliRunner()

    mock_resp = _mock_http_response(EMPTY_API_RESPONSE)
    mock_repo = MagicMock()
    mock_repo.config.project_uuid = "test-uuid-1234"
    mock_repo.config.server_url = "https://test.geoseeq.com"

    with patch("geoseeq.cli.repo.GeoSeeqRepo.find", return_value=mock_repo), patch(
        "requests.Session.get", return_value=mock_resp
    ):
        result = runner.invoke(
            main,
            ["repo", "log", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake-token"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert "No manifest history yet." in result.output


def test_log_not_in_repo(tmp_path):
    """Running log outside a geoseeq repo prints a clear error message."""
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["repo", "log", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake-token"},
    )

    assert result.exit_code != 0
    assert "not inside a GeoSeeq project directory" in result.output


def test_log_pagination_params(tmp_path):
    """--limit and --offset are forwarded to the API URL."""
    make_mock_repo(tmp_path)
    runner = CliRunner()

    mock_resp = _mock_http_response({"count": 0, "results": []})
    mock_repo = MagicMock()
    mock_repo.config.project_uuid = "test-uuid-1234"
    mock_repo.config.server_url = "https://test.geoseeq.com"

    captured_urls = []

    def fake_get(url, **kwargs):
        captured_urls.append(url)
        return mock_resp

    with patch("geoseeq.cli.repo.GeoSeeqRepo.find", return_value=mock_repo), patch(
        "requests.Session.get", side_effect=fake_get
    ):
        result = runner.invoke(
            main,
            ["repo", "log", "--limit", "5", "--offset", "10", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake-token"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert len(captured_urls) == 1
    assert "limit=5" in captured_urls[0]
    assert "offset=10" in captured_urls[0]
