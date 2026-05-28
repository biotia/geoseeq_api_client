"""Unit tests for audit-trail-disabled degradation (GRF-15, current architecture).

When a project's ``audit_trail_mode`` is ``off`` the server never commits the
manifest, so the manifest git repo is refless (no ``main``).  These tests verify
the read-only client degrades gracefully instead of erroring:

* ``log`` reports the disabled state from the history-endpoint envelope;
* ``clone`` writes an empty manifest when the refless clone checks out none, and
  the CLI prints a disabled notice;
* ``git_pull`` no-ops when the remote has no ``main`` ref.

The mode is read live where needed (history envelope / empty-clone signal); it
is NOT persisted in ``RepoConfig`` — see GRF-15.md.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from geoseeq.cli.main import main
from geoseeq.repo import GeoSeeqRepo, Manifest, RepoConfig


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _mock_http_response(data: dict) -> MagicMock:
    """Return a mock requests.Response whose json() returns *data*."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


def _mock_repo() -> MagicMock:
    """Return a mock GeoSeeqRepo exposing the config the log command reads."""
    mock_repo = MagicMock()
    mock_repo.config.project_uuid = "test-uuid-1234"
    mock_repo.config.server_url = "https://test.geoseeq.com"
    return mock_repo


def _build_repo(tmp_path: Path) -> GeoSeeqRepo:
    """Create a minimal slim geoseeq repo under *tmp_path* and return its handle."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()
    RepoConfig(
        project_uuid="proj-uuid",
        server_url="https://backend.geoseeq.com",
    ).save(geoseeq_dir / "config.json")
    Manifest(
        version=1,
        project_uuid="proj-uuid",
        project_name="TestOrg/TestProject",
        server_url="https://backend.geoseeq.com",
        samples={},
        project_results={},
    ).save(geoseeq_dir / "manifest.json")
    (tmp_path / "samples").mkdir(exist_ok=True)
    (tmp_path / "project_results").mkdir(exist_ok=True)
    return GeoSeeqRepo(tmp_path)


# ---------------------------------------------------------------------------
# log tests
# ---------------------------------------------------------------------------


def _run_log(tmp_path: Path, response: dict, extra_args=None):
    """Invoke ``geoseeq repo log`` with a mocked repo + history response."""
    runner = CliRunner()
    args = ["repo", "log", *(extra_args or []), str(tmp_path)]
    with (
        patch("geoseeq.cli.repo.GeoSeeqRepo.find", return_value=_mock_repo()),
        patch("requests.Session.get", return_value=_mock_http_response(response)),
    ):
        return runner.invoke(
            main, args, env={"GEOSEEQ_API_TOKEN": "fake-token"},
            catch_exceptions=False,
        )


def test_log_reports_disabled_when_mode_off(tmp_path):
    """log prints the disabled message when the envelope reports mode off."""
    response = {"audit_trail_mode": "off", "count": 0, "results": []}

    result = _run_log(tmp_path, response)

    assert result.exit_code == 0, result.output
    assert "Audit trail is disabled for this project." in result.output
    # Must short-circuit before the empty-history message.
    assert "No manifest history yet." not in result.output


def test_log_normal_when_mode_on(tmp_path):
    """log does NOT print the disabled message when the envelope reports mode on."""
    response = {
        "audit_trail_mode": "on",
        "count": 1,
        "results": [
            {
                "sha1": "abc123def456abc1",
                "message": "api: upload file",
                "timestamp": "2026-05-20T14:32:00Z",
            }
        ],
    }

    result = _run_log(tmp_path, response)

    assert result.exit_code == 0, result.output
    assert "Audit trail is disabled for this project." not in result.output
    assert "api: upload file" in result.output


def test_log_empty_but_enabled_still_reports_no_history(tmp_path):
    """An enabled project with no commits still prints 'No manifest history yet.'."""
    response = {"audit_trail_mode": "on", "count": 0, "results": []}

    result = _run_log(tmp_path, response)

    assert result.exit_code == 0, result.output
    assert "No manifest history yet." in result.output
    assert "Audit trail is disabled for this project." not in result.output


def test_log_falls_through_when_audit_trail_mode_absent(tmp_path):
    """Older servers that omit audit_trail_mode still show normal history (no crash, no disabled msg)."""
    response = {
        "count": 1,
        "results": [
            {
                "sha1": "abc123def456abc1",
                "message": "api: upload file",
                "timestamp": "2026-05-20T14:32:00Z",
            }
        ],
    }

    result = _run_log(tmp_path, response)

    assert result.exit_code == 0, result.output
    assert "Audit trail is disabled for this project." not in result.output
    assert "api: upload file" in result.output


def test_log_json_emits_raw_response_when_disabled(tmp_path):
    """--json prints the raw API response (including audit_trail_mode) regardless of mode."""
    response = {"audit_trail_mode": "off", "count": 0, "results": []}

    result = _run_log(tmp_path, response, extra_args=["--json"])

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["audit_trail_mode"] == "off"
    # The human-readable disabled message must not leak into JSON output.
    assert "Audit trail is disabled for this project." not in result.output


# ---------------------------------------------------------------------------
# clone tests
# ---------------------------------------------------------------------------


def _mock_project(name: str = "TestProject"):
    """Return a mock Project with a uuid and name (Project has no audit_trail_mode)."""
    proj = MagicMock()
    proj.uuid = "proj-uuid"
    proj.name = name
    return proj


def _run_clone(tmp_path: Path, *, refless: bool):
    """Invoke ``geoseeq repo clone`` with a mocked project + git_clone.

    When *refless* is True, the mocked ``git_clone`` creates ``.geoseeq/`` but no
    ``manifest.json`` (modelling an audit-off / empty manifest repo).  Otherwise
    it writes a manifest the way a normal clone would.
    """
    clone_path = tmp_path / "MyProject"

    def _fake_git_clone(remote_url, geoseeq_dir, token, server_url):
        geoseeq_dir.mkdir(parents=True, exist_ok=True)
        if not refless:
            Manifest(
                version=1,
                project_uuid="proj-uuid",
                project_name="TestOrg/TestProject",
                server_url="https://backend.geoseeq.com",
                samples={},
                project_results={},
            ).save(geoseeq_dir / "manifest.json")

    runner = CliRunner()
    # clone imports git_clone from geoseeq.repo.clone at call time, so patch the
    # definition site (geoseeq.repo.clone.git_clone).
    with (
        patch("geoseeq.cli.repo.handle_project_id", return_value=_mock_project()),
        patch("geoseeq.repo.clone.git_clone", side_effect=_fake_git_clone),
    ):
        result = runner.invoke(
            main,
            ["repo", "clone", "TestOrg/TestProject", str(clone_path)],
            env={"GEOSEEQ_API_TOKEN": "fake-token"},
            catch_exceptions=False,
        )
    return result, clone_path


def test_clone_writes_empty_manifest_for_refless_repo(tmp_path):
    """A refless clone (no checked-out manifest) gets a valid empty manifest."""
    result, clone_path = _run_clone(tmp_path, refless=True)

    assert result.exit_code == 0, result.output
    manifest_path = clone_path / ".geoseeq" / "manifest.json"
    assert manifest_path.exists()

    repo = GeoSeeqRepo(clone_path)
    assert repo.manifest.samples == {}
    assert repo.manifest.project_uuid == "proj-uuid"


def test_clone_prints_disabled_notice_for_refless_repo(tmp_path):
    """The CLI prints the disabled notice when clone wrote an empty manifest."""
    result, _ = _run_clone(tmp_path, refless=True)

    assert result.exit_code == 0, result.output
    assert "Audit trail is disabled for this project" in result.output
    assert "(0 samples)" in result.output


def test_clone_no_notice_when_manifest_present(tmp_path):
    """A normal clone (manifest checked out) does NOT print the disabled notice."""
    result, clone_path = _run_clone(tmp_path, refless=False)

    assert result.exit_code == 0, result.output
    assert "Audit trail is disabled for this project" not in result.output
    repo = GeoSeeqRepo(clone_path)
    assert repo._cloned_empty is False


def test_clone_does_not_persist_audit_mode_in_config(tmp_path):
    """clone never writes an audit_trail_mode field into config.json (GRF-15 decision)."""
    _, clone_path = _run_clone(tmp_path, refless=True)

    config_data = json.loads((clone_path / ".geoseeq" / "config.json").read_text())
    assert "audit_trail_mode" not in config_data
    assert set(config_data) == {"project_uuid", "server_url"}


# ---------------------------------------------------------------------------
# git_pull tests
# ---------------------------------------------------------------------------


def test_git_pull_noops_when_remote_has_no_main(tmp_path):
    """git_pull no-ops (never runs git pull) when ls-remote shows no main ref."""
    repo = _build_repo(tmp_path)

    calls = []

    def _fake_run(cmd, *args, **kwargs):
        calls.append(cmd)
        if "ls-remote" in cmd:
            return MagicMock(stdout="", returncode=0)
        # A pull must never be invoked for a refless remote.
        raise AssertionError(f"git pull should not run for a refless remote: {cmd}")

    with patch("geoseeq.repo.repo.subprocess.run", side_effect=_fake_run):
        repo.git_pull()

    assert any("ls-remote" in cmd for cmd in calls)
    assert not any("pull" in cmd for cmd in calls)


def test_git_pull_pulls_when_remote_has_main(tmp_path):
    """git_pull runs git pull --rebase when ls-remote reports a main ref."""
    repo = _build_repo(tmp_path)

    calls = []

    def _fake_run(cmd, *args, **kwargs):
        calls.append(cmd)
        if "ls-remote" in cmd:
            return MagicMock(
                stdout="abc123\trefs/heads/main\n", returncode=0
            )
        return MagicMock(stdout="", returncode=0)

    with patch("geoseeq.repo.repo.subprocess.run", side_effect=_fake_run):
        repo.git_pull()

    assert any("ls-remote" in cmd for cmd in calls)
    assert any("pull" in cmd and "--rebase" in cmd for cmd in calls)
