"""Unit tests for audit trail disabled degradation (GRF-15).

Verifies that the CLI gracefully skips git operations when a project's
audit_trail_mode is "off", while still creating directories, fetching
manifests from the API, and storing the mode in config.json.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.main import main
from geoseeq.repo import (
    GeoSeeqRepo,
    Manifest,
    ManifestFile,
    ManifestResultFolder,
    ManifestSample,
    RepoConfig,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _md5_hex(data: bytes) -> str:
    """Return the hex MD5 digest of *data*."""
    return hashlib.md5(data).hexdigest()


def _make_manifest_dict(samples: dict | None = None) -> dict:
    """Return a minimal manifest dict.

    *samples* maps sample_name -> {folder_name -> {file_name -> info}}.
    """
    sample_data = {}
    for s_name, folders in (samples or {}).items():
        folder_data = {}
        for f_name, files in folders.items():
            file_data = {}
            for fname, info in files.items():
                file_data[fname] = {
                    "uuid": f"uuid-{fname}",
                    "brn": f"brn:geoseeq:file:uuid-{fname}",
                    "checksum": info["checksum"],
                    "size_bytes": info.get("size_bytes", 100),
                    "local_path": info["local_path"],
                }
            folder_data[f_name] = {"uuid": f"folder-uuid-{f_name}", "files": file_data}
        sample_data[s_name] = {
            "uuid": f"sample-uuid-{s_name}",
            "metadata": {},
            "result_folders": folder_data,
        }
    return {
        "version": 1,
        "project_uuid": "proj-uuid",
        "project_name": "TestOrg/TestProject",
        "server_url": "https://backend.geoseeq.com",
        "samples": sample_data,
        "project_results": {},
    }


def _build_repo(
    tmp_path: Path,
    manifest_dict: dict,
    audit_trail_mode: str = "off",
) -> GeoSeeqRepo:
    """Create a minimal geoseeq repo under *tmp_path* and return a handle."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()

    config = RepoConfig(
        project_uuid="proj-uuid",
        server_url="https://backend.geoseeq.com",
        auth_profile="default",
        git_remote_url="https://backend.geoseeq.com/api/v1/projects/proj-uuid/git",
        audit_trail_mode=audit_trail_mode,
    )
    config.save(geoseeq_dir / "config.json")

    manifest = Manifest.from_dict(manifest_dict)
    manifest.save(geoseeq_dir / "manifest.json")

    (tmp_path / "samples").mkdir(exist_ok=True)
    (tmp_path / "project_results").mkdir(exist_ok=True)

    return GeoSeeqRepo(tmp_path)


# ---------------------------------------------------------------------------
# RepoConfig tests
# ---------------------------------------------------------------------------


class TestRepoConfigAuditTrailMode:
    """Verify audit_trail_mode in RepoConfig serialization."""

    def test_config_includes_audit_trail_mode(self, tmp_path):
        """config.json includes the audit_trail_mode field."""
        config = RepoConfig(
            project_uuid="p-uuid",
            server_url="https://example.com",
            auth_profile="default",
            git_remote_url="https://example.com/api/v1/projects/p-uuid/git",
            audit_trail_mode="off",
        )
        path = tmp_path / "config.json"
        config.save(path)

        data = json.loads(path.read_text())
        assert data["audit_trail_mode"] == "off"

    def test_config_defaults_to_on_for_old_files(self, tmp_path):
        """Loading a config without audit_trail_mode defaults to 'on'.

        An absent field means the server predates the feature, so git
        operations (the original behaviour) should still work.
        """
        old_config = {
            "project_uuid": "p-uuid",
            "server_url": "https://example.com",
            "auth_profile": "default",
            "git_remote_url": "https://example.com/api/v1/projects/p-uuid/git",
        }
        path = tmp_path / "config.json"
        path.write_text(json.dumps(old_config))

        loaded = RepoConfig.load(path)
        assert loaded.audit_trail_mode == "on"

    def test_config_round_trip_on(self, tmp_path):
        """audit_trail_mode='on' survives a save/load round trip."""
        config = RepoConfig(
            project_uuid="p-uuid",
            server_url="https://example.com",
            auth_profile="default",
            git_remote_url="https://example.com/api/v1/projects/p-uuid/git",
            audit_trail_mode="on",
        )
        path = tmp_path / "config.json"
        config.save(path)
        loaded = RepoConfig.load(path)
        assert loaded.audit_trail_mode == "on"


# ---------------------------------------------------------------------------
# clone tests — audit trail disabled
# ---------------------------------------------------------------------------


class TestCloneAuditTrailDisabled:
    """Verify clone behaviour when audit_trail_mode is off."""

    def _mock_project(self):
        """Return a mock project object with a uuid."""
        proj = MagicMock()
        proj.uuid = "proj-uuid"
        return proj

    def _run_clone(self, runner, clone_path, manifest_dict):
        """Invoke `geoseeq repo clone` with audit trail off.

        Mocks the project lookup and API calls; returns the CliRunner result.
        """
        proj = self._mock_project()

        with (
            patch(
                "geoseeq.cli.repo.handle_project_id", return_value=proj,
            ),
            patch(
                "geoseeq.cli.repo._fetch_audit_trail_mode", return_value="off",
            ),
            patch(
                "geoseeq.cli.repo._fetch_manifest_from_api",
                return_value=manifest_dict,
            ),
            patch("geoseeq.cli.repo._git_clone") as mock_git_clone,
        ):
            result = runner.invoke(
                main,
                ["repo", "clone", "TestOrg/TestProject", str(clone_path)],
                env={"GEOSEEQ_API_TOKEN": "fake"},
                catch_exceptions=False,
            )
        return result, mock_git_clone

    def test_clone_creates_geoseeq_dir_without_git(self, tmp_path):
        """clone with disabled audit trail creates .geoseeq/ but never calls git clone."""
        clone_path = tmp_path / "MyProject"
        md = _make_manifest_dict({"Sample1": {}})
        runner = CliRunner()

        result, mock_git_clone = self._run_clone(runner, clone_path, md)

        assert result.exit_code == 0, result.output
        assert (clone_path / ".geoseeq").is_dir()
        assert (clone_path / ".geoseeq" / "config.json").exists()
        assert (clone_path / ".geoseeq" / "manifest.json").exists()
        mock_git_clone.assert_not_called()

    def test_clone_writes_manifest_from_api(self, tmp_path):
        """clone fetches manifest from API and writes it when audit trail is off."""
        clone_path = tmp_path / "MyProject"
        md = _make_manifest_dict({"Sample1": {}})
        runner = CliRunner()

        self._run_clone(runner, clone_path, md)

        manifest_path = clone_path / ".geoseeq" / "manifest.json"
        loaded = json.loads(manifest_path.read_text())
        assert loaded["project_uuid"] == "proj-uuid"
        assert "Sample1" in loaded["samples"]

    def test_clone_stores_audit_trail_mode_in_config(self, tmp_path):
        """clone stores audit_trail_mode='off' in config.json."""
        clone_path = tmp_path / "MyProject"
        md = _make_manifest_dict({})
        runner = CliRunner()

        self._run_clone(runner, clone_path, md)

        config_data = json.loads(
            (clone_path / ".geoseeq" / "config.json").read_text()
        )
        assert config_data["audit_trail_mode"] == "off"

    def test_clone_prints_warning_to_stderr(self, tmp_path):
        """clone calls _warn_audit_trail_disabled which prints to stderr."""
        clone_path = tmp_path / "MyProject"
        md = _make_manifest_dict({})
        runner = CliRunner()

        proj = self._mock_project()
        with (
            patch("geoseeq.cli.repo.handle_project_id", return_value=proj),
            patch("geoseeq.cli.repo._fetch_audit_trail_mode", return_value="off"),
            patch("geoseeq.cli.repo._fetch_manifest_from_api", return_value=md),
            patch("geoseeq.cli.repo._git_clone"),
            patch("geoseeq.cli.repo._warn_audit_trail_disabled") as mock_warn,
        ):
            result = runner.invoke(
                main,
                ["repo", "clone", "TestOrg/TestProject", str(clone_path)],
                env={"GEOSEEQ_API_TOKEN": "fake"},
                catch_exceptions=False,
            )

        assert result.exit_code == 0
        mock_warn.assert_called_once()


# ---------------------------------------------------------------------------
# pull tests — audit trail disabled
# ---------------------------------------------------------------------------


class TestPullAuditTrailDisabled:
    """Verify pull behaviour when audit_trail_mode is off."""

    def test_pull_skips_git_pull(self, tmp_path):
        """pull with disabled audit trail does not call git_pull."""
        md = _make_manifest_dict({"Sample1": {}})
        repo = _build_repo(tmp_path, md, audit_trail_mode="off")

        runner = CliRunner()
        with (
            patch(
                "geoseeq.cli.repo._fetch_audit_trail_mode", return_value="off",
            ),
            patch(
                "geoseeq.cli.repo._fetch_manifest_from_api", return_value=md,
            ),
            patch("geoseeq.cli.repo.GeoSeeqRepo.git_pull") as mock_pull,
        ):
            result = runner.invoke(
                main,
                ["repo", "pull", str(tmp_path)],
                env={"GEOSEEQ_API_TOKEN": "fake"},
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        mock_pull.assert_not_called()

    def test_pull_refreshes_audit_mode_in_config(self, tmp_path):
        """pull updates the stored audit_trail_mode from the server."""
        md = _make_manifest_dict({})
        # Start with "on" in config, server says "off"
        repo = _build_repo(tmp_path, md, audit_trail_mode="on")

        runner = CliRunner()
        with (
            patch(
                "geoseeq.cli.repo._fetch_audit_trail_mode", return_value="off",
            ),
            patch(
                "geoseeq.cli.repo._fetch_manifest_from_api", return_value=md,
            ),
            patch("geoseeq.cli.repo.GeoSeeqRepo.git_pull"),
        ):
            result = runner.invoke(
                main,
                ["repo", "pull", str(tmp_path)],
                env={"GEOSEEQ_API_TOKEN": "fake"},
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        config_data = json.loads(
            (tmp_path / ".geoseeq" / "config.json").read_text()
        )
        assert config_data["audit_trail_mode"] == "off"

    def test_pull_downloads_new_files_when_off(self, tmp_path):
        """pull with mode='off' still downloads newly-appeared files.

        The download loop must execute regardless of audit trail mode;
        only the manifest-fetch mechanism changes (API instead of git).
        """
        new_file_info = {
            "checksum": "md5:abc123",
            "local_path": "samples/Sample1/reads/new.fastq.gz",
        }
        initial_md = _make_manifest_dict({"Sample1": {}})
        updated_md = _make_manifest_dict({
            "Sample1": {
                "reads": {
                    "new.fastq.gz": new_file_info,
                },
            },
        })
        _build_repo(tmp_path, initial_md, audit_trail_mode="off")

        runner = CliRunner()
        with (
            patch(
                "geoseeq.cli.repo._fetch_audit_trail_mode", return_value="off",
            ),
            patch(
                "geoseeq.cli.repo._fetch_manifest_from_api",
                return_value=updated_md,
            ),
            patch("geoseeq.repo.sync.download_file") as mock_download,
        ):
            result = runner.invoke(
                main,
                ["repo", "pull", str(tmp_path)],
                env={"GEOSEEQ_API_TOKEN": "fake"},
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        assert mock_download.call_count == 1
        assert "Pulled 1 new files" in result.output


# ---------------------------------------------------------------------------
# push tests — audit trail disabled
# ---------------------------------------------------------------------------


class TestPushAuditTrailDisabled:
    """Verify push behaviour when audit_trail_mode is off."""

    def test_push_skips_git_pull_after_upload(self, tmp_path):
        """push with disabled audit trail does not call git_pull after upload."""
        content = b"new data"
        checksum = f"md5:{_md5_hex(content)}"
        local_path_str = "samples/Sample1/reads/newfile.fastq.gz"

        md = _make_manifest_dict({"Sample1": {}})
        repo = _build_repo(tmp_path, md, audit_trail_mode="off")

        disk_path = tmp_path / local_path_str
        disk_path.parent.mkdir(parents=True, exist_ok=True)
        disk_path.write_bytes(content)

        fake_mfile = ManifestFile(
            uuid="new-uuid-1",
            brn="brn:gs:sample_result_field:new-uuid-1",
            checksum=checksum,
            size_bytes=len(content),
            local_path=local_path_str,
        )

        mock_srv_folder = MagicMock()
        mock_srv_folder.uuid = "folder-uuid-reads"
        mock_srv_sample = MagicMock()
        mock_srv_sample.result_folder.return_value.idem.return_value = mock_srv_folder

        runner = CliRunner()
        with (
            patch(
                "geoseeq.cli.repo._fetch_audit_trail_mode", return_value="off",
            ),
            patch("geoseeq.cli.repo.upload_file", return_value=fake_mfile),
            patch("geoseeq.cli.repo.GeoSeeqRepo.git_pull") as mock_pull,
            patch(
                "geoseeq.id_constructors.from_uuids.sample_from_uuid",
                return_value=mock_srv_sample,
            ),
        ):
            result = runner.invoke(
                main,
                ["repo", "push", str(tmp_path)],
                env={"GEOSEEQ_API_TOKEN": "fake"},
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        mock_pull.assert_not_called()
        assert "Pushed" in result.output


# ---------------------------------------------------------------------------
# new-sample tests — audit trail disabled
# ---------------------------------------------------------------------------


class TestNewSampleAuditTrailDisabled:
    """Verify new-sample behaviour when audit_trail_mode is off."""

    def test_new_sample_skips_git_pull(self, tmp_path):
        """new-sample with disabled audit trail does not call git_pull."""
        md = _make_manifest_dict({})
        repo = _build_repo(tmp_path, md, audit_trail_mode="off")

        def _fake_create_sample(self, name, metadata, knex):
            """Stub create_sample."""
            self.manifest.samples[name] = ManifestSample(
                uuid="new-sample-uuid",
                metadata=metadata,
                result_folders={},
            )
            return "new-sample-uuid"

        runner = CliRunner()
        with (
            patch(
                "geoseeq.cli.repo._fetch_audit_trail_mode", return_value="off",
            ),
            patch(
                "geoseeq.cli.repo.GeoSeeqRepo.create_sample",
                _fake_create_sample,
            ),
            patch("geoseeq.cli.repo.GeoSeeqRepo.git_pull") as mock_pull,
        ):
            result = runner.invoke(
                main,
                ["repo", "new-sample", "MySample", str(tmp_path)],
                env={"GEOSEEQ_API_TOKEN": "fake"},
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        assert "Created sample 'MySample'" in result.output
        mock_pull.assert_not_called()


# ---------------------------------------------------------------------------
# log tests — audit trail disabled
# ---------------------------------------------------------------------------


class TestLogAuditTrailDisabled:
    """Verify log behaviour when audit_trail_mode is off."""

    def test_log_prints_message_and_exits_zero(self, tmp_path):
        """log prints 'Audit trail is disabled' and exits 0."""
        md = _make_manifest_dict({})
        repo = _build_repo(tmp_path, md, audit_trail_mode="off")

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["repo", "log", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "Audit trail is disabled for this project." in result.output

    def test_log_does_not_call_api_when_disabled(self, tmp_path):
        """log does not make any API calls when audit trail is off."""
        md = _make_manifest_dict({})
        repo = _build_repo(tmp_path, md, audit_trail_mode="off")

        runner = CliRunner()
        with patch("requests.Session.get") as mock_get:
            result = runner.invoke(
                main,
                ["repo", "log", str(tmp_path)],
                env={"GEOSEEQ_API_TOKEN": "fake"},
                catch_exceptions=False,
            )

        assert result.exit_code == 0
        mock_get.assert_not_called()
