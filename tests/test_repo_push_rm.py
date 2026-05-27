"""Unit tests for push, new-sample, and rm commands (GRF-06, GRF-07)."""
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
from geoseeq.repo.status import RepoStatus


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _md5_hex(data: bytes) -> str:
    """Return the hex MD5 digest of *data*."""
    return hashlib.md5(data).hexdigest()


def _make_manifest_dict(samples: dict | None = None) -> dict:
    """Return a minimal manifest dict.

    *samples* maps sample_name -> {folder_name -> {file_name -> {local_path, checksum}}}.
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


def _build_repo(tmp_path: Path, manifest_dict: dict) -> GeoSeeqRepo:
    """Create a minimal geoseeq repo under *tmp_path* and return a GeoSeeqRepo handle."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()

    config = RepoConfig(
        project_uuid="proj-uuid",
        server_url="https://backend.geoseeq.com",
        auth_profile="default",
        git_remote_url="https://backend.geoseeq.com/api/v1/projects/proj-uuid/git",
    )
    config.save(geoseeq_dir / "config.json")

    manifest = Manifest.from_dict(manifest_dict)
    manifest.save(geoseeq_dir / "manifest.json")

    (tmp_path / "samples").mkdir(exist_ok=True)
    (tmp_path / "project_results").mkdir(exist_ok=True)

    return GeoSeeqRepo(tmp_path)


# ---------------------------------------------------------------------------
# push tests
# ---------------------------------------------------------------------------


def test_push_uploads_new_local_files(tmp_path):
    """push uploads new-local files, updates the manifest, and calls git_pull."""
    content = b"new data"
    checksum = f"md5:{_md5_hex(content)}"
    local_path_str = "samples/Sample1/reads/newfile.fastq.gz"

    # Manifest starts empty (no files)
    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)

    # Create the file on disk so it appears as new-local
    disk_path = tmp_path / local_path_str
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    uploaded = []

    def _fake_upload_file(r, lp, s_name, f_name, fname, knex):
        """Record the call and return a ManifestFile."""
        uploaded.append((s_name, f_name, fname))
        return ManifestFile(
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
        patch("geoseeq.cli.repo.upload_file", side_effect=_fake_upload_file),
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
    assert len(uploaded) == 1
    assert uploaded[0] == ("Sample1", "reads", "newfile.fastq.gz")
    mock_pull.assert_called_once()
    assert "Pushed" in result.output


def test_push_does_not_write_git_commits(tmp_path):
    """push never calls commit() or git_push() — the server is sole commit authority."""
    content = b"data"
    local_path_str = "samples/Sample1/reads/file.fastq.gz"

    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)

    disk_path = tmp_path / local_path_str
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    fake_mfile = ManifestFile(
        uuid="u1",
        brn="brn:gs:sample_result_field:u1",
        checksum=f"md5:{_md5_hex(content)}",
        size_bytes=len(content),
        local_path=local_path_str,
    )

    mock_srv_folder = MagicMock()
    mock_srv_folder.uuid = "folder-uuid-reads"
    mock_srv_sample = MagicMock()
    mock_srv_sample.result_folder.return_value.idem.return_value = mock_srv_folder

    runner = CliRunner()
    with (
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
    mock_pull.assert_called_once()
    # Verify commit and git_push methods no longer exist on GeoSeeqRepo
    assert not hasattr(GeoSeeqRepo, "commit")
    assert not hasattr(GeoSeeqRepo, "git_push")


def test_push_nothing_to_push(tmp_path):
    """push prints 'Nothing to push.' when status is clean."""
    content = b"existing"
    checksum = f"md5:{_md5_hex(content)}"
    local_path_str = "samples/Sample1/reads/file.fastq.gz"

    md = _make_manifest_dict(
        {"Sample1": {"reads": {"file.fastq.gz": {"local_path": local_path_str, "checksum": checksum}}}}
    )
    repo = _build_repo(tmp_path, md)

    # Write matching file so status is clean
    disk_path = tmp_path / local_path_str
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "push", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "Nothing to push." in result.output


# ---------------------------------------------------------------------------
# new-sample tests
# ---------------------------------------------------------------------------


def test_new_sample_creates_server_and_directory(tmp_path):
    """new-sample calls create_sample, creates the local directory, and calls git_pull."""
    md = _make_manifest_dict({})
    repo = _build_repo(tmp_path, md)

    def _fake_create_sample(self, name, metadata, knex):
        """Stub create_sample: add a ManifestSample entry."""
        self.manifest.samples[name] = ManifestSample(
            uuid="new-sample-uuid",
            metadata=metadata,
            result_folders={},
        )
        return "new-sample-uuid"

    runner = CliRunner()
    with (
        patch("geoseeq.cli.repo.GeoSeeqRepo.create_sample", _fake_create_sample),
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
    assert (tmp_path / "samples" / "MySample").is_dir()
    mock_pull.assert_called_once()


def test_new_sample_loads_metadata_from_file(tmp_path):
    """new-sample reads metadata from --metadata-file and passes it to create_sample."""
    md = _make_manifest_dict({})
    repo = _build_repo(tmp_path, md)

    meta = {"location": "USA", "collection_date": "2024-01-01"}
    meta_file = tmp_path / "meta.json"
    meta_file.write_text(json.dumps(meta))

    received_metadata = []

    def _fake_create_sample(self, name, metadata, knex):
        received_metadata.append(metadata)
        self.manifest.samples[name] = ManifestSample(
            uuid="new-sample-uuid", metadata=metadata, result_folders={}
        )
        return "new-sample-uuid"

    runner = CliRunner()
    with (
        patch("geoseeq.cli.repo.GeoSeeqRepo.create_sample", _fake_create_sample),
        patch("geoseeq.cli.repo.GeoSeeqRepo.git_pull"),
    ):
        result = runner.invoke(
            main,
            [
                "repo",
                "new-sample",
                "MySample",
                "--metadata-file",
                str(meta_file),
                str(tmp_path),
            ],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert received_metadata[0] == meta


# ---------------------------------------------------------------------------
# rm tests
# ---------------------------------------------------------------------------


def test_rm_clean_repo_removes_directory(tmp_path):
    """rm removes the repo root when the status is fully synced."""
    content = b"data"
    checksum = f"md5:{_md5_hex(content)}"
    local_path_str = "samples/Sample1/reads/file.fastq.gz"

    md = _make_manifest_dict(
        {"Sample1": {"reads": {"file.fastq.gz": {"local_path": local_path_str, "checksum": checksum}}}}
    )
    repo = _build_repo(tmp_path, md)

    disk_path = tmp_path / local_path_str
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    import shutil

    removed = []

    def _fake_rmtree(path):
        removed.append(path)

    runner = CliRunner()
    with patch("geoseeq.cli.repo.shutil.rmtree", side_effect=_fake_rmtree):
        result = runner.invoke(
            main,
            ["repo", "rm", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert len(removed) == 1
    assert removed[0] == tmp_path
    assert "Removed local repo" in result.output


def test_rm_dirty_repo_raises_error(tmp_path):
    """rm raises ClickException with the exact error string if new_local is not empty."""
    local_path_str = "samples/Sample1/reads/file.fastq.gz"

    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)

    # Write a file that is not in the manifest → new_local
    disk_path = tmp_path / local_path_str
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(b"unsaved data")

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "rm", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code != 0
    assert "project is not fully synced" in result.output
    assert "geoseeq repo status" in result.output
    # Repo root must still exist
    assert tmp_path.exists()
