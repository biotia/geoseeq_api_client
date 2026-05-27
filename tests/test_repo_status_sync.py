"""Unit tests for geoseeq.repo.status and geoseeq.repo.sync (GRF-04, GRF-05)."""
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
from geoseeq.repo.status import RepoStatus, compute_status
from geoseeq.repo.sync import ChecksumError, download_file, offload_file


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _md5_hex(data: bytes) -> str:
    """Return the hex MD5 digest of *data*."""
    return hashlib.md5(data).hexdigest()


def _make_manifest_dict(files: dict[str, dict]) -> dict:
    """Build a minimal manifest dict with one sample containing *files*.

    *files* maps file_name -> {local_path, checksum}.
    """
    manifest_files = {}
    for fname, info in files.items():
        manifest_files[fname] = {
            "uuid": f"uuid-{fname}",
            "brn": f"brn:geoseeq:file:uuid-{fname}",
            "checksum": info["checksum"],
            "size_bytes": 100,
            "local_path": info["local_path"],
        }
    return {
        "version": 1,
        "project_uuid": "proj-uuid",
        "project_name": "TestOrg/TestProject",
        "server_url": "https://backend.geoseeq.com",
        "samples": {
            "Sample1": {
                "uuid": "sample-uuid-1",
                "metadata": {},
                "result_folders": {
                    "reads": {
                        "uuid": "folder-uuid-1",
                        "files": manifest_files,
                    }
                },
            }
        },
        "project_results": {},
    }


def _build_repo(tmp_path: Path, manifest_dict: dict, audit_trail_mode: str = "on") -> GeoSeeqRepo:
    """Create a minimal geoseeq repo under *tmp_path* and return its handle."""
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
# compute_status tests
# ---------------------------------------------------------------------------


def test_compute_status_absent(tmp_path):
    """A manifest file that does not exist on disk appears in absent."""
    local_path = "samples/Sample1/reads/file.fastq.gz"
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": local_path, "checksum": "md5:deadbeef"},
    })
    repo = _build_repo(tmp_path, md)

    status = compute_status(repo)

    assert local_path in status.absent
    assert local_path not in status.downloaded
    assert local_path not in status.modified_local


def test_compute_status_downloaded(tmp_path):
    """A file on disk with a matching checksum appears in downloaded."""
    content = b"hello world"
    checksum = f"md5:{_md5_hex(content)}"
    local_path = "samples/Sample1/reads/file.fastq.gz"
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": local_path, "checksum": checksum},
    })
    repo = _build_repo(tmp_path, md)

    disk_path = tmp_path / local_path
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    status = compute_status(repo)

    assert local_path in status.downloaded
    assert local_path not in status.absent
    assert local_path not in status.modified_local


def test_compute_status_modified_local(tmp_path):
    """A file on disk with a wrong checksum appears in modified_local."""
    local_path = "samples/Sample1/reads/file.fastq.gz"
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": local_path, "checksum": "md5:000000"},
    })
    repo = _build_repo(tmp_path, md)

    disk_path = tmp_path / local_path
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(b"different content")

    status = compute_status(repo)

    assert local_path in status.modified_local
    assert local_path not in status.absent
    assert local_path not in status.downloaded


def test_compute_status_new_local(tmp_path):
    """A file under samples/ not in the manifest appears in new_local."""
    md = _make_manifest_dict({})  # empty manifest
    repo = _build_repo(tmp_path, md)

    new_file = tmp_path / "samples" / "Sample1" / "extra_file.txt"
    new_file.parent.mkdir(parents=True, exist_ok=True)
    new_file.write_bytes(b"extra")

    status = compute_status(repo)

    rel = str(new_file.relative_to(tmp_path))
    assert rel in status.new_local


# ---------------------------------------------------------------------------
# download_file tests
# ---------------------------------------------------------------------------


def test_download_file_checksum_error(tmp_path):
    """download_file raises ChecksumError when the downloaded content is wrong."""
    content = b"bad content"  # does NOT match the manifest checksum
    local_path_str = "samples/Sample1/reads/file.fastq.gz"
    manifest_file = ManifestFile(
        uuid="uuid-1",
        brn="brn:geoseeq:file:uuid-1",
        checksum="md5:000000000000000000000000000000ff",
        size_bytes=100,
        local_path=local_path_str,
    )
    md = _make_manifest_dict({
        "file.fastq.gz": {
            "local_path": local_path_str,
            "checksum": manifest_file.checksum,
        }
    })
    repo = _build_repo(tmp_path, md)

    mock_result_file = MagicMock()

    def _fake_download(filename, cache):
        """Write wrong content to disk to trigger a checksum error."""
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_bytes(content)

    mock_result_file.download.side_effect = _fake_download
    knex = MagicMock()

    with patch(
        "geoseeq.id_constructors.from_uuids.result_file_from_uuid",
        return_value=mock_result_file,
    ):
        with pytest.raises(ChecksumError) as exc_info:
            download_file(repo, manifest_file, knex)

    actual_hex = _md5_hex(content)
    assert manifest_file.local_path in str(exc_info.value)
    assert manifest_file.checksum in str(exc_info.value)
    assert f"md5:{actual_hex}" in str(exc_info.value)


def test_download_file_success(tmp_path):
    """download_file succeeds when downloaded content matches the checksum."""
    content = b"correct content"
    checksum = f"md5:{_md5_hex(content)}"
    local_path_str = "samples/Sample1/reads/file.fastq.gz"
    manifest_file = ManifestFile(
        uuid="uuid-1",
        brn="brn:geoseeq:file:uuid-1",
        checksum=checksum,
        size_bytes=len(content),
        local_path=local_path_str,
    )
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": local_path_str, "checksum": checksum},
    })
    repo = _build_repo(tmp_path, md)

    mock_result_file = MagicMock()

    def _fake_download(filename, cache):
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_bytes(content)

    mock_result_file.download.side_effect = _fake_download
    knex = MagicMock()

    with patch(
        "geoseeq.id_constructors.from_uuids.result_file_from_uuid",
        return_value=mock_result_file,
    ):
        download_file(repo, manifest_file, knex)  # should not raise

    assert (tmp_path / local_path_str).exists()


# ---------------------------------------------------------------------------
# offload_file tests
# ---------------------------------------------------------------------------


def test_offload_removes_file(tmp_path):
    """offload_file deletes the local copy of a manifest file."""
    content = b"some data"
    checksum = f"md5:{_md5_hex(content)}"
    local_path_str = "samples/Sample1/reads/file.fastq.gz"
    manifest_file = ManifestFile(
        uuid="uuid-1",
        brn="brn:geoseeq:file:uuid-1",
        checksum=checksum,
        size_bytes=len(content),
        local_path=local_path_str,
    )
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": local_path_str, "checksum": checksum},
    })
    repo = _build_repo(tmp_path, md)

    disk_path = tmp_path / local_path_str
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    offload_file(repo, manifest_file)

    assert not disk_path.exists()


def test_offload_noop_when_absent(tmp_path):
    """offload_file is a no-op when the local file does not exist."""
    manifest_file = ManifestFile(
        uuid="uuid-1",
        brn="brn:geoseeq:file:uuid-1",
        checksum="md5:000",
        size_bytes=0,
        local_path="samples/Sample1/reads/file.fastq.gz",
    )
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": manifest_file.local_path, "checksum": "md5:000"},
    })
    repo = _build_repo(tmp_path, md)
    offload_file(repo, manifest_file)  # should not raise


# ---------------------------------------------------------------------------
# CLI: status command
# ---------------------------------------------------------------------------


def test_status_fully_synced_message(tmp_path):
    """status prints a clean-state message when all files are downloaded."""
    content = b"data"
    checksum = f"md5:{_md5_hex(content)}"
    local_path_str = "samples/Sample1/reads/file.fastq.gz"
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": local_path_str, "checksum": checksum},
    })
    repo = _build_repo(tmp_path, md)

    disk_path = tmp_path / local_path_str
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "status", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "fully synced" in result.output


def test_status_shows_absent_files(tmp_path):
    """status lists absent files when they are missing from disk."""
    local_path_str = "samples/Sample1/reads/file.fastq.gz"
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": local_path_str, "checksum": "md5:abc"},
    })
    repo = _build_repo(tmp_path, md)

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "status", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "absent" in result.output
    assert local_path_str in result.output


# ---------------------------------------------------------------------------
# CLI: pull command
# ---------------------------------------------------------------------------


def test_pull_downloads_only_new_files(tmp_path):
    """pull only downloads files that appear in the new manifest but not the old."""
    content = b"new file"
    checksum = f"md5:{_md5_hex(content)}"
    old_local = "samples/Sample1/reads/old.fastq.gz"
    new_local = "samples/Sample1/reads/new.fastq.gz"

    # Initial manifest has only the old file
    old_md = _make_manifest_dict({
        "old.fastq.gz": {"local_path": old_local, "checksum": "md5:old"},
    })
    repo = _build_repo(tmp_path, old_md)

    # After git_pull the manifest gains the new file
    new_md_dict = _make_manifest_dict({
        "old.fastq.gz": {"local_path": old_local, "checksum": "md5:old"},
        "new.fastq.gz": {"local_path": new_local, "checksum": checksum},
    })
    new_manifest = Manifest.from_dict(new_md_dict)

    downloaded = []

    def _fake_download(r, mfile, knex):
        """Record which files were downloaded and write fake content."""
        downloaded.append(mfile.local_path)
        disk = tmp_path / mfile.local_path
        disk.parent.mkdir(parents=True, exist_ok=True)
        disk.write_bytes(content)

    def _fake_git_pull():
        """Replace the manifest on disk with the new one."""
        new_manifest.save(tmp_path / ".geoseeq" / "manifest.json")

    runner = CliRunner()
    with (
        patch("geoseeq.cli.repo._fetch_audit_trail_mode", return_value="on"),
        patch("geoseeq.cli.repo._update_config_audit_mode"),
        patch("geoseeq.cli.repo.GeoSeeqRepo.git_pull", side_effect=_fake_git_pull),
        patch("geoseeq.repo.sync.download_file", side_effect=_fake_download),
        patch("geoseeq.cli.repo.write_pipeline_configs"),
    ):
        result = runner.invoke(
            main,
            ["repo", "pull", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert new_local in downloaded
    assert old_local not in downloaded
    assert "1 new file" in result.output


# ---------------------------------------------------------------------------
# CLI: offload command refuses modified-local files
# ---------------------------------------------------------------------------


def test_offload_refuses_modified_local(tmp_path):
    """offload raises ClickException if any targeted file has a checksum mismatch."""
    local_path_str = "samples/Sample1/reads/file.fastq.gz"
    md = _make_manifest_dict({
        "file.fastq.gz": {"local_path": local_path_str, "checksum": "md5:expected"},
    })
    repo = _build_repo(tmp_path, md)

    # Write content that does NOT match the manifest checksum
    disk_path = tmp_path / local_path_str
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(b"tampered content")

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "offload", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code != 0
    assert "Refusing" in result.output or "modified-local" in result.output
    # File must still be present — not deleted
    assert disk_path.exists()
