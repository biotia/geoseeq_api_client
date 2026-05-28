"""Unit tests for GeoSeeqRepo status/sync methods + status/pull/offload CLI (GRF-04, GRF-05)."""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.main import main
from geoseeq.repo import (
    ChecksumError,
    GeoSeeqRepo,
    Manifest,
    RepoConfig,
    RepoStatus,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _md5_hex(data: bytes) -> str:
    """Return the hex MD5 digest of *data*."""
    return hashlib.md5(data).hexdigest()


def _stored_data(filename: str) -> dict:
    """Build a server-style stored_data descriptor carrying a cloud uri."""
    return {
        "__type__": "s3",
        "uri": f"s3://bucket/{filename}",
        "endpoint_url": "https://s3.amazonaws.com",
    }


def _make_manifest_dict(files: dict[str, dict]) -> dict:
    """Build a minimal manifest dict with one sample containing *files*.

    *files* maps field_name -> {filename, checksum}.  The on-disk local path
    is derived by ``Manifest.iter_files`` from the sample/module/filename.
    """
    manifest_files = {}
    for field_name, info in files.items():
        manifest_files[field_name] = {
            "uuid": f"uuid-{field_name}",
            "checksum": info["checksum"],
            "size_bytes": 100,
            "stored_data": _stored_data(info["filename"]),
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


def _build_repo(tmp_path: Path, manifest_dict: dict) -> GeoSeeqRepo:
    """Create a minimal geoseeq repo under *tmp_path* and return its handle."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()

    config = RepoConfig(
        project_uuid="proj-uuid",
        server_url="https://backend.geoseeq.com",
    )
    config.save(geoseeq_dir / "config.json")

    manifest = Manifest.from_dict(manifest_dict)
    manifest.save(geoseeq_dir / "manifest.json")

    (tmp_path / "samples").mkdir(exist_ok=True)
    (tmp_path / "project_results").mkdir(exist_ok=True)

    return GeoSeeqRepo(tmp_path)


def _single_file_repo(tmp_path: Path, filename: str, checksum: str):
    """Build a repo with a single reads file and return (repo, entry, local_path)."""
    md = _make_manifest_dict({"read_1": {"filename": filename, "checksum": checksum}})
    repo = _build_repo(tmp_path, md)
    entry = next(repo.manifest.iter_files())
    return repo, entry, entry.local_path


# ---------------------------------------------------------------------------
# compute_status tests
# ---------------------------------------------------------------------------


def test_compute_status_absent(tmp_path):
    """A manifest file that does not exist on disk appears in absent."""
    repo, _entry, local_path = _single_file_repo(
        tmp_path, "file.fastq.gz", "md5:deadbeef"
    )

    status = repo.compute_status()

    assert local_path in status.absent
    assert local_path not in status.downloaded
    assert local_path not in status.modified_local


def test_compute_status_downloaded(tmp_path):
    """A file on disk with a matching checksum appears in downloaded."""
    content = b"hello world"
    checksum = f"md5:{_md5_hex(content)}"
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", checksum)

    disk_path = tmp_path / local_path
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    status = repo.compute_status()

    assert local_path in status.downloaded
    assert local_path not in status.absent
    assert local_path not in status.modified_local


def test_compute_status_modified_local(tmp_path):
    """A file on disk with a wrong checksum appears in modified_local."""
    repo, _entry, local_path = _single_file_repo(
        tmp_path, "file.fastq.gz", "md5:000000"
    )

    disk_path = tmp_path / local_path
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(b"different content")

    status = repo.compute_status()

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

    status = repo.compute_status()

    rel = str(new_file.relative_to(tmp_path))
    assert rel in status.new_local


def test_compute_status_returns_repo_status(tmp_path):
    """compute_status returns a RepoStatus instance."""
    repo, _entry, _local_path = _single_file_repo(tmp_path, "f.gz", "md5:x")
    assert isinstance(repo.compute_status(), RepoStatus)


# ---------------------------------------------------------------------------
# download_file tests
# ---------------------------------------------------------------------------


def test_download_file_checksum_error(tmp_path):
    """download_file raises ChecksumError when the downloaded content is wrong."""
    content = b"bad content"  # does NOT match the manifest checksum
    checksum = "md5:000000000000000000000000000000ff"
    repo, entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", checksum)

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
            repo.download_file(entry, knex)

    actual_hex = _md5_hex(content)
    assert local_path in str(exc_info.value)
    assert checksum in str(exc_info.value)
    assert f"md5:{actual_hex}" in str(exc_info.value)


def test_download_file_success(tmp_path):
    """download_file succeeds when downloaded content matches the checksum."""
    content = b"correct content"
    checksum = f"md5:{_md5_hex(content)}"
    repo, entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", checksum)

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
        repo.download_file(entry, knex)  # should not raise

    assert (tmp_path / local_path).exists()


# ---------------------------------------------------------------------------
# offload_file tests
# ---------------------------------------------------------------------------


def test_offload_removes_file(tmp_path):
    """offload_file deletes the local copy and reports True."""
    content = b"some data"
    checksum = f"md5:{_md5_hex(content)}"
    repo, entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", checksum)

    disk_path = tmp_path / local_path
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    assert repo.offload_file(entry) is True
    assert not disk_path.exists()


def test_offload_noop_when_absent(tmp_path):
    """offload_file is a no-op (returns False) when the local file is absent."""
    repo, entry, _local_path = _single_file_repo(tmp_path, "file.fastq.gz", "md5:000")
    assert repo.offload_file(entry) is False


# ---------------------------------------------------------------------------
# CLI: status command
# ---------------------------------------------------------------------------


def test_status_fully_synced_message(tmp_path):
    """status prints a clean-state message when all files are downloaded."""
    content = b"data"
    checksum = f"md5:{_md5_hex(content)}"
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", checksum)

    disk_path = tmp_path / local_path
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
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "md5:abc")

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "status", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "absent" in result.output
    assert local_path in result.output


def test_status_sample_filter(tmp_path):
    """status --sample restricts output to the named sample."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "md5:abc")

    runner = CliRunner()
    # Filtering to a different sample hides the absent file -> fully synced.
    result = runner.invoke(
        main,
        ["repo", "status", "--sample", "OtherSample", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "fully synced" in result.output
    assert local_path not in result.output


# ---------------------------------------------------------------------------
# CLI: pull command
# ---------------------------------------------------------------------------


def test_pull_reports_new_files_without_downloading(tmp_path):
    """pull reports newly-appeared files but does not download them."""
    # Initial manifest has only the old file
    old_md = _make_manifest_dict({
        "read_1": {"filename": "old.fastq.gz", "checksum": "md5:old"},
    })
    _build_repo(tmp_path, old_md)

    # After git_pull the manifest gains a new file
    new_md_dict = _make_manifest_dict({
        "read_1": {"filename": "old.fastq.gz", "checksum": "md5:old"},
        "read_2": {"filename": "new.fastq.gz", "checksum": "md5:new"},
    })
    new_manifest = Manifest.from_dict(new_md_dict)
    new_local = "samples/Sample1/reads/new.fastq.gz"

    def _fake_git_pull():
        """Replace the manifest on disk with the new one."""
        new_manifest.save(tmp_path / ".geoseeq" / "manifest.json")

    runner = CliRunner()
    with (
        patch("geoseeq.cli.repo.GeoSeeqRepo.git_pull", side_effect=_fake_git_pull),
        patch("geoseeq.cli.repo.GeoSeeqRepo.write_pipeline_configs"),
    ):
        result = runner.invoke(
            main,
            ["repo", "pull", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert "1 new file" in result.output
    assert "geoseeq repo download" in result.output
    # File should NOT have been downloaded
    assert not (tmp_path / new_local).exists()


# ---------------------------------------------------------------------------
# CLI: offload command refuses modified-local files
# ---------------------------------------------------------------------------


def test_offload_refuses_modified_local(tmp_path):
    """offload raises ClickException if any targeted file has a checksum mismatch."""
    repo, _entry, local_path = _single_file_repo(
        tmp_path, "file.fastq.gz", "md5:expected"
    )

    # Write content that does NOT match the manifest checksum
    disk_path = tmp_path / local_path
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
