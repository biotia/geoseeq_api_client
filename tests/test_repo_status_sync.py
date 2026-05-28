"""Unit tests for GeoSeeqRepo version-aware status/sync + status/pull/download/offload CLI.

Covers the version-aware change-detection scheme (GRF-09):
* local edits are detected via the ``.geoseeq/state.json`` xxh3 content hash;
* server staleness is detected via the manifest ``version_replicate``.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.main import main
from geoseeq.repo import (
    GeoSeeqRepo,
    Manifest,
    RepoConfig,
    RepoExistsError,
    RepoStatus,
)
from geoseeq.repo.state import RepoState, fast_hash, record_for


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _stored_data(filename: str) -> dict:
    """Build a server-style stored_data descriptor carrying a cloud uri."""
    return {
        "__type__": "s3",
        "uri": f"s3://bucket/{filename}",
        "endpoint_url": "https://s3.amazonaws.com",
    }


def _make_manifest_dict(files: dict[str, dict]) -> dict:
    """Build a minimal manifest dict with one sample containing *files*.

    *files* maps field_name -> {filename, version_replicate, [checksum]}.  The
    on-disk local path is derived by ``Manifest.iter_files`` from the
    sample/module/filename.
    """
    manifest_files = {}
    for field_name, info in files.items():
        manifest_files[field_name] = {
            "uuid": f"uuid-{field_name}",
            "checksum": info.get("checksum", {"ETag": "abc-1"}),
            "size_bytes": 100,
            "stored_data": _stored_data(info["filename"]),
            "version_replicate": info["version_replicate"],
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


def _single_file_repo(tmp_path: Path, filename: str, version_replicate: str = "v1"):
    """Build a repo with a single reads file and return (repo, entry, local_path)."""
    md = _make_manifest_dict(
        {"read_1": {"filename": filename, "version_replicate": version_replicate}}
    )
    repo = _build_repo(tmp_path, md)
    entry = next(repo.manifest.iter_files())
    return repo, entry, entry.local_path


def _write_disk_file(repo: GeoSeeqRepo, local_path: str, content: bytes) -> Path:
    """Write *content* to *local_path* under the repo root and return the path."""
    disk_path = repo.root / local_path
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)
    return disk_path


def _seed_state(repo: GeoSeeqRepo, local_path: str, version_replicate: str) -> None:
    """Record the on-disk file at *local_path* into state.json (post-download view)."""
    state = RepoState.load(repo.root / ".geoseeq")
    state.set(local_path, record_for(repo.root / local_path, version_replicate))
    state.save()


# ---------------------------------------------------------------------------
# state.py tests
# ---------------------------------------------------------------------------


def test_fast_hash_is_deterministic(tmp_path):
    """fast_hash returns the same digest for identical content and differs otherwise."""
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    a.write_bytes(b"hello world" * 1000)
    b.write_bytes(b"hello world" * 1000)
    assert fast_hash(a) == fast_hash(b)

    b.write_bytes(b"different content")
    assert fast_hash(a) != fast_hash(b)


def test_record_for_captures_size_mtime_xxh3(tmp_path):
    """record_for captures size, mtime and the xxh3 hash for a path on disk."""
    p = tmp_path / "f.bin"
    p.write_bytes(b"some bytes here")
    record = record_for(p, "v-7")

    stat = p.stat()
    assert record["version_replicate"] == "v-7"
    assert record["size_bytes"] == stat.st_size
    assert record["mtime"] == stat.st_mtime
    assert record["xxh3"] == fast_hash(p)


def test_repo_state_roundtrip(tmp_path):
    """RepoState set/save/load round-trips records faithfully."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()

    state = RepoState.load(geoseeq_dir)
    assert state.records == {}  # empty when no file exists yet

    record = {"version_replicate": "v1", "size_bytes": 10, "mtime": 1.5, "xxh3": "deadbeef"}
    state.set("samples/S1/reads/f.gz", record)
    state.save()

    reloaded = RepoState.load(geoseeq_dir)
    assert reloaded.get("samples/S1/reads/f.gz") == record


def test_repo_state_remove(tmp_path):
    """RepoState.remove drops a record and is a no-op for unknown paths."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()
    state = RepoState.load(geoseeq_dir)
    state.set("p", {"version_replicate": "v", "size_bytes": 1, "mtime": 0.0, "xxh3": "x"})

    state.remove("p")
    assert state.get("p") is None
    state.remove("does-not-exist")  # no error


# ---------------------------------------------------------------------------
# compute_status tests
# ---------------------------------------------------------------------------


def test_compute_status_absent(tmp_path):
    """A manifest file that does not exist on disk appears in absent."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz")

    status = repo.compute_status()

    assert local_path in status.absent
    assert local_path not in status.downloaded
    assert local_path not in status.modified_local
    assert local_path not in status.outdated


def test_compute_status_downloaded_unchanged(tmp_path):
    """A recorded file unchanged on disk (stat fast-path) appears in downloaded."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")
    _write_disk_file(repo, local_path, b"hello world")
    _seed_state(repo, local_path, "v1")

    status = repo.compute_status()

    assert local_path in status.downloaded
    assert local_path not in status.absent
    assert local_path not in status.modified_local
    assert local_path not in status.outdated


def test_compute_status_modified_local(tmp_path):
    """A file whose content changed after download appears in modified_local."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")
    _write_disk_file(repo, local_path, b"original content")
    _seed_state(repo, local_path, "v1")

    # Edit the bytes in place so size and hash differ from the recorded state.
    _write_disk_file(repo, local_path, b"locally edited content!!")

    status = repo.compute_status()

    assert local_path in status.modified_local
    assert local_path not in status.downloaded
    assert local_path not in status.absent


def test_compute_status_modified_same_size_different_bytes(tmp_path):
    """A same-size edit is caught via the xxh3 hash when the stat differs.

    The stat fast-path short-circuits only when BOTH size and mtime match the
    record (an unedited file).  Here size is unchanged but the recorded mtime
    differs from disk, so compute_status must fall back to the content hash —
    which reveals the same-size byte change.
    """
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")
    disk_path = _write_disk_file(repo, local_path, b"AAAAAAAAAAAA")
    _seed_state(repo, local_path, "v1")

    # Same-length but different content; force a recorded-mtime mismatch so the
    # cheap stat path cannot short-circuit and the hash comparison runs.
    disk_path.write_bytes(b"BBBBBBBBBBBB")
    state = RepoState.load(repo.root / ".geoseeq")
    record = state.get(local_path)
    record["mtime"] = record["mtime"] - 100.0  # stale recorded mtime
    state.set(local_path, record)
    state.save()

    status = repo.compute_status()

    assert local_path in status.modified_local
    assert local_path not in status.downloaded


def test_compute_status_outdated(tmp_path):
    """A file whose manifest version_replicate differs from the record is outdated."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v2")
    _write_disk_file(repo, local_path, b"hello world")
    # Recorded under the OLD server version; manifest now reports v2.
    _seed_state(repo, local_path, "v1")

    status = repo.compute_status()

    assert local_path in status.outdated
    # Unmodified locally, so it is also reported as downloaded (independent lists).
    assert local_path in status.downloaded
    assert local_path not in status.modified_local


def test_compute_status_present_without_record_is_downloaded(tmp_path):
    """A file present on disk with no state record is treated as downloaded.

    This is the back-compat case: a file fetched by an older client that did
    not maintain state.json.  We cannot verify its content, so we do not flag
    it as modified.
    """
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")
    _write_disk_file(repo, local_path, b"present but unrecorded")

    status = repo.compute_status()

    assert local_path in status.downloaded
    assert local_path not in status.modified_local
    assert local_path not in status.outdated


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
    repo, _entry, _local_path = _single_file_repo(tmp_path, "f.gz")
    assert isinstance(repo.compute_status(), RepoStatus)


# ---------------------------------------------------------------------------
# download_file tests
# ---------------------------------------------------------------------------


def test_download_file_records_state(tmp_path):
    """download_file records version_replicate/size/mtime/xxh3 and never raises on ETag."""
    content = b"downloaded content"
    repo, entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v-dl-9")

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
        repo.download_file(entry, knex)  # must not raise

    disk_path = tmp_path / local_path
    assert disk_path.exists()

    record = RepoState.load(tmp_path / ".geoseeq").get(local_path)
    assert record is not None
    assert record["version_replicate"] == "v-dl-9"
    assert record["size_bytes"] == len(content)
    assert record["xxh3"] == fast_hash(disk_path)
    assert record["mtime"] == disk_path.stat().st_mtime


def test_download_then_status_is_downloaded(tmp_path):
    """A freshly downloaded file is reported as downloaded by compute_status."""
    content = b"freshly downloaded"
    repo, entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")

    mock_result_file = MagicMock()
    mock_result_file.download.side_effect = lambda filename, cache: (
        Path(filename).parent.mkdir(parents=True, exist_ok=True),
        Path(filename).write_bytes(content),
    )
    knex = MagicMock()

    with patch(
        "geoseeq.id_constructors.from_uuids.result_file_from_uuid",
        return_value=mock_result_file,
    ):
        repo.download_file(entry, knex)

    status = repo.compute_status()
    assert local_path in status.downloaded
    assert local_path not in status.modified_local
    assert local_path not in status.outdated


# ---------------------------------------------------------------------------
# offload_file tests
# ---------------------------------------------------------------------------


def test_offload_removes_file_and_state_record(tmp_path):
    """offload_file deletes the local copy, reports True, and drops the state record."""
    repo, entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")
    disk_path = _write_disk_file(repo, local_path, b"some data")
    _seed_state(repo, local_path, "v1")

    assert repo.offload_file(entry) is True
    assert not disk_path.exists()
    assert RepoState.load(tmp_path / ".geoseeq").get(local_path) is None


def test_offload_noop_when_absent(tmp_path):
    """offload_file is a no-op (returns False) when the local file is absent."""
    repo, entry, _local_path = _single_file_repo(tmp_path, "file.fastq.gz")
    assert repo.offload_file(entry) is False


# ---------------------------------------------------------------------------
# CLI: status command
# ---------------------------------------------------------------------------


def test_status_fully_synced_message(tmp_path):
    """status prints a clean-state message when all files are downloaded and current."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")
    _write_disk_file(repo, local_path, b"data")
    _seed_state(repo, local_path, "v1")

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
    _repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz")

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


def test_status_shows_outdated_files(tmp_path):
    """status lists outdated files when a newer server version exists."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v2")
    _write_disk_file(repo, local_path, b"data")
    _seed_state(repo, local_path, "v1")  # recorded under the older version

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "status", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "outdated" in result.output
    assert local_path in result.output


def test_status_sample_filter(tmp_path):
    """status --sample restricts output to the named sample."""
    _repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz")

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
    old_md = _make_manifest_dict(
        {"read_1": {"filename": "old.fastq.gz", "version_replicate": "v1"}}
    )
    _build_repo(tmp_path, old_md)

    new_md_dict = _make_manifest_dict(
        {
            "read_1": {"filename": "old.fastq.gz", "version_replicate": "v1"},
            "read_2": {"filename": "new.fastq.gz", "version_replicate": "v1"},
        }
    )
    new_manifest = Manifest.from_dict(new_md_dict)
    new_local = "samples/Sample1/reads/new.fastq.gz"

    def _fake_git_pull():
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
    assert "1 new" in result.output
    assert "0 updated" in result.output
    assert "geoseeq repo download" in result.output
    assert not (tmp_path / new_local).exists()


def test_pull_reports_updated_files_distinctly(tmp_path):
    """pull distinguishes files whose version_replicate changed from new files."""
    old_md = _make_manifest_dict(
        {"read_1": {"filename": "f.fastq.gz", "version_replicate": "v1"}}
    )
    _build_repo(tmp_path, old_md)

    # Same path, bumped version_replicate -> an update, not a new file.
    new_md_dict = _make_manifest_dict(
        {"read_1": {"filename": "f.fastq.gz", "version_replicate": "v2"}}
    )
    new_manifest = Manifest.from_dict(new_md_dict)

    def _fake_git_pull():
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
    assert "0 new" in result.output
    assert "1 updated" in result.output


def test_pull_already_up_to_date(tmp_path):
    """pull prints 'Already up to date.' when the manifest gained no changes."""
    md = _make_manifest_dict(
        {"read_1": {"filename": "old.fastq.gz", "version_replicate": "v1"}}
    )
    _build_repo(tmp_path, md)

    runner = CliRunner()
    with (
        patch("geoseeq.cli.repo.GeoSeeqRepo.git_pull"),
        patch("geoseeq.cli.repo.GeoSeeqRepo.write_pipeline_configs"),
    ):
        result = runner.invoke(
            main,
            ["repo", "pull", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert "Already up to date." in result.output


# ---------------------------------------------------------------------------
# CLI: offload command refuses modified-local files
# ---------------------------------------------------------------------------


def test_offload_refuses_modified_local(tmp_path):
    """offload raises ClickException if any targeted file has been edited locally."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")
    _write_disk_file(repo, local_path, b"original content")
    _seed_state(repo, local_path, "v1")
    # Edit on disk so it no longer matches the recorded hash.
    _write_disk_file(repo, local_path, b"tampered content is longer")

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "offload", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code != 0
    assert "Refusing" in result.output or "modified-local" in result.output
    assert (tmp_path / local_path).exists()


def test_offload_count_reflects_actual_deletions(tmp_path):
    """offload reports the number of files actually deleted, not just targeted."""
    md = _make_manifest_dict(
        {
            "read_1": {"filename": "present.fastq.gz", "version_replicate": "v1"},
            "read_2": {"filename": "absent.fastq.gz", "version_replicate": "v1"},
        }
    )
    repo = _build_repo(tmp_path, md)

    present_entry = next(
        e for e in repo.manifest.iter_files() if "present" in e.local_path
    )
    _write_disk_file(repo, present_entry.local_path, b"present")
    _seed_state(repo, present_entry.local_path, "v1")

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "offload", "--yes", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "Offloaded 1 file(s)." in result.output
    assert not (tmp_path / present_entry.local_path).exists()


# ---------------------------------------------------------------------------
# CLI: download command
# ---------------------------------------------------------------------------


def test_download_all_adds_targets_and_downloads(tmp_path):
    """download --all builds a target per manifest file and runs the manager."""
    md = _make_manifest_dict(
        {
            "read_1": {"filename": "r1.fastq.gz", "version_replicate": "v1"},
            "read_2": {"filename": "r2.fastq.gz", "version_replicate": "v1"},
        }
    )
    repo = _build_repo(tmp_path, md)
    expected_paths = {e.local_path for e in repo.manifest.iter_files()}

    mock_manager = MagicMock()
    added: list[str] = []
    mock_manager.add_download.side_effect = lambda rf, dest: added.append(dest)

    runner = CliRunner()
    with (
        patch(
            "geoseeq.upload_download_manager.GeoSeeqDownloadManager",
            return_value=mock_manager,
        ),
        patch(
            "geoseeq.id_constructors.from_uuids.result_file_from_uuid",
            return_value=MagicMock(),
        ),
    ):
        result = runner.invoke(
            main,
            ["repo", "download", "--all", "--yes", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    mock_manager.download_files.assert_called_once()
    assert mock_manager.add_download.call_count == len(expected_paths)
    added_rel = {str(Path(dest).relative_to(tmp_path)) for dest in added}
    assert added_rel == expected_paths


def test_download_includes_outdated_files(tmp_path):
    """download (no --all) targets outdated files in addition to absent ones."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v2")
    _write_disk_file(repo, local_path, b"old version content")
    _seed_state(repo, local_path, "v1")  # recorded under v1, manifest now v2

    mock_manager = MagicMock()
    added: list[str] = []
    mock_manager.add_download.side_effect = lambda rf, dest: added.append(dest)

    runner = CliRunner()
    with (
        patch(
            "geoseeq.upload_download_manager.GeoSeeqDownloadManager",
            return_value=mock_manager,
        ),
        patch(
            "geoseeq.id_constructors.from_uuids.result_file_from_uuid",
            return_value=MagicMock(),
        ),
    ):
        result = runner.invoke(
            main,
            ["repo", "download", "--yes", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    # The outdated file is selected for download even though it is on disk.
    assert mock_manager.add_download.call_count == 1
    assert str(Path(added[0]).relative_to(tmp_path)) == local_path


def test_download_records_state_after_fetch(tmp_path):
    """After a CLI download the per-file state is recorded for downloaded targets."""
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v-cli-3")

    content = b"cli downloaded bytes"

    def _fake_download_files():
        """Stand in for the manager: write the file the command queued."""
        disk_path = tmp_path / local_path
        disk_path.parent.mkdir(parents=True, exist_ok=True)
        disk_path.write_bytes(content)

    mock_manager = MagicMock()
    mock_manager.download_files.side_effect = _fake_download_files

    runner = CliRunner()
    with (
        patch(
            "geoseeq.upload_download_manager.GeoSeeqDownloadManager",
            return_value=mock_manager,
        ),
        patch(
            "geoseeq.id_constructors.from_uuids.result_file_from_uuid",
            return_value=MagicMock(),
        ),
    ):
        result = runner.invoke(
            main,
            ["repo", "download", "--yes", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    record = RepoState.load(tmp_path / ".geoseeq").get(local_path)
    assert record is not None
    assert record["version_replicate"] == "v-cli-3"
    assert record["size_bytes"] == len(content)


# ---------------------------------------------------------------------------
# GeoSeeqRepo.clone server_url derivation
# ---------------------------------------------------------------------------


def test_clone_derives_server_url_and_remote(tmp_path):
    """clone() strips trailing /api from endpoint_url and persists the bare host."""
    from geoseeq.knex import Knex

    knex = Knex(endpoint_url="https://backend.geoseeq.com")
    assert knex.endpoint_url == "https://backend.geoseeq.com/api"
    knex.add_api_token("secret-token")

    proj = MagicMock()
    proj.uuid = "proj-uuid-9999"

    clone_path = tmp_path / "cloned"

    def _fake_git_clone(remote_url, geoseeq_dir, token, server_url):
        geoseeq_dir.mkdir(parents=True, exist_ok=True)
        Manifest.from_dict(_make_manifest_dict({})).save(
            geoseeq_dir / "manifest.json"
        )

    with (
        patch("geoseeq.repo.clone.git_clone", side_effect=_fake_git_clone),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        repo = GeoSeeqRepo.clone(knex, proj, clone_path)

    config = RepoConfig.load(clone_path / ".geoseeq" / "config.json")
    assert config.server_url == "https://backend.geoseeq.com"
    assert (
        config.git_remote_url
        == "https://backend.geoseeq.com/api/v1/projects/proj-uuid-9999/git"
    )
    assert repo.config.server_url == "https://backend.geoseeq.com"


def test_clone_gitignores_state_json(tmp_path):
    """clone() gitignores both config.json and state.json."""
    from geoseeq.knex import Knex

    knex = Knex(endpoint_url="https://backend.geoseeq.com")
    knex.add_api_token("secret-token")

    proj = MagicMock()
    proj.uuid = "proj-uuid-9999"

    clone_path = tmp_path / "cloned"

    def _fake_git_clone(remote_url, geoseeq_dir, token, server_url):
        geoseeq_dir.mkdir(parents=True, exist_ok=True)
        Manifest.from_dict(_make_manifest_dict({})).save(
            geoseeq_dir / "manifest.json"
        )

    with (
        patch("geoseeq.repo.clone.git_clone", side_effect=_fake_git_clone),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        GeoSeeqRepo.clone(knex, proj, clone_path)

    gitignore = (clone_path / ".geoseeq" / ".gitignore").read_text()
    assert "config.json" in gitignore
    assert "state.json" in gitignore


def test_clone_raises_when_repo_already_exists(tmp_path):
    """clone() raises RepoExistsError (not a CLI exception) on an existing repo."""
    from geoseeq.knex import Knex

    knex = Knex(endpoint_url="https://backend.geoseeq.com")
    proj = MagicMock()
    proj.uuid = "proj-uuid-9999"

    clone_path = tmp_path / "cloned"
    (clone_path / ".geoseeq").mkdir(parents=True)

    with pytest.raises(RepoExistsError):
        GeoSeeqRepo.clone(knex, proj, clone_path)
