"""Unit tests for GeoSeeqRepo version-aware status/sync + status/pull/download/offload CLI.

Covers the version-aware change-detection scheme (GRF-09):
* local edits are detected via the ``.geoseeq/state.json`` xxh3 content hash;
* server staleness is detected via the manifest ``version_replicate``.
"""
from __future__ import annotations

import json
import os
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
from geoseeq.repo.state import (
    DownloadStateRecorder,
    RepoState,
    fast_hash,
    record_for,
    record_under_lock,
)


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
# record_under_lock / DownloadStateRecorder tests
# ---------------------------------------------------------------------------


def _record(version="v1", size=1, mtime=0.0, xxh3="x") -> dict:
    """Build a minimal state record dict."""
    return {"version_replicate": version, "size_bytes": size, "mtime": mtime, "xxh3": xxh3}


def test_record_under_lock_adds_and_persists(tmp_path):
    """record_under_lock writes a single path's record and persists it to disk."""
    geoseeq_dir = tmp_path / ".geoseeq"

    record_under_lock(geoseeq_dir, "samples/S1/reads/a.gz", _record("v1", 10))

    reloaded = RepoState.load(geoseeq_dir)
    assert reloaded.get("samples/S1/reads/a.gz") == _record("v1", 10)


def test_record_under_lock_sequential_completions_do_not_clobber(tmp_path):
    """A second record_under_lock updates a different path without losing the first.

    Simulates two sequential file 'completions' writing to the same state.json:
    the second write must not drop the first path's record.
    """
    geoseeq_dir = tmp_path / ".geoseeq"

    record_under_lock(geoseeq_dir, "a", _record("v1", 1))
    record_under_lock(geoseeq_dir, "b", _record("v2", 2))

    reloaded = RepoState.load(geoseeq_dir)
    assert reloaded.get("a") == _record("v1", 1)
    assert reloaded.get("b") == _record("v2", 2)


def test_record_under_lock_updates_existing_path(tmp_path):
    """record_under_lock replaces an existing path's record in place."""
    geoseeq_dir = tmp_path / ".geoseeq"

    record_under_lock(geoseeq_dir, "a", _record("v1", 1))
    record_under_lock(geoseeq_dir, "a", _record("v2", 99))

    reloaded = RepoState.load(geoseeq_dir)
    assert reloaded.get("a") == _record("v2", 99)


def test_download_state_recorder_is_picklable():
    """DownloadStateRecorder round-trips through pickle (multiprocessing requirement).

    The callback is handed to ``multiprocessing.Pool`` under ``--cores>1``, so it
    MUST be picklable.  A module-level class with str attrs pickles; a lambda or
    local closure would not.
    """
    import pickle

    recorder = DownloadStateRecorder("some/.geoseeq", "v-7")
    restored = pickle.loads(pickle.dumps(recorder))

    assert isinstance(restored, DownloadStateRecorder)
    assert restored.geoseeq_dir == "some/.geoseeq"
    assert restored.version_replicate == "v-7"


def test_download_state_recorder_writes_expected_record(tmp_path):
    """Calling a recorder with (rel_path, abs_local_path) writes the full record."""
    geoseeq_dir = tmp_path / ".geoseeq"
    content = b"recorder under test"
    disk_path = tmp_path / "samples" / "S1" / "reads" / "f.gz"
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)

    recorder = DownloadStateRecorder(str(geoseeq_dir), "v-rec-5")
    recorder("samples/S1/reads/f.gz", str(disk_path))

    record = RepoState.load(geoseeq_dir).get("samples/S1/reads/f.gz")
    assert record["version_replicate"] == "v-rec-5"
    assert record["size_bytes"] == len(content)
    assert record["mtime"] == disk_path.stat().st_mtime
    assert record["xxh3"] == fast_hash(disk_path)


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


def test_outdated_file_no_longer_outdated_after_redownload(tmp_path):
    """Re-downloading an outdated file clears it from outdated and marks it downloaded.

    Proves the end-to-end contract: a file recorded under an old server version
    (v1) whose manifest now reports v2 is initially ``outdated``.  After a
    download writes new bytes and records state for v2 (via the same
    ``record_for`` path the CLI/SDK use), the file is no longer outdated and is
    classified ``downloaded`` against the new content's xxh3.
    """
    old_content = b"old version content"
    new_content = b"v2 fetched content differs"
    repo, entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v2")
    disk_path = _write_disk_file(repo, local_path, old_content)
    # Recorded under the OLD server version; manifest now reports v2.
    _seed_state(repo, local_path, "v1")

    before = repo.compute_status()
    assert local_path in before.outdated
    assert local_path in before.downloaded

    # Simulate a download that writes NEW bytes and records state for v2,
    # mirroring reality: download_file fetches by uuid then calls record_for.
    mock_result_file = MagicMock()

    def _fake_download(filename, cache):
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_bytes(new_content)

    mock_result_file.download.side_effect = _fake_download
    knex = MagicMock()

    with patch(
        "geoseeq.id_constructors.from_uuids.result_file_from_uuid",
        return_value=mock_result_file,
    ):
        repo.download_file(entry, knex)

    # The recorded state now reflects v2 and the new content's hash.
    record = RepoState.load(tmp_path / ".geoseeq").get(local_path)
    assert record["version_replicate"] == "v2"
    assert record["xxh3"] == fast_hash(disk_path)

    after = repo.compute_status()
    assert after.outdated == []
    assert local_path in after.downloaded
    assert local_path not in after.modified_local


def test_touch_refresh_keeps_downloaded_and_refreshes_recorded_mtime(tmp_path):
    """A touched-but-unchanged file stays downloaded and its recorded mtime is refreshed.

    When only the mtime changes (content byte-identical), ``_is_modified`` must
    fall back to the xxh3 hash, find it matches, and refresh the recorded mtime
    so the next status read can use the cheap stat fast-path.  Asserts both the
    classification (``downloaded``, not ``modified_local``) and that the
    persisted state.json record's ``mtime`` was updated to the new mtime.
    """
    repo, _entry, local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")
    disk_path = _write_disk_file(repo, local_path, b"identical content bytes")
    _seed_state(repo, local_path, "v1")

    recorded_mtime = RepoState.load(tmp_path / ".geoseeq").get(local_path)["mtime"]

    # Touch the file: change mtime without changing content.
    new_mtime = recorded_mtime + 500.0
    os.utime(disk_path, (new_mtime, new_mtime))
    assert disk_path.stat().st_mtime != recorded_mtime

    status = repo.compute_status()

    assert local_path in status.downloaded
    assert local_path not in status.modified_local

    # The refreshed mtime was persisted, so the next status uses the stat fast-path.
    refreshed = RepoState.load(tmp_path / ".geoseeq").get(local_path)
    assert refreshed["mtime"] == disk_path.stat().st_mtime
    assert refreshed["mtime"] != recorded_mtime


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
# GeoSeeqRepo.pull tests
# ---------------------------------------------------------------------------


def _swap_manifest_on_pull(repo: GeoSeeqRepo, new_manifest_dict: dict):
    """Return a git_pull stand-in that writes *new_manifest_dict* to disk."""

    def _fake_git_pull():
        Manifest.from_dict(new_manifest_dict).save(
            repo.root / ".geoseeq" / "manifest.json"
        )

    return _fake_git_pull


def test_pull_returns_new_entries(tmp_path):
    """pull() returns a file that appeared in the manifest as new, not updated."""
    old_md = _make_manifest_dict(
        {"read_1": {"filename": "old.fastq.gz", "version_replicate": "v1"}}
    )
    repo = _build_repo(tmp_path, old_md)

    new_md = _make_manifest_dict(
        {
            "read_1": {"filename": "old.fastq.gz", "version_replicate": "v1"},
            "read_2": {"filename": "new.fastq.gz", "version_replicate": "v1"},
        }
    )

    with (
        patch.object(GeoSeeqRepo, "git_pull", side_effect=_swap_manifest_on_pull(repo, new_md)),
        patch.object(GeoSeeqRepo, "write_pipeline_configs") as mock_write,
    ):
        new_files, updated_files = repo.pull()

    assert [e.local_path for e in new_files] == ["samples/Sample1/reads/new.fastq.gz"]
    assert updated_files == []
    mock_write.assert_called_once()


def test_pull_returns_updated_entries(tmp_path):
    """pull() classifies a same-path version_replicate bump as updated, not new."""
    old_md = _make_manifest_dict(
        {"read_1": {"filename": "f.fastq.gz", "version_replicate": "v1"}}
    )
    repo = _build_repo(tmp_path, old_md)

    new_md = _make_manifest_dict(
        {"read_1": {"filename": "f.fastq.gz", "version_replicate": "v2"}}
    )

    with (
        patch.object(GeoSeeqRepo, "git_pull", side_effect=_swap_manifest_on_pull(repo, new_md)),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        new_files, updated_files = repo.pull()

    assert new_files == []
    assert [e.local_path for e in updated_files] == ["samples/Sample1/reads/f.fastq.gz"]


def test_pull_returns_empty_when_unchanged(tmp_path):
    """pull() returns ([], []) when the manifest gains no new or updated files."""
    md = _make_manifest_dict(
        {"read_1": {"filename": "f.fastq.gz", "version_replicate": "v1"}}
    )
    repo = _build_repo(tmp_path, md)

    with (
        patch.object(GeoSeeqRepo, "git_pull"),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        new_files, updated_files = repo.pull()

    assert new_files == []
    assert updated_files == []


def test_pull_invalidates_cached_manifest(tmp_path):
    """pull() invalidates the cached manifest so the refreshed file is visible."""
    old_md = _make_manifest_dict(
        {"read_1": {"filename": "old.fastq.gz", "version_replicate": "v1"}}
    )
    repo = _build_repo(tmp_path, old_md)
    # Prime the manifest cache with the pre-pull state.
    assert {e.local_path for e in repo.manifest.iter_files()} == {
        "samples/Sample1/reads/old.fastq.gz"
    }

    new_md = _make_manifest_dict(
        {
            "read_1": {"filename": "old.fastq.gz", "version_replicate": "v1"},
            "read_2": {"filename": "new.fastq.gz", "version_replicate": "v1"},
        }
    )

    with (
        patch.object(GeoSeeqRepo, "git_pull", side_effect=_swap_manifest_on_pull(repo, new_md)),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        repo.pull()

    # The cache was invalidated, so the new file is now visible on the handle.
    assert "samples/Sample1/reads/new.fastq.gz" in {
        e.local_path for e in repo.manifest.iter_files()
    }


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
    mock_manager.add_download.side_effect = lambda rf, dest, **kwargs: added.append(dest)

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
    mock_manager.add_download.side_effect = lambda rf, dest, **kwargs: added.append(dest)

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


def _capture_download_callbacks(tmp_path):
    """Run the CLI download against a mocked manager, capturing per-file callbacks.

    Returns a list of ``(key, callback)`` pairs the command queued via
    ``add_download``.  ``download_files`` is left as a no-op so the test can
    drive the callbacks itself, file-by-file, to model how the real manager
    invokes ``callback(key, local_path)`` as each download completes.
    """
    captured: list[tuple[str, object]] = []
    mock_manager = MagicMock()
    mock_manager.add_download.side_effect = (
        lambda rf, dest, key=None, callback=None, **kw: captured.append((key, callback))
    )

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
    return captured


def _invoke_callback(repo, key, callback, content: bytes) -> None:
    """Write *content* to disk for *key* then invoke *callback(key, abs_path)*.

    Models the download manager: the file lands on disk first, then the manager
    calls the per-file callback with the repo-root-relative key and the absolute
    on-disk path.
    """
    disk_path = repo.root / key
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(content)
    callback(key, str(disk_path))


def test_download_records_state_per_callback(tmp_path):
    """Each completed file is recorded by its own per-file download callback.

    Mirrors the manager contract: ``add_download`` is given a ``key`` and a
    ``callback``; the manager calls ``callback(key, local_path)`` once per
    completed file.  Driving every captured callback records every target.
    """
    md = _make_manifest_dict(
        {
            "read_1": {"filename": "r1.fastq.gz", "version_replicate": "v-cli-1"},
            "read_2": {"filename": "r2.fastq.gz", "version_replicate": "v-cli-2"},
        }
    )
    repo = _build_repo(tmp_path, md)
    expected_paths = {e.local_path for e in repo.manifest.iter_files()}

    captured = _capture_download_callbacks(tmp_path)
    assert {key for key, _ in captured} == expected_paths

    for key, callback in captured:
        _invoke_callback(repo, key, callback, b"downloaded " + key.encode())

    state = RepoState.load(tmp_path / ".geoseeq")
    assert set(state.records) == expected_paths
    for key in expected_paths:
        assert state.get(key)["size_bytes"] == len(b"downloaded " + key.encode())


def test_download_state_is_resumable_when_interrupted(tmp_path):
    """Invoking only the first callback records ONLY that file (resumability).

    If a run is interrupted mid-batch, files that already completed must still
    be tracked.  Because recording happens per file via the callback, driving
    just the first captured callback persists exactly that one record and no
    other — the rest stay absent and will be re-fetched on the next run.
    """
    md = _make_manifest_dict(
        {
            "read_1": {"filename": "r1.fastq.gz", "version_replicate": "v1"},
            "read_2": {"filename": "r2.fastq.gz", "version_replicate": "v1"},
        }
    )
    repo = _build_repo(tmp_path, md)

    captured = _capture_download_callbacks(tmp_path)
    first_key, first_callback = captured[0]
    _invoke_callback(repo, first_key, first_callback, b"only this one completed")

    state = RepoState.load(tmp_path / ".geoseeq")
    assert set(state.records) == {first_key}


def test_record_downloaded_state_helper_removed():
    """The old batch ``_record_downloaded_state`` helper is gone.

    Recording now happens per-file via the manager callback; the post-download
    batch helper must not linger as a second, divergent recording path.
    """
    import geoseeq.cli.repo as repo_cli

    assert not hasattr(repo_cli, "_record_downloaded_state")


def test_download_cores_passed_to_manager(tmp_path):
    """download --cores N is accepted and forwarded as n_parallel_downloads."""
    repo, _entry, _local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")

    mock_manager = MagicMock()
    captured: dict = {}

    def _capture(**kwargs):
        captured.update(kwargs)
        return mock_manager

    runner = CliRunner()
    with (
        patch(
            "geoseeq.upload_download_manager.GeoSeeqDownloadManager",
            side_effect=_capture,
        ),
        patch(
            "geoseeq.id_constructors.from_uuids.result_file_from_uuid",
            return_value=MagicMock(),
        ),
    ):
        result = runner.invoke(
            main,
            ["repo", "download", "--cores", "4", "--all", "--yes", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert captured["n_parallel_downloads"] == 4


def test_download_cores_defaults_to_one(tmp_path):
    """Without --cores the download manager still receives n_parallel_downloads=1."""
    repo, _entry, _local_path = _single_file_repo(tmp_path, "file.fastq.gz", "v1")

    mock_manager = MagicMock()
    captured: dict = {}

    def _capture(**kwargs):
        captured.update(kwargs)
        return mock_manager

    runner = CliRunner()
    with (
        patch(
            "geoseeq.upload_download_manager.GeoSeeqDownloadManager",
            side_effect=_capture,
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
    assert captured["n_parallel_downloads"] == 1


# ---------------------------------------------------------------------------
# cli.utils helpers
# ---------------------------------------------------------------------------


def test_format_timestamp_parses_iso8601():
    """format_timestamp renders an ISO-8601 'Z' timestamp as 'YYYY-MM-DD HH:MM:SS'."""
    from geoseeq.cli.utils import format_timestamp

    assert format_timestamp("2026-05-20T14:32:00Z") == "2026-05-20 14:32:00"


def test_format_timestamp_returns_raw_on_failure():
    """format_timestamp returns the input unchanged when it cannot be parsed."""
    from geoseeq.cli.utils import format_timestamp

    assert format_timestamp("not-a-date") == "not-a-date"
    assert format_timestamp("") == ""


def test_human_size_scales_units():
    """human_size scales bytes through B/KB/MB and formats to one decimal place."""
    from geoseeq.cli.utils import human_size

    assert human_size(0) == "0.0 B"
    assert human_size(512) == "512.0 B"
    assert human_size(1024) == "1.0 KB"
    assert human_size(1024 * 1024) == "1.0 MB"


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

    captured = {}

    def _fake_git_clone(remote_url, geoseeq_dir, token, server_url):
        captured["remote_url"] = remote_url
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
    # The manifest routes are mounted at /api/projects/... (no /v1 prefix).
    assert (
        config.git_remote_url
        == "https://backend.geoseeq.com/api/projects/proj-uuid-9999/git"
    )
    # clone() must pass the same (no-/v1) remote URL to git_clone.
    assert (
        captured["remote_url"]
        == "https://backend.geoseeq.com/api/projects/proj-uuid-9999/git"
    )
    assert repo.config.server_url == "https://backend.geoseeq.com"


def test_clone_gitignores_state_json(tmp_path):
    """clone() gitignores the client-private config.json, state.json and lock file."""
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
    assert ".state.lock" in gitignore


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
