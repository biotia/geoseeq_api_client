"""Unit tests for repo push, new-sample, and rm (GRF-06, GRF-07).

These exercise the read-only architecture: the client uploads via the API and
``git_pull``s the server's freshly-committed manifest — it never writes or
commits the manifest itself.  Tests therefore assert state.json side effects and
the absence of any manifest.save/git-commit, and use a ``git_pull`` mock that
rewrites manifest.json the way the server would.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from geoseeq.cli.main import main
from geoseeq.repo import GeoSeeqRepo, Manifest, RepoConfig
from geoseeq.repo.state import RepoState, record_for


# ---------------------------------------------------------------------------
# Shared helpers (current manifest schema: stored_data uri + version_replicate)
# ---------------------------------------------------------------------------


def _stored_data(filename: str) -> dict:
    """Build a server-style stored_data descriptor carrying a cloud uri."""
    return {
        "__type__": "s3",
        "uri": f"s3://bucket/{filename}",
        "endpoint_url": "https://s3.amazonaws.com",
    }


def _make_manifest_dict(samples: dict[str, dict] | None = None) -> dict:
    """Build a manifest dict from a nested sample/module/field description.

    *samples* maps sample_name -> {module_name -> {field_name -> {filename,
    version_replicate}}}.  A sample mapped to an empty dict has no result
    folders (used to model a brand-new, file-less sample).
    """
    sample_data: dict[str, dict] = {}
    for s_name, modules in (samples or {}).items():
        folders = {}
        for module_name, fields in modules.items():
            file_data = {}
            for field_name, info in fields.items():
                file_data[field_name] = {
                    "uuid": f"uuid-{field_name}",
                    "checksum": {"ETag": "abc-1"},
                    "size_bytes": 100,
                    "stored_data": _stored_data(info["filename"]),
                    "version_replicate": info["version_replicate"],
                }
            folders[module_name] = {
                "uuid": f"folder-uuid-{module_name}",
                "files": file_data,
            }
        sample_data[s_name] = {
            "uuid": f"sample-uuid-{s_name}",
            "metadata": {},
            "result_folders": folders,
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
    """Create a minimal geoseeq repo under *tmp_path* and return its handle."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()

    config = RepoConfig(
        project_uuid="proj-uuid",
        server_url="https://backend.geoseeq.com",
    )
    config.save(geoseeq_dir / "config.json")

    Manifest.from_dict(manifest_dict).save(geoseeq_dir / "manifest.json")

    (tmp_path / "samples").mkdir(exist_ok=True)
    (tmp_path / "project_results").mkdir(exist_ok=True)

    return GeoSeeqRepo(tmp_path)


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


def _mock_upload_manager():
    """Return (manager_class_mock, instance_mock) for patching GeoSeeqUploadManager.

    The instance records every ``add_local_file_to_result_folder`` call as a tuple
    of (result_folder, local_path, geoseeq_file_name) on ``.added`` for assertions.
    """
    instance = MagicMock()
    instance.added = []

    def _add(result_folder, local_path, geoseeq_file_name=None):
        instance.added.append((result_folder, local_path, geoseeq_file_name))

    instance.add_local_file_to_result_folder.side_effect = _add
    manager_cls = MagicMock(return_value=instance)
    return manager_cls, instance


def _mock_sample_obj(folder_uuid: str = "folder-uuid-reads"):
    """Return a mock server sample whose result_folder(...).idem() yields a folder."""
    folder = MagicMock()
    folder.uuid = folder_uuid
    sample_obj = MagicMock()
    sample_obj.result_folder.return_value.idem.return_value = folder
    return sample_obj, folder


# ---------------------------------------------------------------------------
# push tests
# ---------------------------------------------------------------------------


def test_push_uploads_new_and_modified_local(tmp_path):
    """push queues uploads for new_local + modified_local with the right folder/name."""
    # Sample1 has one tracked (downloaded) file we will modify, plus we add a new file.
    md = _make_manifest_dict(
        {"Sample1": {"reads": {"read_1": {"filename": "old.fastq.gz", "version_replicate": "v1"}}}}
    )
    repo = _build_repo(tmp_path, md)

    tracked = "samples/Sample1/reads/old.fastq.gz"
    _write_disk_file(repo, tracked, b"original")
    _seed_state(repo, tracked, "v1")
    # Modify the tracked file so it becomes modified_local.
    _write_disk_file(repo, tracked, b"edited content")

    new_file = "samples/Sample1/reads/new.fastq.gz"
    _write_disk_file(repo, new_file, b"brand new")

    manager_cls, instance = _mock_upload_manager()
    sample_obj, _folder = _mock_sample_obj()

    with (
        patch("geoseeq.upload_download_manager.GeoSeeqUploadManager", manager_cls),
        patch("geoseeq.id_constructors.from_uuids.sample_from_uuid", return_value=sample_obj),
        patch.object(GeoSeeqRepo, "git_pull") as mock_pull,
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        pushed = repo.push(MagicMock())

    assert set(pushed) == {tracked, new_file}
    # Both files were queued against result_folder("reads").idem() with basename names.
    queued_names = {name for (_rf, _lp, name) in instance.added}
    assert queued_names == {"old.fastq.gz", "new.fastq.gz"}
    sample_obj.result_folder.assert_called_with("reads")
    sample_obj.result_folder.return_value.idem.assert_called()
    instance.upload_files.assert_called_once()
    mock_pull.assert_called_once()


def test_push_records_state_after_pull(tmp_path):
    """push records pushed files in state.json using the refreshed version_replicate."""
    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)

    new_file = "samples/Sample1/reads/new.fastq.gz"
    _write_disk_file(repo, new_file, b"payload")

    # After upload the server commits; git_pull brings down a manifest that now
    # lists the file at version_replicate "v9".
    post_md = _make_manifest_dict(
        {"Sample1": {"reads": {"read_1": {"filename": "new.fastq.gz", "version_replicate": "v9"}}}}
    )

    def _fake_pull():
        Manifest.from_dict(post_md).save(repo.root / ".geoseeq" / "manifest.json")

    manager_cls, _instance = _mock_upload_manager()
    sample_obj, _folder = _mock_sample_obj()

    with (
        patch("geoseeq.upload_download_manager.GeoSeeqUploadManager", manager_cls),
        patch("geoseeq.id_constructors.from_uuids.sample_from_uuid", return_value=sample_obj),
        patch.object(GeoSeeqRepo, "git_pull", side_effect=_fake_pull),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        repo.push(MagicMock())

    state = RepoState.load(repo.root / ".geoseeq")
    record = state.get(new_file)
    assert record is not None
    assert record["version_replicate"] == "v9"


def test_push_modified_reupload_clears_modified_and_outdated(tmp_path):
    """After re-uploading a modified file, status no longer flags it modified/outdated."""
    md = _make_manifest_dict(
        {"Sample1": {"reads": {"read_1": {"filename": "f.fastq.gz", "version_replicate": "v1"}}}}
    )
    repo = _build_repo(tmp_path, md)

    tracked = "samples/Sample1/reads/f.fastq.gz"
    _write_disk_file(repo, tracked, b"v1 content")
    _seed_state(repo, tracked, "v1")
    # Edit locally -> modified_local.
    _write_disk_file(repo, tracked, b"locally edited content")

    pre = repo.compute_status()
    assert tracked in pre.modified_local

    # Server commits a new version after the push.
    post_md = _make_manifest_dict(
        {"Sample1": {"reads": {"read_1": {"filename": "f.fastq.gz", "version_replicate": "v2"}}}}
    )

    def _fake_pull():
        Manifest.from_dict(post_md).save(repo.root / ".geoseeq" / "manifest.json")

    manager_cls, _instance = _mock_upload_manager()
    sample_obj, _folder = _mock_sample_obj()

    with (
        patch("geoseeq.upload_download_manager.GeoSeeqUploadManager", manager_cls),
        patch("geoseeq.id_constructors.from_uuids.sample_from_uuid", return_value=sample_obj),
        patch.object(GeoSeeqRepo, "git_pull", side_effect=_fake_pull),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        repo.push(MagicMock())

    post = repo.compute_status()
    assert tracked not in post.modified_local
    assert tracked not in post.outdated
    assert tracked in post.downloaded


def test_push_does_not_write_manifest_or_commit(tmp_path):
    """push never calls Manifest.save — the server is the sole commit authority."""
    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)
    _write_disk_file(repo, "samples/Sample1/reads/new.fastq.gz", b"data")

    manager_cls, _instance = _mock_upload_manager()
    sample_obj, _folder = _mock_sample_obj()

    with (
        patch("geoseeq.upload_download_manager.GeoSeeqUploadManager", manager_cls),
        patch("geoseeq.id_constructors.from_uuids.sample_from_uuid", return_value=sample_obj),
        patch.object(GeoSeeqRepo, "git_pull"),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
        patch.object(Manifest, "save") as mock_save,
    ):
        repo.push(MagicMock())

    mock_save.assert_not_called()


def test_push_sample_filter(tmp_path):
    """push --sample X only uploads files under samples/X/."""
    md = _make_manifest_dict({"Sample1": {}, "Sample2": {}})
    repo = _build_repo(tmp_path, md)
    _write_disk_file(repo, "samples/Sample1/reads/a.fastq.gz", b"a")
    _write_disk_file(repo, "samples/Sample2/reads/b.fastq.gz", b"b")

    manager_cls, _instance = _mock_upload_manager()
    sample_obj, _folder = _mock_sample_obj()

    with (
        patch("geoseeq.upload_download_manager.GeoSeeqUploadManager", manager_cls),
        patch("geoseeq.id_constructors.from_uuids.sample_from_uuid", return_value=sample_obj),
        patch.object(GeoSeeqRepo, "git_pull"),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        pushed = repo.push(MagicMock(), sample="Sample1")

    assert pushed == ["samples/Sample1/reads/a.fastq.gz"]


def test_push_nothing_returns_empty(tmp_path):
    """push returns [] (and the CLI prints 'Nothing to push.') when status is clean."""
    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)

    with patch.object(GeoSeeqRepo, "git_pull") as mock_pull:
        pushed = repo.push(MagicMock())

    assert pushed == []
    mock_pull.assert_not_called()


def test_push_skips_unparseable_new_local(tmp_path):
    """push logs+skips a top-level/unparseable new_local path instead of raising."""
    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)
    # A file directly under samples/ has only 2 path parts -> unparseable.
    _write_disk_file(repo, "samples/loose.txt", b"loose")

    manager_cls, instance = _mock_upload_manager()

    with (
        patch("geoseeq.upload_download_manager.GeoSeeqUploadManager", manager_cls),
        patch.object(GeoSeeqRepo, "git_pull") as mock_pull,
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        pushed = repo.push(MagicMock())

    assert pushed == []
    assert instance.added == []
    mock_pull.assert_not_called()


def test_push_unknown_sample_raises(tmp_path):
    """push raises a 'new-sample first' error for a sample not in the manifest."""
    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)
    # File for a sample that does not exist in the manifest.
    _write_disk_file(repo, "samples/Ghost/reads/x.fastq.gz", b"x")

    manager_cls, _instance = _mock_upload_manager()

    with (
        patch("geoseeq.upload_download_manager.GeoSeeqUploadManager", manager_cls),
        patch.object(GeoSeeqRepo, "git_pull"),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        with pytest.raises(ValueError, match="new-sample"):
            repo.push(MagicMock())


def test_push_cli_nothing_to_push(tmp_path):
    """The CLI prints 'Nothing to push.' when there is nothing to upload."""
    md = _make_manifest_dict({"Sample1": {}})
    _build_repo(tmp_path, md)

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "push", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "Nothing to push." in result.output


def test_push_cli_reports_pushed_samples(tmp_path):
    """The CLI reports the count and sample names of pushed files."""
    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)
    _write_disk_file(repo, "samples/Sample1/reads/new.fastq.gz", b"data")

    manager_cls, _instance = _mock_upload_manager()
    sample_obj, _folder = _mock_sample_obj()

    runner = CliRunner()
    with (
        patch("geoseeq.upload_download_manager.GeoSeeqUploadManager", manager_cls),
        patch("geoseeq.id_constructors.from_uuids.sample_from_uuid", return_value=sample_obj),
        patch.object(GeoSeeqRepo, "git_pull"),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
    ):
        result = runner.invoke(
            main,
            ["repo", "push", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert "Pushed 1 files for Sample1" in result.output


# ---------------------------------------------------------------------------
# new-sample tests
# ---------------------------------------------------------------------------


def test_new_sample_creates_dir_and_pulls(tmp_path):
    """new_sample creates samples/<name>/, calls git_pull, and never saves the manifest."""
    md = _make_manifest_dict({})
    repo = _build_repo(tmp_path, md)

    project = MagicMock()
    created = MagicMock()
    created.uuid = "new-sample-uuid"
    project.sample.return_value.idem.return_value = created

    with (
        patch("geoseeq.id_constructors.from_uuids.project_from_uuid", return_value=project),
        patch.object(GeoSeeqRepo, "git_pull") as mock_pull,
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
        patch.object(Manifest, "save") as mock_save,
    ):
        uuid = repo.new_sample("MySample", {}, MagicMock())

    assert uuid == "new-sample-uuid"
    assert (tmp_path / "samples" / "MySample").is_dir()
    project.sample.assert_called_once_with("MySample", metadata={})
    mock_pull.assert_called_once()
    mock_save.assert_not_called()


def test_new_sample_cli_creates_sample_dir(tmp_path):
    """The new-sample CLI creates the directory under samples/, not sample_results/."""
    md = _make_manifest_dict({})
    _build_repo(tmp_path, md)

    project = MagicMock()
    project.sample.return_value.idem.return_value.uuid = "uuid"

    runner = CliRunner()
    with (
        patch("geoseeq.id_constructors.from_uuids.project_from_uuid", return_value=project),
        patch.object(GeoSeeqRepo, "git_pull"),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
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
    assert not (tmp_path / "sample_results").exists()


def test_new_sample_cli_loads_metadata_file(tmp_path):
    """The new-sample CLI loads metadata from --metadata-file and passes it through."""
    md = _make_manifest_dict({})
    _build_repo(tmp_path, md)

    meta = {"location": "USA", "collection_date": "2024-01-01"}
    meta_file = tmp_path / "meta.json"
    meta_file.write_text(json.dumps(meta))

    project = MagicMock()
    project.sample.return_value.idem.return_value.uuid = "uuid"

    runner = CliRunner()
    with (
        patch("geoseeq.id_constructors.from_uuids.project_from_uuid", return_value=project),
        patch.object(GeoSeeqRepo, "git_pull"),
        patch.object(GeoSeeqRepo, "write_pipeline_configs"),
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
    project.sample.assert_called_once_with("MySample", metadata=meta)


# ---------------------------------------------------------------------------
# rm tests
# ---------------------------------------------------------------------------


def test_rm_clean_repo_removes_directory(tmp_path):
    """rm removes the repo root when the repo is fully synced."""
    md = _make_manifest_dict(
        {"Sample1": {"reads": {"read_1": {"filename": "f.fastq.gz", "version_replicate": "v1"}}}}
    )
    repo = _build_repo(tmp_path, md)

    tracked = "samples/Sample1/reads/f.fastq.gz"
    _write_disk_file(repo, tracked, b"data")
    _seed_state(repo, tracked, "v1")

    removed = []

    runner = CliRunner()
    with patch("geoseeq.cli.repo.shutil.rmtree", side_effect=removed.append):
        result = runner.invoke(
            main,
            ["repo", "rm", str(tmp_path)],
            env={"GEOSEEQ_API_TOKEN": "fake"},
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert removed == [tmp_path]
    assert "Removed local repo" in result.output


def test_rm_dirty_repo_raises_exact_error(tmp_path):
    """rm refuses (exact error string) and leaves the repo intact when new_local exists."""
    md = _make_manifest_dict({"Sample1": {}})
    repo = _build_repo(tmp_path, md)
    # A file not in the manifest -> new_local -> not fully synced.
    _write_disk_file(repo, "samples/Sample1/reads/unsaved.fastq.gz", b"unsaved")

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["repo", "rm", str(tmp_path)],
        env={"GEOSEEQ_API_TOKEN": "fake"},
        catch_exceptions=False,
    )

    assert result.exit_code != 0
    assert (
        "Error: project is not fully synced. "
        "Run 'geoseeq repo status' to see what's out of sync."
    ) in result.output
    assert tmp_path.exists()
