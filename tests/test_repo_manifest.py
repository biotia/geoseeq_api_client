"""Unit tests for geoseeq.repo: manifest, config, GeoSeeqRepo, pipeline configs, and clone CLI."""
from __future__ import annotations

import json
import subprocess
import tempfile
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
    NonFastForwardError,
    NotARepoError,
    RepoConfig,
)
from geoseeq.repo.pipeline_config import write_pipeline_configs


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

SAMPLE_MANIFEST_DICT = {
    "version": 1,
    "project_uuid": "proj-uuid-1234",
    "project_name": "TestOrg/TestProject",
    "server_url": "https://backend.geoseeq.com",
    "samples": {
        "Sample1": {
            "uuid": "sample-uuid-1",
            "metadata": {"location": "NYC"},
            "result_folders": {
                "reads": {
                    "uuid": "folder-uuid-1",
                    "files": {
                        "Sample1_R1.fastq.gz": {
                            "uuid": "file-uuid-r1",
                            "brn": "brn:geoseeq:file:file-uuid-r1",
                            "checksum": "md5:abc123",
                            "size_bytes": 1234567890,
                            "local_path": "samples/Sample1/reads/Sample1_R1.fastq.gz",
                        },
                        "Sample1_R2.fastq.gz": {
                            "uuid": "file-uuid-r2",
                            "brn": "brn:geoseeq:file:file-uuid-r2",
                            "checksum": "md5:def456",
                            "size_bytes": 1234567891,
                            "local_path": "samples/Sample1/reads/Sample1_R2.fastq.gz",
                        },
                    },
                }
            },
        },
        "Sample2": {
            "uuid": "sample-uuid-2",
            "metadata": {},
            "result_folders": {},
        },
    },
    "project_results": {},
}


@pytest.fixture
def manifest_dict():
    """Return a canonical sample manifest dictionary."""
    return SAMPLE_MANIFEST_DICT.copy()


@pytest.fixture
def tmp_path_with_repo(tmp_path):
    """Create a minimal fake geoseeq repo structure under tmp_path."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()

    config = RepoConfig(
        project_uuid="proj-uuid-1234",
        server_url="https://backend.geoseeq.com",
        auth_profile="default",
        git_remote_url="https://backend.geoseeq.com/api/v1/projects/proj-uuid-1234/git",
    )
    config.save(geoseeq_dir / "config.json")

    manifest = Manifest.from_dict(SAMPLE_MANIFEST_DICT)
    manifest.save(geoseeq_dir / "manifest.json")

    return tmp_path


# ---------------------------------------------------------------------------
# ManifestFile tests
# ---------------------------------------------------------------------------


def test_manifest_file_from_dict():
    """ManifestFile deserializes correctly from a dict."""
    data = {
        "uuid": "file-uuid-r1",
        "brn": "brn:geoseeq:file:file-uuid-r1",
        "checksum": "md5:abc123",
        "size_bytes": 1234567890,
        "local_path": "samples/Sample1/reads/Sample1_R1.fastq.gz",
    }
    f = ManifestFile.from_dict(data)
    assert f.uuid == "file-uuid-r1"
    assert f.checksum == "md5:abc123"
    assert f.size_bytes == 1234567890
    assert f.local_path == "samples/Sample1/reads/Sample1_R1.fastq.gz"


def test_manifest_file_roundtrip():
    """ManifestFile to_dict / from_dict is a lossless round-trip."""
    original = {
        "uuid": "u1",
        "brn": "brn:x",
        "checksum": "md5:zzz",
        "size_bytes": 999,
        "local_path": "a/b/c.gz",
    }
    assert ManifestFile.from_dict(original).to_dict() == original


# ---------------------------------------------------------------------------
# ManifestResultFolder tests
# ---------------------------------------------------------------------------


def test_manifest_result_folder_from_dict():
    """ManifestResultFolder deserializes files correctly."""
    data = {
        "uuid": "folder-uuid-1",
        "files": {
            "Sample1_R1.fastq.gz": {
                "uuid": "file-uuid-r1",
                "brn": "brn:geoseeq:file:file-uuid-r1",
                "checksum": "md5:abc123",
                "size_bytes": 100,
                "local_path": "samples/Sample1/reads/Sample1_R1.fastq.gz",
            }
        },
    }
    rf = ManifestResultFolder.from_dict(data)
    assert rf.uuid == "folder-uuid-1"
    assert "Sample1_R1.fastq.gz" in rf.files
    assert isinstance(rf.files["Sample1_R1.fastq.gz"], ManifestFile)


# ---------------------------------------------------------------------------
# ManifestSample tests
# ---------------------------------------------------------------------------


def test_manifest_sample_from_dict():
    """ManifestSample deserializes result_folders and metadata."""
    data = SAMPLE_MANIFEST_DICT["samples"]["Sample1"]
    s = ManifestSample.from_dict(data)
    assert s.uuid == "sample-uuid-1"
    assert s.metadata == {"location": "NYC"}
    assert "reads" in s.result_folders
    assert isinstance(s.result_folders["reads"], ManifestResultFolder)


# ---------------------------------------------------------------------------
# Manifest load / save round-trip tests
# ---------------------------------------------------------------------------


def test_manifest_load_save_roundtrip(tmp_path, manifest_dict):
    """Manifest.load and manifest.save are lossless and produce no manifest_etag."""
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as fh:
        fh.write(json.dumps(manifest_dict))

    loaded = Manifest.load(manifest_path)
    assert loaded.version == 1
    assert loaded.project_uuid == "proj-uuid-1234"
    assert loaded.project_name == "TestOrg/TestProject"
    assert loaded.server_url == "https://backend.geoseeq.com"
    assert "Sample1" in loaded.samples
    assert "Sample2" in loaded.samples

    out_path = tmp_path / "manifest_out.json"
    loaded.save(out_path)
    with open(out_path) as fh:
        saved_dict = json.loads(fh.read())

    # No manifest_etag field should ever appear
    assert "manifest_etag" not in saved_dict
    # Content matches original
    assert saved_dict == manifest_dict


def test_manifest_from_dict_no_etag(manifest_dict):
    """Manifest.from_dict / to_dict never includes manifest_etag."""
    m = Manifest.from_dict(manifest_dict)
    out = m.to_dict()
    assert "manifest_etag" not in out


def test_manifest_empty_samples():
    """Manifest with no samples round-trips cleanly."""
    data = {
        "version": 1,
        "project_uuid": "p",
        "project_name": "O/P",
        "server_url": "https://x.com",
        "samples": {},
        "project_results": {},
    }
    m = Manifest.from_dict(data)
    assert m.samples == {}
    assert m.to_dict() == data


# ---------------------------------------------------------------------------
# GeoSeeqRepo.find() tests
# ---------------------------------------------------------------------------


def test_geoseeq_repo_find_from_root(tmp_path_with_repo):
    """GeoSeeqRepo.find() locates the repo root when called from the root itself."""
    repo = GeoSeeqRepo.find(tmp_path_with_repo)
    assert repo.root == tmp_path_with_repo


def test_geoseeq_repo_find_from_subdirectory(tmp_path_with_repo):
    """GeoSeeqRepo.find() walks up and finds the repo from a subdirectory."""
    subdir = tmp_path_with_repo / "samples" / "Sample1"
    subdir.mkdir(parents=True)
    repo = GeoSeeqRepo.find(subdir)
    assert repo.root == tmp_path_with_repo


def test_geoseeq_repo_find_raises_outside_repo(tmp_path):
    """GeoSeeqRepo.find() raises NotARepoError when not inside a repo."""
    with pytest.raises(NotARepoError):
        GeoSeeqRepo.find(tmp_path)


def test_geoseeq_repo_manifest_lazy_load(tmp_path_with_repo):
    """GeoSeeqRepo.manifest loads lazily and has correct project name."""
    repo = GeoSeeqRepo(tmp_path_with_repo)
    assert repo._manifest is None
    manifest = repo.manifest
    assert manifest.project_name == "TestOrg/TestProject"
    assert repo._manifest is manifest  # cached


def test_geoseeq_repo_config_lazy_load(tmp_path_with_repo):
    """GeoSeeqRepo.config loads lazily and has correct project UUID."""
    repo = GeoSeeqRepo(tmp_path_with_repo)
    assert repo._config is None
    config = repo.config
    assert config.project_uuid == "proj-uuid-1234"
    assert repo._config is config  # cached


# ---------------------------------------------------------------------------
# GeoSeeqRepo git operations tests (subprocess mocked)
# ---------------------------------------------------------------------------


def test_geoseeq_repo_commit(tmp_path_with_repo):
    """GeoSeeqRepo.commit() calls git add and git commit."""
    repo = GeoSeeqRepo(tmp_path_with_repo)
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        repo.commit("test commit message")

    calls = mock_run.call_args_list
    assert len(calls) == 2
    assert "add" in calls[0][0][0]
    assert "manifest.json" in calls[0][0][0]
    assert "commit" in calls[1][0][0]
    assert "test commit message" in calls[1][0][0]


def test_geoseeq_repo_git_push_success(tmp_path_with_repo):
    """GeoSeeqRepo.git_push() succeeds when git returns 0."""
    repo = GeoSeeqRepo(tmp_path_with_repo)
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        repo.git_push()  # should not raise


def test_geoseeq_repo_git_push_non_fast_forward(tmp_path_with_repo):
    """GeoSeeqRepo.git_push() raises NonFastForwardError on rejected push."""
    repo = GeoSeeqRepo(tmp_path_with_repo)
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="error: failed to push some refs (non-fast-forward)",
        )
        with pytest.raises(NonFastForwardError):
            repo.git_push()


def test_geoseeq_repo_git_pull(tmp_path_with_repo):
    """GeoSeeqRepo.git_pull() calls git pull --rebase."""
    repo = GeoSeeqRepo(tmp_path_with_repo)
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        repo.git_pull()

    args = mock_run.call_args[0][0]
    assert "pull" in args
    assert "--rebase" in args


# ---------------------------------------------------------------------------
# write_pipeline_configs tests
# ---------------------------------------------------------------------------


def test_write_pipeline_configs_produces_correct_json(tmp_path_with_repo):
    """write_pipeline_configs writes a JSON config for samples with reads."""
    repo = GeoSeeqRepo(tmp_path_with_repo)
    write_pipeline_configs(repo)

    config_path = tmp_path_with_repo / "sample_configs" / "Sample1.json"
    assert config_path.exists()

    with open(config_path) as fh:
        config = json.loads(fh.read())

    assert config["sample_name"] == "Sample1"
    assert config["reads_1"] == "samples/Sample1/reads/Sample1_R1.fastq.gz"
    assert config["reads_2"] == "samples/Sample1/reads/Sample1_R2.fastq.gz"
    assert config["fastq_checksum"] == "md5:abc123"
    assert config["bdx_result_dir"] == "samples/"
    assert config["geoseeq_uuid"] == "folder-uuid-1"
    assert config["geoseeq_endpoint"] == "https://backend.geoseeq.com"
    assert config["metadata"] == {"location": "NYC"}


def test_write_pipeline_configs_skips_samples_without_reads(tmp_path_with_repo):
    """write_pipeline_configs does not write a config for Sample2 (no reads folder)."""
    repo = GeoSeeqRepo(tmp_path_with_repo)
    write_pipeline_configs(repo)

    config_path = tmp_path_with_repo / "sample_configs" / "Sample2.json"
    assert not config_path.exists()


def test_write_pipeline_configs_creates_dir(tmp_path_with_repo):
    """write_pipeline_configs creates sample_configs/ if it doesn't exist."""
    config_dir = tmp_path_with_repo / "sample_configs"
    if config_dir.exists():
        import shutil
        shutil.rmtree(config_dir)

    repo = GeoSeeqRepo(tmp_path_with_repo)
    write_pipeline_configs(repo)
    assert config_dir.exists()


# ---------------------------------------------------------------------------
# CLI clone command (API and git subprocess mocked)
# ---------------------------------------------------------------------------


def _make_fake_project(uuid="proj-uuid-1234"):
    """Return a minimal mock project object."""
    proj = MagicMock()
    proj.uuid = uuid
    proj.name = "TestOrg/TestProject"
    return proj


def _build_mock_geoseeq_dir(clone_path: Path) -> None:
    """Seed .geoseeq/ with a minimal manifest so write_pipeline_configs works."""
    geoseeq_dir = clone_path / ".geoseeq"
    geoseeq_dir.mkdir(parents=True, exist_ok=True)

    manifest = Manifest.from_dict(SAMPLE_MANIFEST_DICT)
    manifest.save(geoseeq_dir / "manifest.json")


def _invoke_clone(runner, extra_args, env=None):
    """Invoke the clone command with a fake API token in the environment."""
    env = env or {}
    env.setdefault("GEOSEEQ_API_TOKEN", "fake-token")
    return runner.invoke(
        main,
        ["repo", "clone"] + extra_args,
        env=env,
        catch_exceptions=False,
    )


def test_clone_creates_directory_structure(tmp_path):
    """clone creates the expected directory layout and sample_configs."""
    clone_path = tmp_path / "TestProject"
    runner = CliRunner()

    with (
        patch(
            "geoseeq.cli.repo.handle_project_id",
            return_value=_make_fake_project(),
        ),
        patch(
            "geoseeq.cli.repo._git_clone",
            side_effect=lambda remote_url, geoseeq_dir, token, server_url: _build_mock_geoseeq_dir(
                geoseeq_dir.parent
            ),
        ),
    ):
        result = _invoke_clone(runner, ["TestOrg/TestProject", str(clone_path)])

    assert result.exit_code == 0, result.output
    assert clone_path.is_dir()
    assert (clone_path / "samples").is_dir()
    assert (clone_path / "project_results").is_dir()
    assert (clone_path / "sample_configs").is_dir()
    assert (clone_path / "sample_configs" / "Sample1.json").exists()
    assert not (clone_path / "sample_configs" / "Sample2.json").exists()


def test_clone_writes_config_json(tmp_path):
    """clone writes config.json into .geoseeq/."""
    clone_path = tmp_path / "TestProject"
    runner = CliRunner()

    with (
        patch(
            "geoseeq.cli.repo.handle_project_id",
            return_value=_make_fake_project(),
        ),
        patch(
            "geoseeq.cli.repo._git_clone",
            side_effect=lambda remote_url, geoseeq_dir, token, server_url: _build_mock_geoseeq_dir(
                geoseeq_dir.parent
            ),
        ),
    ):
        result = _invoke_clone(runner, ["TestOrg/TestProject", str(clone_path)])

    assert result.exit_code == 0, result.output
    config_path = clone_path / ".geoseeq" / "config.json"
    assert config_path.exists()

    config = RepoConfig.load(config_path)
    assert config.project_uuid == "proj-uuid-1234"


def test_clone_gitignores_config_json(tmp_path):
    """clone ensures config.json appears in .geoseeq/.gitignore."""
    clone_path = tmp_path / "TestProject"
    runner = CliRunner()

    with (
        patch(
            "geoseeq.cli.repo.handle_project_id",
            return_value=_make_fake_project(),
        ),
        patch(
            "geoseeq.cli.repo._git_clone",
            side_effect=lambda remote_url, geoseeq_dir, token, server_url: _build_mock_geoseeq_dir(
                geoseeq_dir.parent
            ),
        ),
    ):
        result = _invoke_clone(runner, ["TestOrg/TestProject", str(clone_path)])

    assert result.exit_code == 0, result.output
    gitignore = (clone_path / ".geoseeq" / ".gitignore").read_text()
    assert "config.json" in gitignore


def test_clone_fails_if_repo_already_exists(tmp_path):
    """clone raises ClickException when .geoseeq/ already exists."""
    clone_path = tmp_path / "TestProject"
    (clone_path / ".geoseeq").mkdir(parents=True)
    runner = CliRunner()

    with patch(
        "geoseeq.cli.repo.handle_project_id",
        return_value=_make_fake_project(),
    ):
        result = runner.invoke(
            main,
            ["repo", "clone", "TestOrg/TestProject", str(clone_path)],
            env={"GEOSEEQ_API_TOKEN": "fake-token"},
        )

    assert result.exit_code != 0
    assert "already contains a geoseeq repo" in result.output


def test_clone_default_path_is_last_component(tmp_path):
    """clone uses the last component of project_name as the default path."""
    runner = CliRunner()

    with (
        runner.isolated_filesystem(temp_dir=tmp_path),
        patch(
            "geoseeq.cli.repo.handle_project_id",
            return_value=_make_fake_project(),
        ),
        patch(
            "geoseeq.cli.repo._git_clone",
            side_effect=lambda remote_url, geoseeq_dir, token, server_url: _build_mock_geoseeq_dir(
                geoseeq_dir.parent
            ),
        ),
    ):
        result = _invoke_clone(runner, ["TestOrg/TestProject"])

    assert result.exit_code == 0, result.output
    assert "TestProject" in result.output
