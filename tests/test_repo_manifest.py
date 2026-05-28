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
from geoseeq.cli.repo import _scrub_token
from geoseeq.constants import READS_MODULE_NAMES
from geoseeq.fastq import classify_fastq_field
from geoseeq.repo import (
    GeoSeeqRepo,
    Manifest,
    ManifestFile,
    ManifestFileEntry,
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

def _stored_data(filename: str) -> dict:
    """Build a realistic server-style stored_data descriptor for *filename*."""
    return {
        "__type__": "s3",
        "uri": f"s3://bucket/{filename}",
        "endpoint_url": "https://s3.amazonaws.com",
    }


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
                "raw_reads": {
                    "uuid": "folder-uuid-1",
                    "files": {
                        "read_1": {
                            "uuid": "file-uuid-r1",
                            "checksum": "md5:abc123",
                            "size_bytes": 1234567890,
                            "stored_data": _stored_data("Sample1_R1.fastq.gz"),
                            "version_replicate": "v1",
                        },
                        "read_2": {
                            "uuid": "file-uuid-r2",
                            "checksum": "md5:def456",
                            "size_bytes": 1234567891,
                            "stored_data": _stored_data("Sample1_R2.fastq.gz"),
                            "version_replicate": "v1",
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
        "checksum": "md5:abc123",
        "size_bytes": 1234567890,
        "stored_data": _stored_data("Sample1_R1.fastq.gz"),
    }
    f = ManifestFile.from_dict(data)
    assert f.uuid == "file-uuid-r1"
    assert f.checksum == "md5:abc123"
    assert f.size_bytes == 1234567890
    assert f.stored_data["uri"] == "s3://bucket/Sample1_R1.fastq.gz"


def test_manifest_file_roundtrip():
    """ManifestFile to_dict / from_dict is a lossless round-trip."""
    original = {
        "uuid": "u1",
        "checksum": "md5:zzz",
        "size_bytes": 999,
        "stored_data": _stored_data("c.gz"),
        "version_replicate": "v2",
    }
    assert ManifestFile.from_dict(original).to_dict() == original


def test_manifest_file_version_replicate_roundtrips():
    """version_replicate survives a from_dict / to_dict round-trip."""
    data = {
        "uuid": "u1",
        "checksum": "md5:zzz",
        "size_bytes": 1,
        "stored_data": _stored_data("c.gz"),
        "version_replicate": "rep-7",
    }
    f = ManifestFile.from_dict(data)
    assert f.version_replicate == "rep-7"
    assert f.to_dict()["version_replicate"] == "rep-7"


def test_manifest_file_version_replicate_defaults_empty_when_absent():
    """A manifest file with no version_replicate defaults it to '' (back-compat)."""
    data = {
        "uuid": "u1",
        "checksum": "md5:zzz",
        "size_bytes": 1,
        "stored_data": _stored_data("c.gz"),
    }
    f = ManifestFile.from_dict(data)
    assert f.version_replicate == ""
    assert f.to_dict()["version_replicate"] == ""


def test_manifest_file_from_dict_ignores_unknown_keys():
    """ManifestFile.from_dict ignores legacy keys like brn/local_path."""
    data = {
        "uuid": "u1",
        "checksum": "md5:zzz",
        "size_bytes": 999,
        "stored_data": _stored_data("c.gz"),
        "brn": "brn:legacy",
        "local_path": "legacy/path/c.gz",
    }
    f = ManifestFile.from_dict(data)
    assert f.uuid == "u1"
    assert not hasattr(f, "brn")
    assert not hasattr(f, "local_path")


def test_manifest_file_filename_derives_from_uri():
    """ManifestFile.filename is the basename of the stored cloud URI."""
    f = ManifestFile.from_dict(
        {
            "uuid": "u1",
            "checksum": "md5:zzz",
            "size_bytes": 1,
            "stored_data": {"uri": "s3://bucket/path/to/Sample1_R1.fastq.gz"},
        }
    )
    assert f.filename == "Sample1_R1.fastq.gz"


def test_manifest_file_filename_empty_when_no_uri():
    """ManifestFile.filename is empty when stored_data has no uri."""
    f = ManifestFile(uuid="u1", checksum="c", size_bytes=1, stored_data={})
    assert f.filename == ""


# ---------------------------------------------------------------------------
# ManifestResultFolder tests
# ---------------------------------------------------------------------------


def test_manifest_result_folder_from_dict():
    """ManifestResultFolder deserializes files correctly."""
    data = {
        "uuid": "folder-uuid-1",
        "files": {
            "read_1": {
                "uuid": "file-uuid-r1",
                "checksum": "md5:abc123",
                "size_bytes": 100,
                "stored_data": _stored_data("Sample1_R1.fastq.gz"),
            }
        },
    }
    rf = ManifestResultFolder.from_dict(data)
    assert rf.uuid == "folder-uuid-1"
    assert "read_1" in rf.files
    assert isinstance(rf.files["read_1"], ManifestFile)


# ---------------------------------------------------------------------------
# ManifestSample tests
# ---------------------------------------------------------------------------


def test_manifest_sample_from_dict():
    """ManifestSample deserializes result_folders and metadata."""
    data = SAMPLE_MANIFEST_DICT["samples"]["Sample1"]
    s = ManifestSample.from_dict(data)
    assert s.uuid == "sample-uuid-1"
    assert s.metadata == {"location": "NYC"}
    assert "raw_reads" in s.result_folders
    assert isinstance(s.result_folders["raw_reads"], ManifestResultFolder)


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


# ---------------------------------------------------------------------------
# Manifest.iter_files tests
# ---------------------------------------------------------------------------


def test_iter_files_derives_sample_paths():
    """iter_files yields sample files with samples/<sample>/<module>/<filename>."""
    m = Manifest.from_dict(SAMPLE_MANIFEST_DICT)
    entries = list(m.iter_files())

    assert len(entries) == 2
    by_field = {e.field_name: e for e in entries}
    assert by_field["read_1"].sample_name == "Sample1"
    assert by_field["read_1"].module_name == "raw_reads"
    assert by_field["read_1"].local_path == "samples/Sample1/raw_reads/Sample1_R1.fastq.gz"
    assert by_field["read_2"].local_path == "samples/Sample1/raw_reads/Sample1_R2.fastq.gz"
    assert all(isinstance(e, ManifestFileEntry) for e in entries)


def _project_results_manifest_dict() -> dict:
    """Return a manifest dict carrying a single project-level result file."""
    return {
        "version": 1,
        "project_uuid": "p",
        "project_name": "O/P",
        "server_url": "https://x.com",
        "samples": {},
        "project_results": {
            "summary": {
                "uuid": "pr-folder-1",
                "files": {
                    "report": {
                        "uuid": "pr-file-1",
                        "checksum": "md5:rrr",
                        "size_bytes": 10,
                        "stored_data": _stored_data("report.html"),
                        "version_replicate": "",
                    }
                },
            }
        },
    }


def test_iter_files_derives_project_result_paths():
    """iter_files yields project-level files under project_results/<module>/."""
    m = Manifest.from_dict(_project_results_manifest_dict())
    entries = list(m.iter_files())

    assert len(entries) == 1
    entry = entries[0]
    assert entry.sample_name is None
    assert entry.module_name == "summary"
    assert entry.local_path == "project_results/summary/report.html"


def test_manifest_project_results_roundtrip():
    """Manifest with project_results round-trips losslessly via to_dict."""
    data = _project_results_manifest_dict()
    assert Manifest.from_dict(data).to_dict() == data


def test_iter_files_skips_sample_entry_without_uri():
    """A sample result field whose stored_data lacks a uri is omitted by iter_files.

    Such inline/non-file fields are not downloadable and must not yield a
    broken, filename-less local path like ``samples/Sample1/raw_reads/``.
    """
    data = {
        "version": 1,
        "project_uuid": "p",
        "project_name": "O/P",
        "server_url": "https://x.com",
        "samples": {
            "Sample1": {
                "uuid": "sample-uuid-1",
                "metadata": {},
                "result_folders": {
                    "raw_reads": {
                        "uuid": "folder-uuid-1",
                        "files": {
                            # downloadable file (has a cloud uri)
                            "read_1": {
                                "uuid": "file-uuid-r1",
                                "checksum": "md5:abc",
                                "size_bytes": 1,
                                "stored_data": _stored_data("Sample1_R1.fastq.gz"),
                            },
                            # inline metric: stored_data present but no uri
                            "metric": {
                                "uuid": "file-uuid-m",
                                "checksum": "md5:m",
                                "size_bytes": 1,
                                "stored_data": {"__type__": "inline", "value": 0.9},
                            },
                            # empty stored_data: also no uri
                            "empty": {
                                "uuid": "file-uuid-e",
                                "checksum": "md5:e",
                                "size_bytes": 0,
                                "stored_data": {},
                            },
                        },
                    }
                },
            }
        },
        "project_results": {},
    }
    m = Manifest.from_dict(data)
    entries = list(m.iter_files())

    # Only the field with a cloud uri is yielded.
    assert len(entries) == 1
    assert entries[0].field_name == "read_1"
    assert entries[0].local_path == "samples/Sample1/raw_reads/Sample1_R1.fastq.gz"
    # No entry has a directory-like (filename-less) local path.
    assert all(not e.local_path.endswith("/") for e in entries)


def test_iter_files_skips_project_entry_without_uri():
    """A project-level result field lacking a uri is omitted by iter_files."""
    data = {
        "version": 1,
        "project_uuid": "p",
        "project_name": "O/P",
        "server_url": "https://x.com",
        "samples": {},
        "project_results": {
            "summary": {
                "uuid": "pr-folder-1",
                "files": {
                    "report": {
                        "uuid": "pr-file-1",
                        "checksum": "md5:rrr",
                        "size_bytes": 10,
                        "stored_data": _stored_data("report.html"),
                    },
                    "inline_stat": {
                        "uuid": "pr-file-2",
                        "checksum": "md5:s",
                        "size_bytes": 1,
                        "stored_data": {"__type__": "inline", "value": 1},
                    },
                },
            }
        },
    }
    m = Manifest.from_dict(data)
    entries = list(m.iter_files())

    assert len(entries) == 1
    assert entries[0].field_name == "report"
    assert entries[0].local_path == "project_results/summary/report.html"


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
# classify_fastq_field tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field_name,expected",
    [
        ("read_1", (1, 1)),
        ("read_2", (2, 1)),
        ("paired_end::read_2::lane_3", (2, 3)),
        ("paired_end::read_1::lane_10", (1, 10)),
        ("reads", (1, 1)),  # single-end / no pair token -> read 1, lane 1
        ("single_end::reads", (1, 1)),
    ],
)
def test_classify_fastq_field(field_name, expected):
    """classify_fastq_field reads pair/lane from the server-style field name."""
    assert classify_fastq_field(field_name) == expected


# ---------------------------------------------------------------------------
# READS_MODULE_NAMES tests
# ---------------------------------------------------------------------------


def test_reads_module_names_includes_server_and_legacy_names():
    """READS_MODULE_NAMES carries the server reads folders plus legacy names."""
    assert "raw::single_short_reads" in READS_MODULE_NAMES
    assert "short_read::paired_end" in READS_MODULE_NAMES
    assert "reads" in READS_MODULE_NAMES
    assert "raw_reads" in READS_MODULE_NAMES


def test_reads_module_names_excludes_fasta():
    """genome::fasta is not a reads folder and must not be in READS_MODULE_NAMES."""
    assert "genome::fasta" not in READS_MODULE_NAMES


def test_write_pipeline_configs_uses_central_reads_module_names(tmp_path):
    """A folder named raw::single_short_reads (in the central set) yields a config."""
    files = {"reads": _read_file("u1", "Sample1.fastq.gz", "md5:single")}
    repo = _repo_with_manifest(
        tmp_path, _reads_manifest("raw::single_short_reads", files)
    )
    write_pipeline_configs(repo)
    assert (tmp_path / "sample_configs" / "Sample1.json").exists()


def test_write_pipeline_configs_skips_fasta_only_sample(tmp_path):
    """A sample whose only folder is genome::fasta is not treated as having reads."""
    files = {"contig": _read_file("u1", "Sample1.fasta", "md5:fasta")}
    repo = _repo_with_manifest(tmp_path, _reads_manifest("genome::fasta", files))
    write_pipeline_configs(repo)
    assert not (tmp_path / "sample_configs" / "Sample1.json").exists()


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
    assert config["reads_1"] == ["samples/Sample1/raw_reads/Sample1_R1.fastq.gz"]
    assert config["reads_2"] == ["samples/Sample1/raw_reads/Sample1_R2.fastq.gz"]
    assert config["fastq_checksum"] == "md5:abc123"
    assert config["bdx_result_dir"] == "samples/"
    assert config["geoseeq_uuid"] == "sample-uuid-1"
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


def _repo_with_manifest(tmp_path, manifest_dict) -> GeoSeeqRepo:
    """Seed a tmp geoseeq repo with the given manifest dict and return a handle."""
    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()
    RepoConfig(
        project_uuid="proj-uuid-1234",
        server_url="https://backend.geoseeq.com",
    ).save(geoseeq_dir / "config.json")
    Manifest.from_dict(manifest_dict).save(geoseeq_dir / "manifest.json")
    return GeoSeeqRepo(tmp_path)


def _reads_manifest(module_name, files):
    """Build a one-sample manifest dict with *files* under *module_name*."""
    return {
        "version": 1,
        "project_uuid": "proj-uuid-1234",
        "project_name": "TestOrg/TestProject",
        "server_url": "https://backend.geoseeq.com",
        "samples": {
            "Sample1": {
                "uuid": "sample-uuid-1",
                "metadata": {},
                "result_folders": {
                    module_name: {"uuid": "folder-uuid-1", "files": files}
                },
            }
        },
        "project_results": {},
    }


def _read_file(uuid, filename, checksum="md5:x"):
    """Build a single manifest file entry dict."""
    return {
        "uuid": uuid,
        "checksum": checksum,
        "size_bytes": 1,
        "stored_data": _stored_data(filename),
    }


def test_write_pipeline_configs_orders_reads_by_lane(tmp_path):
    """reads_1/reads_2 are ordered by lane number derived from the field name."""
    files = {
        "paired_end::read_1::lane_2": _read_file("u1", "S1_L2_R1.fastq.gz"),
        "paired_end::read_2::lane_2": _read_file("u2", "S1_L2_R2.fastq.gz"),
        "paired_end::read_1::lane_1": _read_file("u3", "S1_L1_R1.fastq.gz", "md5:lane1"),
        "paired_end::read_2::lane_1": _read_file("u4", "S1_L1_R2.fastq.gz"),
    }
    repo = _repo_with_manifest(tmp_path, _reads_manifest("short_read::paired_end", files))
    write_pipeline_configs(repo)

    with open(tmp_path / "sample_configs" / "Sample1.json") as fh:
        config = json.loads(fh.read())

    base = "samples/Sample1/short_read::paired_end"
    assert config["reads_1"] == [f"{base}/S1_L1_R1.fastq.gz", f"{base}/S1_L2_R1.fastq.gz"]
    assert config["reads_2"] == [f"{base}/S1_L1_R2.fastq.gz", f"{base}/S1_L2_R2.fastq.gz"]
    # checksum comes from the first read_1 file (lane 1)
    assert config["fastq_checksum"] == "md5:lane1"


def test_write_pipeline_configs_lane_ordering_is_numeric_not_lexical(tmp_path):
    """Lane ordering is numeric (lane_2 < lane_10), not lexical (lane_10 < lane_2).

    Lexical sort would place lane_10 before lane_2 because '1' < '2', reversing
    the intended physical lane order. The implementation uses int() on the lane
    number, so lane_10 must sort after lane_2.
    """
    files = {
        "paired_end::read_1::lane_10": _read_file("u1", "S1_L10_R1.fastq.gz"),
        "paired_end::read_1::lane_2": _read_file("u2", "S1_L2_R1.fastq.gz", "md5:lane2"),
        "paired_end::read_1::lane_1": _read_file("u3", "S1_L1_R1.fastq.gz", "md5:lane1"),
    }
    repo = _repo_with_manifest(tmp_path, _reads_manifest("short_read::paired_end", files))
    write_pipeline_configs(repo)

    with open(tmp_path / "sample_configs" / "Sample1.json") as fh:
        config = json.loads(fh.read())

    base = "samples/Sample1/short_read::paired_end"
    assert config["reads_1"] == [
        f"{base}/S1_L1_R1.fastq.gz",
        f"{base}/S1_L2_R1.fastq.gz",
        f"{base}/S1_L10_R1.fastq.gz",
    ], "lane_10 must sort after lane_2 (numeric, not lexical)"
    # checksum comes from the first read_1 file (lane 1)
    assert config["fastq_checksum"] == "md5:lane1"


def test_write_pipeline_configs_single_end_has_empty_reads_2(tmp_path):
    """A single-end field (no read_2 token) is treated as read 1, reads_2 empty."""
    files = {"reads": _read_file("u1", "Sample1.fastq.gz", "md5:single")}
    repo = _repo_with_manifest(
        tmp_path, _reads_manifest("raw::single_short_reads", files)
    )
    write_pipeline_configs(repo)

    with open(tmp_path / "sample_configs" / "Sample1.json") as fh:
        config = json.loads(fh.read())

    assert config["reads_1"] == [
        "samples/Sample1/raw::single_short_reads/Sample1.fastq.gz"
    ]
    assert config["reads_2"] == []
    assert config["fastq_checksum"] == "md5:single"


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


def test_scrub_token_removes_credential_from_url():
    """_scrub_token replaces x:<token>@ with x:***@ in error strings."""
    token = "secret123"
    text = "fatal: repository 'https://x:secret123@host/repo.git/' not found"
    scrubbed = _scrub_token(text, token)
    assert "secret123" not in scrubbed
    assert "x:***@" in scrubbed


def test_scrub_token_noop_when_token_is_none():
    """_scrub_token is a no-op when no token is provided."""
    text = "some error text"
    assert _scrub_token(text, None) == text


def test_clone_git_error_does_not_leak_token(tmp_path):
    """clone surfaces git errors without including the API token."""
    clone_path = tmp_path / "TestProject"
    runner = CliRunner()

    with (
        patch(
            "geoseeq.cli.repo.handle_project_id",
            return_value=_make_fake_project(),
        ),
        patch(
            "geoseeq.cli.repo._build_authenticated_url",
            return_value="https://x:secret-token@host/repo.git",
        ),
        patch("subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="fatal: repository 'https://x:secret-token@host/repo.git' not found",
        )
        result = runner.invoke(
            main,
            ["repo", "clone", "TestOrg/TestProject", str(clone_path)],
            env={"GEOSEEQ_API_TOKEN": "secret-token"},
        )

    assert result.exit_code != 0
    assert "secret-token" not in result.output


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


def test_clone_config_omits_legacy_keys(tmp_path):
    """clone writes config.json with no auth_profile/git_remote_url keys."""
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
    with open(clone_path / ".geoseeq" / "config.json") as fh:
        raw = json.loads(fh.read())
    assert set(raw.keys()) == {"project_uuid", "server_url"}
    assert "auth_profile" not in raw
    assert "git_remote_url" not in raw


def test_repo_config_git_remote_url_is_computed():
    """RepoConfig.git_remote_url is derived from server_url and project_uuid."""
    config = RepoConfig(
        project_uuid="proj-uuid-1234",
        server_url="https://backend.geoseeq.com/",
    )
    assert (
        config.git_remote_url
        == "https://backend.geoseeq.com/api/v1/projects/proj-uuid-1234/git"
    )


def test_repo_config_from_dict_ignores_legacy_keys():
    """RepoConfig.from_dict ignores legacy auth_profile/git_remote_url keys."""
    config = RepoConfig.from_dict(
        {
            "project_uuid": "p",
            "server_url": "https://x.com",
            "auth_profile": "myprofile",
            "git_remote_url": "https://x.com/stale",
        }
    )
    assert config.project_uuid == "p"
    assert config.server_url == "https://x.com"
    assert not hasattr(config, "auth_profile")
    # git_remote_url is recomputed, never the stale persisted value
    assert config.git_remote_url == "https://x.com/api/v1/projects/p/git"


def test_ensure_config_gitignored_appends_to_existing_gitignore(tmp_path):
    """_ensure_config_gitignored appends config.json when .gitignore already exists."""
    from geoseeq.cli.repo import _ensure_config_gitignored

    geoseeq_dir = tmp_path / ".geoseeq"
    geoseeq_dir.mkdir()
    gitignore_path = geoseeq_dir / ".gitignore"
    gitignore_path.write_text("*.pyc\n")  # existing entries, no config.json

    _ensure_config_gitignored(geoseeq_dir)

    content = gitignore_path.read_text()
    assert "config.json" in content
    assert "*.pyc" in content  # original entry preserved


def test_repo_help_lists_clone_subcommand():
    """geoseeq repo --help lists clone as a subcommand (smoke test)."""
    runner = CliRunner()
    result = runner.invoke(main, ["repo", "--help"])
    assert result.exit_code == 0
    assert "clone" in result.output
