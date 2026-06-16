import warnings
from pathlib import Path

import pytest

from geoseeq.cli._grouping import group_files
from geoseeq.cli.upload.upload_reads import (
    _LINK_TYPE_S3_DEPRECATION_MSG,
    _maybe_warn_link_type_s3_deprecated,
)


class DummyKnex:
    def __init__(self, groups):
        self.groups = groups
        self.calls = []

    def post(self, endpoint, json):
        self.calls.append((endpoint, json))
        if endpoint == "bulk_upload/group_files":
            return self.groups
        raise AssertionError(f"Unexpected endpoint {endpoint}")


def test_group_files_applies_name_map(tmp_path: Path):
    name_map_file = tmp_path / "name_map.csv"
    name_map_file.write_text("current,new\nold_name,new_name\n")

    groups = [
        {"sample_name": "old_name", "fields": {"R1": "old_name_R1.fastq"}},
    ]
    knex = DummyKnex(groups)

    filepaths = {"old_name_R1.fastq": "/tmp/old_name_R1.fastq"}

    updated_groups = group_files(
        knex,
        filepaths,
        "short_read::single_end",
        regex=r"(?P<sample_name>.+)",
        yes=True,
        name_map=(name_map_file, "current", "new"),
    )

    assert updated_groups[0]["sample_name"] == "new_name"
    # Ensure the grouping endpoint was called using the provided paths.
    assert knex.calls[0][0] == "bulk_upload/group_files"
    assert filepaths.keys() == set(knex.calls[0][1]["filenames"])


def test_group_files_without_name_map():
    """group_files() with name_map=None (the new default) leaves sample names unchanged."""
    groups = [
        {"sample_name": "sample_A", "fields": {"R1": "sample_A_R1.fastq"}},
    ]
    knex = DummyKnex(groups)
    filepaths = {"sample_A_R1.fastq": "/tmp/sample_A_R1.fastq"}

    result = group_files(
        knex,
        filepaths,
        "short_read::single_end",
        regex=r"(?P<sample_name>.+)",
        yes=True,
        # name_map omitted — exercises the None default path
    )

    assert result[0]["sample_name"] == "sample_A"
    assert knex.calls[0][0] == "bulk_upload/group_files"
    assert filepaths.keys() == set(knex.calls[0][1]["filenames"])


def test_group_files_confirm_aborts_when_not_yes():
    """group_files() with yes=False aborts (via click.Abort) when the user declines the prompt.

    CliRunner surfaces the abort as a non-zero exit code.
    """
    from click.testing import CliRunner
    import click

    groups = [
        {"sample_name": "sample_B", "fields": {"R1": "sample_B_R1.fastq"}},
    ]
    knex = DummyKnex(groups)
    filepaths = {"sample_B_R1.fastq": "/tmp/sample_B_R1.fastq"}

    @click.command()
    def _run():
        group_files(
            knex,
            filepaths,
            "short_read::single_end",
            regex=r"(?P<sample_name>.+)",
            yes=False,
        )

    runner = CliRunner()
    # Simulate the user typing "n" at the confirmation prompt.
    result = runner.invoke(_run, input="n\n")
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# LR-05: deprecation of `upload reads --link-type s3` for local-file-list
# ---------------------------------------------------------------------------


def _local_filepaths():
    """Return a filepaths-shape dict with local paths (no s3:// values)."""
    return {
        "sample_A_R1.fastq.gz": "/data/reads/sample_A_R1.fastq.gz",
        "sample_A_R2.fastq.gz": "/data/reads/sample_A_R2.fastq.gz",
    }


def test_link_type_s3_with_local_paths_warns(capsys):
    """`--link-type s3` over local paths must fire DeprecationWarning + stderr echo."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _maybe_warn_link_type_s3_deprecated("s3", _local_filepaths())

    assert len(caught) == 1
    assert issubclass(caught[0].category, DeprecationWarning)
    assert _LINK_TYPE_S3_DEPRECATION_MSG in str(caught[0].message)

    # User-visible stderr echo (so warning filters can't silence it).
    err = capsys.readouterr().err
    assert "DeprecationWarning" in err
    assert "geoseeq link reads" in err
    assert "geoseeq s3 register" in err


def test_link_type_upload_does_not_warn(capsys):
    """Default byte-upload mode must NOT emit the deprecation warning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _maybe_warn_link_type_s3_deprecated("upload", _local_filepaths())

    assert caught == []
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("other_link_type", ["ftp", "sra", "azure", "http"])
def test_link_type_other_does_not_warn(other_link_type, capsys):
    """Other --link-type values (ftp/sra/azure/http) are out of scope."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _maybe_warn_link_type_s3_deprecated(other_link_type, _local_filepaths())

    assert caught == []
    assert capsys.readouterr().err == ""


def test_link_type_s3_with_s3_uri_paths_does_not_warn(capsys):
    """If the file list values are themselves s3:// URIs, don't warn (out of scope)."""
    s3_paths = {
        "sample_A_R1.fastq.gz": "s3://my-bucket/path/sample_A_R1.fastq.gz",
        "sample_A_R2.fastq.gz": "s3://my-bucket/path/sample_A_R2.fastq.gz",
    }
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _maybe_warn_link_type_s3_deprecated("s3", s3_paths)

    assert caught == []
    assert capsys.readouterr().err == ""
