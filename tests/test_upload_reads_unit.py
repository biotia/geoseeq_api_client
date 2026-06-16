from pathlib import Path

from geoseeq.cli._grouping import group_files


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
    """group_files() with yes=False raises SystemExit when the user declines the prompt."""
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
