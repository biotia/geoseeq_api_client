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
