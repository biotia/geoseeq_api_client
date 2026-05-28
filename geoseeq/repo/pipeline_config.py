"""Generate per-sample pipeline config JSON files from a GeoSeeqRepo manifest."""
from __future__ import annotations

import json
import re
from typing import List

from .manifest import ManifestFileEntry
from .repo import GeoSeeqRepo

# Reads-module folder names. These mirror the ``folder_name`` column of the
# server's DATA_NAMES table (geoseeq_server pangea/core/views/data_views/fastq.py)
# and additionally include the legacy ``reads``/``raw_reads`` names that appear
# in server tests, so detection stays permissive across both conventions.
READ_MODULE_NAMES = {
    "raw::raw_reads",
    "raw::single_short_reads",
    "short_read::paired_end",
    "short_read::single_end",
    "long_read::nanopore",
    "long_read::pacbio",
    "reads",
    "raw_reads",
}

# Read pair number and lane number are encoded in the file's *field name* (the
# dict key the server emits), e.g. "read_1", "paired_end::read_2::lane_3".
READ_PAIR_RE = re.compile(r"read_(?P<pair_num>1|2)")
LANE_RE = re.compile(r"lane_(?P<lane_num>\d+)")


def _lane_num(field_name: str) -> int:
    """Return the 1-based lane number encoded in *field_name* (default 1)."""
    match = LANE_RE.search(field_name)
    return int(match.group("lane_num")) if match else 1


def _is_read_2(field_name: str) -> bool:
    """Return True if *field_name* designates read 2 of a pair.

    Anything that does not explicitly match ``read_2`` (including single-end
    reads with no pair token at all) is treated as read 1.
    """
    match = READ_PAIR_RE.search(field_name)
    return match is not None and match.group("pair_num") == "2"


def _ordered_read_paths(entries: List[ManifestFileEntry], read_2: bool) -> List[str]:
    """Return derived local paths for read-1 (or read-2) files, lane-ordered."""
    selected = [e for e in entries if _is_read_2(e.field_name) == read_2]
    selected.sort(key=lambda e: _lane_num(e.field_name))
    return [e.local_path for e in selected]


def _build_sample_config(
    sample_name: str,
    sample_uuid: str,
    metadata: dict,
    reads_entries: List[ManifestFileEntry],
    repo_config,
) -> dict:
    """Build the pipeline config dict for a single sample's reads folder.

    *sample_uuid* is the UUID of the sample itself (written to ``geoseeq_uuid``).
    *reads_entries* are the manifest entries for the sample's reads folder, each
    carrying the file's derived ``local_path``.
    """
    reads_1 = _ordered_read_paths(reads_entries, read_2=False)
    reads_2 = _ordered_read_paths(reads_entries, read_2=True)

    checksum = ""
    if reads_1:
        first = next(e for e in reads_entries if e.local_path == reads_1[0])
        checksum = first.mfile.checksum

    return {
        "sample_name": sample_name,
        "reads_1": reads_1,
        "reads_2": reads_2,
        "fastq_checksum": checksum,
        "bdx_result_dir": "samples/",
        "geoseeq_uuid": sample_uuid,
        "geoseeq_endpoint": repo_config.server_url,
        "metadata": metadata,
    }


def write_pipeline_configs(repo: GeoSeeqRepo) -> None:
    """Write one pipeline config JSON per sample that has a reads result folder.

    Config files are written to <repo.root>/sample_configs/<sample_name>.json.
    A folder counts as a reads folder when its ``module_name`` is in
    READ_MODULE_NAMES. Samples without a reads folder are silently skipped.
    """
    config_dir = repo.root / "sample_configs"
    config_dir.mkdir(exist_ok=True)

    manifest = repo.manifest
    for sample_name, sample in manifest.samples.items():
        reads_modules = [
            name
            for name in sample.result_folders
            if name in READ_MODULE_NAMES
        ]
        if not reads_modules:
            continue

        reads_entries = [
            entry
            for entry in manifest.iter_files()
            if entry.sample_name == sample_name and entry.module_name in reads_modules
        ]

        sample_config = _build_sample_config(
            sample_name,
            sample.uuid,
            sample.metadata,
            reads_entries,
            repo.config,
        )

        out_path = config_dir / f"{sample_name}.json"
        with open(out_path, "w") as fh:
            fh.write(json.dumps(sample_config, indent=2))
