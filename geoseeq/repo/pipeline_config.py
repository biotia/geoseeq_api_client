"""Generate per-sample pipeline config JSON files from a GeoSeeqRepo manifest."""
from __future__ import annotations

import json
from typing import List

from geoseeq.constants import READS_MODULE_NAMES
from geoseeq.fastq import classify_fastq_field

from .manifest import ManifestFileEntry
from .repo import GeoSeeqRepo


def _ordered_read_paths(entries: List[ManifestFileEntry], read_num: int) -> List[str]:
    """Return derived local paths for the given *read_num* (1 or 2), lane-ordered."""
    classified = [(e, *classify_fastq_field(e.field_name)) for e in entries]
    selected = [
        (e, lane_num) for e, entry_read_num, lane_num in classified
        if entry_read_num == read_num
    ]
    selected.sort(key=lambda pair: pair[1])
    return [e.local_path for e, _ in selected]


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
    reads_1 = _ordered_read_paths(reads_entries, read_num=1)
    reads_2 = _ordered_read_paths(reads_entries, read_num=2)

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
    ``constants.READS_MODULE_NAMES``. Samples without a reads folder are silently
    skipped.
    """
    config_dir = repo.root / "sample_configs"
    config_dir.mkdir(exist_ok=True)

    manifest = repo.manifest
    # Materialize the flattened file list once; iter_files walks the whole
    # manifest, so calling it per-sample would be O(samples x total_files).
    all_entries = list(manifest.iter_files())
    for sample_name, sample in manifest.samples.items():
        reads_modules = [
            name
            for name in sample.result_folders
            if name in READS_MODULE_NAMES
        ]
        if not reads_modules:
            continue

        reads_entries = [
            entry
            for entry in all_entries
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
