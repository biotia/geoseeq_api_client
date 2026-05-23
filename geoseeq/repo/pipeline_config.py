"""Generate per-sample pipeline config JSON files from a GeoSeeqRepo manifest."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from .repo import GeoSeeqRepo


def _find_read_file(files: dict, read_tag: str) -> Optional[str]:
    """Return the filename of the first file whose name contains *read_tag*, or None."""
    for name in files:
        if read_tag in name:
            return name
    return None


def _build_sample_config(sample_name: str, folder, repo_config) -> dict:
    """Build the pipeline config dict for a single sample's reads folder."""
    files = folder.files

    r1_name = _find_read_file(files, "_R1") or _find_read_file(files, "_1")
    r2_name = _find_read_file(files, "_R2") or _find_read_file(files, "_2")

    reads_base = f"samples/{sample_name}/reads"
    reads_1 = f"{reads_base}/{r1_name}" if r1_name else None
    reads_2 = f"{reads_base}/{r2_name}" if r2_name else None

    checksum = files[r1_name].checksum if r1_name else ""

    config = {
        "sample_name": sample_name,
        "reads_1": reads_1,
        "reads_2": reads_2,
        "fastq_checksum": checksum,
        "bdx_result_dir": "samples/",
        "geoseeq_uuid": folder.uuid,
        "geoseeq_endpoint": repo_config.server_url,
        "metadata": {},
    }
    return config


def write_pipeline_configs(repo: GeoSeeqRepo) -> None:
    """Write one pipeline config JSON per sample that has a 'reads' result folder.

    Config files are written to <repo.root>/sample_configs/<sample_name>.json.
    Samples without a 'reads' result folder are silently skipped.
    """
    config_dir = repo.root / "sample_configs"
    config_dir.mkdir(exist_ok=True)

    for sample_name, sample in repo.manifest.samples.items():
        reads_folder = sample.result_folders.get("reads")
        if reads_folder is None:
            continue

        sample_config = _build_sample_config(sample_name, reads_folder, repo.config)
        sample_config["metadata"] = sample.metadata

        out_path = config_dir / f"{sample_name}.json"
        with open(out_path, "w") as fh:
            fh.write(json.dumps(sample_config, indent=2))
