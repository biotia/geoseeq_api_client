"""Filesystem status comparison against the manifest for a GeoSeeqRepo."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from .repo import GeoSeeqRepo


@dataclass
class RepoStatus:
    """Classification of local files relative to the manifest.

    downloaded: paths present on disk with a matching checksum.
    absent: paths in the manifest that do not exist on disk.
    new_local: paths found on disk under samples/ or project_results/
               that are not in the manifest.
    modified_local: paths present on disk whose checksum differs from
                    the manifest entry.
    """

    downloaded: List[str] = field(default_factory=list)
    absent: List[str] = field(default_factory=list)
    new_local: List[str] = field(default_factory=list)
    modified_local: List[str] = field(default_factory=list)


def _md5(path: Path) -> str:
    """Return the hex MD5 digest of the file at *path*."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_status(repo: "GeoSeeqRepo") -> RepoStatus:
    """Compare the manifest against the local filesystem and return a RepoStatus.

    For every file in the manifest, check whether it exists on disk and
    whether its MD5 checksum matches.  Then scan samples/ and
    project_results/ for any files not listed in the manifest.
    """
    status = RepoStatus()
    manifest_paths: set[str] = set()

    for _sample_name, sample in repo.manifest.samples.items():
        for _folder_name, folder in sample.result_folders.items():
            for _file_name, mfile in folder.files.items():
                manifest_paths.add(mfile.local_path)
                disk_path = repo.root / mfile.local_path
                if not disk_path.exists():
                    status.absent.append(mfile.local_path)
                else:
                    expected_hex = (
                        mfile.checksum.split(":", 1)[-1]
                        if ":" in mfile.checksum
                        else mfile.checksum
                    )
                    actual_hex = _md5(disk_path)
                    if actual_hex == expected_hex:
                        status.downloaded.append(mfile.local_path)
                    else:
                        status.modified_local.append(mfile.local_path)

    for scan_dir in ("samples", "project_results"):
        base = repo.root / scan_dir
        if base.exists():
            for p in base.rglob("*"):
                if p.is_file():
                    rel = str(p.relative_to(repo.root))
                    if rel not in manifest_paths:
                        status.new_local.append(rel)

    return status
