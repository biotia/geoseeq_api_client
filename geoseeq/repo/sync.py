"""Download and offload operations for files tracked in a GeoSeeqRepo manifest."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .repo import GeoSeeqRepo
    from .manifest import ManifestFile


class ChecksumError(Exception):
    """Raised when a downloaded file's checksum does not match the manifest."""


def _md5(path: Path) -> str:
    """Return the hex MD5 digest of the file at *path*."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(repo: "GeoSeeqRepo", manifest_file: "ManifestFile", knex) -> None:
    """Download a file from GeoSeeq to the path given by manifest_file.local_path.

    Looks up the result file by UUID via *knex*, downloads it to the local
    path under repo.root, then verifies the MD5 checksum.

    Raises ChecksumError if the downloaded content does not match the
    checksum recorded in the manifest.
    """
    from geoseeq.id_constructors.from_uuids import result_file_from_uuid

    local_path = repo.root / manifest_file.local_path
    local_path.parent.mkdir(parents=True, exist_ok=True)

    result_file = result_file_from_uuid(knex, manifest_file.uuid)
    result_file.download(filename=str(local_path), cache=False)

    expected_hex = (
        manifest_file.checksum.split(":", 1)[-1]
        if ":" in manifest_file.checksum
        else manifest_file.checksum
    )
    actual_hex = _md5(local_path)
    if actual_hex != expected_hex:
        raise ChecksumError(
            f"Error: {manifest_file.local_path} checksum mismatch. "
            f"Expected {manifest_file.checksum}, got md5:{actual_hex}."
        )


def offload_file(repo: "GeoSeeqRepo", manifest_file: "ManifestFile") -> None:
    """Delete the local copy of a file; the manifest entry is preserved.

    A no-op if the file does not exist on disk.
    """
    local_path = repo.root / manifest_file.local_path
    if local_path.exists():
        local_path.unlink()
