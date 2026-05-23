"""Download, upload, and offload operations for files tracked in a GeoSeeqRepo manifest."""
from __future__ import annotations

from os.path import getsize
from pathlib import Path
from typing import TYPE_CHECKING

from .manifest import ManifestFile, ManifestResultFolder, _md5

if TYPE_CHECKING:
    from .repo import GeoSeeqRepo


class ChecksumError(Exception):
    """Raised when a downloaded file's checksum does not match the manifest."""


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


def upload_file(
    repo: "GeoSeeqRepo",
    local_path: Path,
    sample_name: str,
    folder_name: str,
    file_name: str,
    knex,
) -> ManifestFile:
    """Upload a local file to GeoSeeq and return a populated ManifestFile.

    Looks up the sample and result folder by name/UUID from the manifest, then
    uses the knex upload API to push the file.  The result folder is created on
    the server if it does not already exist (``idem()`` semantics).

    Returns a ManifestFile with UUID, BRN, checksum, size_bytes, and local_path
    filled in.  The checksum is formatted as ``md5:<hex>``.
    """
    from geoseeq.id_constructors.from_uuids import sample_from_uuid

    manifest_sample = repo.manifest.samples[sample_name]
    sample = sample_from_uuid(knex, manifest_sample.uuid)

    result_folder = sample.result_folder(folder_name).idem()

    result_file = result_folder.result_file(file_name)
    result_file.upload_file(str(local_path), use_atomic_upload=True)

    checksum_hex = _md5(local_path)
    size_bytes = getsize(local_path)
    local_path_str = str(local_path.relative_to(repo.root))

    brn = f"brn:{knex.instance_code()}:sample_result_field:{result_file.uuid}"

    return ManifestFile(
        uuid=result_file.uuid,
        brn=brn,
        checksum=f"md5:{checksum_hex}",
        size_bytes=size_bytes,
        local_path=local_path_str,
    )


def offload_file(repo: "GeoSeeqRepo", manifest_file: "ManifestFile") -> None:
    """Delete the local copy of a file; the manifest entry is preserved.

    A no-op if the file does not exist on disk.
    """
    local_path = repo.root / manifest_file.local_path
    if local_path.exists():
        local_path.unlink()
