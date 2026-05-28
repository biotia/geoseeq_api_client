"""Error type for download/offload operations on a GeoSeeqRepo manifest."""
from __future__ import annotations


class ChecksumError(Exception):
    """Raised when a downloaded file's checksum does not match the manifest.

    The download/offload behavior lives on :meth:`GeoSeeqRepo.download_file`
    and :meth:`GeoSeeqRepo.offload_file` so the public surface is
    ``repo.download_file(entry, knex)`` / ``repo.offload_file(entry)``; this
    module only owns the exception they raise.
    """
