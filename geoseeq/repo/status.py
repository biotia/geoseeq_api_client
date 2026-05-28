"""Status data type for comparing a GeoSeeqRepo against its local filesystem."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class RepoStatus:
    """Classification of local files relative to the manifest.

    downloaded: paths present on disk, unchanged locally and current with the
                server version.
    absent: paths in the manifest that do not exist on disk.
    new_local: paths found on disk under samples/ or project_results/
               that are not in the manifest.
    modified_local: paths present on disk whose recorded content hash differs
                    from the on-disk content (edited since download).
    outdated: paths present on disk whose recorded server ``version_replicate``
              differs from the manifest's — a newer version exists on the
              server.  A file may be both unmodified locally and outdated, so
              this list is independent of ``modified_local``/``downloaded``.

    The comparison logic lives on :meth:`GeoSeeqRepo.compute_status` so the
    public surface is ``repo.compute_status()``; this module only owns the
    data type it returns.
    """

    downloaded: List[str] = field(default_factory=list)
    absent: List[str] = field(default_factory=list)
    new_local: List[str] = field(default_factory=list)
    modified_local: List[str] = field(default_factory=list)
    outdated: List[str] = field(default_factory=list)
