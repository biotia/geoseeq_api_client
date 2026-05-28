"""Status data type for comparing a GeoSeeqRepo against its local filesystem."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class RepoStatus:
    """Classification of local files relative to the manifest.

    downloaded: paths present on disk with a matching checksum.
    absent: paths in the manifest that do not exist on disk.
    new_local: paths found on disk under samples/ or project_results/
               that are not in the manifest.
    modified_local: paths present on disk whose checksum differs from
                    the manifest entry.

    The comparison logic lives on :meth:`GeoSeeqRepo.compute_status` so the
    public surface is ``repo.compute_status()``; this module only owns the
    data type it returns.
    """

    downloaded: List[str] = field(default_factory=list)
    absent: List[str] = field(default_factory=list)
    new_local: List[str] = field(default_factory=list)
    modified_local: List[str] = field(default_factory=list)
