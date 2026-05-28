"""Local index of downloaded-file state for a GeoSeeqRepo.

The index lives at ``<repo.root>/.geoseeq/state.json`` and is keyed by
repo-root-relative path.  Each record captures everything needed to answer two
questions without re-downloading:

* "have I edited this file locally?" — compared against the recorded
  ``size_bytes`` / ``mtime`` (a cheap stat fast-path) and, when those differ,
  the recorded ``xxh3`` content hash.
* "does the server have a newer version?" — compared against the manifest's
  ``version_replicate`` (see :class:`geoseeq.repo.manifest.ManifestFile`).

The index is private to the client and is gitignored; the server never sees it.
Each record has the shape::

    {"version_replicate": str, "size_bytes": int, "mtime": float, "xxh3": str}
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Optional

import xxhash

_STATE_FILENAME = "state.json"
_CHUNK_SIZE = 1024 * 1024  # 1 MiB streaming reads for hashing


def fast_hash(path: Path) -> str:
    """Return the xxh3-64 hex digest of the file at *path*.

    Streams the file in 1 MiB chunks so arbitrarily large reads files can be
    hashed without loading them fully into memory.  xxh3 is a fast,
    non-cryptographic hash — appropriate here because we only need to detect
    local edits, not defend against adversarial collisions.
    """
    h = xxhash.xxh3_64()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def record_for(path_on_disk: Path, version_replicate: str) -> dict:
    """Build a fresh state record for the file at *path_on_disk*.

    Captures the current size and mtime (via a single ``os.stat``) plus the
    xxh3 content hash, tagged with the server *version_replicate* the content
    corresponds to.
    """
    stat = os.stat(path_on_disk)
    return {
        "version_replicate": version_replicate,
        "size_bytes": stat.st_size,
        "mtime": stat.st_mtime,
        "xxh3": fast_hash(path_on_disk),
    }


class RepoState:
    """Read/write helper for the ``.geoseeq/state.json`` local index.

    Wraps the plain dict the index serializes to.  Construct via
    :meth:`load`, mutate with :meth:`set`/:meth:`remove`, then :meth:`save`.
    """

    def __init__(self, geoseeq_dir: Path, records: Optional[Dict[str, dict]] = None) -> None:
        """Initialise with the ``.geoseeq/`` dir and an optional records dict."""
        self.geoseeq_dir = geoseeq_dir
        self.records: Dict[str, dict] = records if records is not None else {}

    @property
    def path(self) -> Path:
        """The on-disk location of the state file."""
        return self.geoseeq_dir / _STATE_FILENAME

    @classmethod
    def load(cls, geoseeq_dir: Path) -> "RepoState":
        """Load the state index from *geoseeq_dir*, or start empty if absent."""
        state_path = Path(geoseeq_dir) / _STATE_FILENAME
        if not state_path.exists():
            return cls(Path(geoseeq_dir), {})
        with open(state_path, "r") as fh:
            records = json.loads(fh.read())
        return cls(Path(geoseeq_dir), records)

    def save(self) -> None:
        """Persist the state index to ``state.json`` as indented JSON."""
        self.geoseeq_dir.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as fh:
            fh.write(json.dumps(self.records, indent=2, sort_keys=True))

    def get(self, rel_path: str) -> Optional[dict]:
        """Return the record for *rel_path*, or None if not tracked."""
        return self.records.get(rel_path)

    def set(self, rel_path: str, record: dict) -> None:
        """Insert or replace the record for *rel_path*."""
        self.records[rel_path] = record

    def remove(self, rel_path: str) -> None:
        """Remove the record for *rel_path* if present (no-op otherwise)."""
        self.records.pop(rel_path, None)
