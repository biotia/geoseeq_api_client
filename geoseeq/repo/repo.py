"""GeoSeeqRepo handle: wraps a local .geoseeq/ git repo."""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from .config import RepoConfig
from .manifest import Manifest, ManifestFileEntry, _md5
from .status import RepoStatus
from .sync import ChecksumError

if TYPE_CHECKING:
    from geoseeq.knex import Knex
    from geoseeq.organization import Project


class NotARepoError(Exception):
    """Raised when no .geoseeq/config.json is found in the directory tree."""


def _expected_hex(checksum: str) -> str:
    """Return the bare hex digest of a manifest checksum (drops a ``md5:`` prefix)."""
    return checksum.split(":", 1)[-1] if ":" in checksum else checksum


class GeoSeeqRepo:
    """Handle for a local geoseeq project repository.

    A geoseeq repo is a directory tree that contains a .geoseeq/ subdirectory
    which is itself a git repository tracking manifest.json.  The parent
    directory holds working copies of downloaded sample data.

    The client is READ-ONLY against the manifest git repo: the server is the
    sole commit authority.  The only git operations the client performs are
    the initial clone and ``git_pull``.
    """

    def __init__(self, root: Path) -> None:
        """Initialise the handle with the project root directory."""
        self.root = root
        self._manifest: Manifest | None = None
        self._config: RepoConfig | None = None

    @property
    def manifest(self) -> Manifest:
        """Lazy-load and return the manifest for this repo."""
        if self._manifest is None:
            self._manifest = Manifest.load(self.root / ".geoseeq" / "manifest.json")
        return self._manifest

    @property
    def config(self) -> RepoConfig:
        """Lazy-load and return the config for this repo."""
        if self._config is None:
            self._config = RepoConfig.load(self.root / ".geoseeq" / "config.json")
        return self._config

    @classmethod
    def find(cls, cwd: Path) -> GeoSeeqRepo:
        """Walk up the directory tree from *cwd* looking for .geoseeq/config.json.

        Raises NotARepoError if no geoseeq repo root is found.
        """
        candidate = cwd.resolve()
        while True:
            if (candidate / ".geoseeq" / "config.json").exists():
                return cls(candidate)
            parent = candidate.parent
            if parent == candidate:
                raise NotARepoError(
                    f"Not inside a geoseeq repo (searched from {cwd})"
                )
            candidate = parent

    @classmethod
    def clone(
        cls,
        knex: "Knex",
        proj: "Project",
        clone_path: Path,
        profile: Optional[str] = None,
    ) -> GeoSeeqRepo:
        """Clone *proj*'s manifest repo into *clone_path* and return the handle.

        Creates the standard directory layout, clones the project's manifest
        git repo into ``.geoseeq/``, writes the slim config.json, gitignores
        it, then regenerates the pipeline configs.  ``profile`` is accepted for
        call-site symmetry but is intentionally not persisted.

        Raises click.ClickException if ``.geoseeq/`` already exists or the git
        clone fails.
        """
        import click

        from .clone import (
            create_repo_directories,
            ensure_config_gitignored,
            git_clone,
            write_config,
        )

        # Knex appends "/api" to the endpoint at construction; the bare server
        # URL is what gets persisted and what git_remote_url is derived from.
        server_url = knex.endpoint_url.rstrip("/")
        if server_url.endswith("/api"):
            server_url = server_url[: -len("/api")]
        token = knex.auth.token if knex.auth is not None else None
        git_remote_url = f"{server_url}/api/v1/projects/{proj.uuid}/git"

        geoseeq_dir = clone_path / ".geoseeq"
        if geoseeq_dir.exists():
            raise click.ClickException("Directory already contains a geoseeq repo.")

        create_repo_directories(clone_path)
        git_clone(git_remote_url, geoseeq_dir, token, server_url)
        write_config(geoseeq_dir, proj.uuid, server_url)
        ensure_config_gitignored(geoseeq_dir)

        repo = cls(clone_path)
        repo.write_pipeline_configs()
        return repo

    def git_pull(self) -> None:
        """Run git pull --rebase origin main inside .geoseeq/.

        The client is read-only against the manifest git repo — the server
        is the sole commit authority.  This fetch is the only git operation
        the client performs (besides the initial clone).

        Uses check=True so subprocess.CalledProcessError propagates to the
        caller intentionally — no special error type is defined for pull
        failures.
        """
        geoseeq_dir = self.root / ".geoseeq"
        subprocess.run(
            ["git", "-C", str(geoseeq_dir), "pull", "--rebase", "origin", "main"],
            check=True,
        )

    def write_pipeline_configs(self) -> None:
        """Regenerate the per-sample pipeline config JSON files for this repo."""
        from .pipeline_config import write_pipeline_configs

        write_pipeline_configs(self)

    def compute_status(self) -> RepoStatus:
        """Compare the manifest against the local filesystem and return a RepoStatus.

        For every file in the manifest (via ``iter_files``), check whether it
        exists on disk and whether its MD5 checksum matches.  Then scan
        ``samples/`` and ``project_results/`` for any files not listed in the
        manifest.
        """
        status = RepoStatus()
        manifest_paths: set[str] = set()

        for entry in self.manifest.iter_files():
            manifest_paths.add(entry.local_path)
            disk_path = self.root / entry.local_path
            if not disk_path.exists():
                status.absent.append(entry.local_path)
                continue
            actual_hex = _md5(disk_path)
            if actual_hex == _expected_hex(entry.mfile.checksum):
                status.downloaded.append(entry.local_path)
            else:
                status.modified_local.append(entry.local_path)

        for scan_dir in ("samples", "project_results"):
            base = self.root / scan_dir
            if not base.exists():
                continue
            for p in base.rglob("*"):
                if p.is_file():
                    rel = str(p.relative_to(self.root))
                    if rel not in manifest_paths:
                        status.new_local.append(rel)

        return status

    def download_file(self, entry: ManifestFileEntry, knex: "Knex") -> None:
        """Download the file for *entry* to its derived local path and verify it.

        Looks up the result file by UUID via *knex*, downloads it to
        ``entry.local_path`` under repo.root, then verifies the MD5 checksum.

        Raises ChecksumError if the downloaded content does not match the
        checksum recorded in the manifest.
        """
        from geoseeq.id_constructors.from_uuids import result_file_from_uuid

        local_path = self.root / entry.local_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        result_file = result_file_from_uuid(knex, entry.mfile.uuid)
        result_file.download(filename=str(local_path), cache=False)

        actual_hex = _md5(local_path)
        if actual_hex != _expected_hex(entry.mfile.checksum):
            raise ChecksumError(
                f"Error: {entry.local_path} checksum mismatch. "
                f"Expected {entry.mfile.checksum}, got md5:{actual_hex}."
            )

    def offload_file(self, entry: ManifestFileEntry) -> None:
        """Delete the local copy of *entry*; the manifest entry is preserved.

        A no-op if the file does not exist on disk.
        """
        local_path = self.root / entry.local_path
        if local_path.exists():
            local_path.unlink()
