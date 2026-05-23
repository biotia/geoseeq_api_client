"""GeoSeeqRepo handle: wraps a local .geoseeq/ git repo."""
from __future__ import annotations

import subprocess
from pathlib import Path

from .config import RepoConfig
from .manifest import Manifest


class NonFastForwardError(Exception):
    """Raised when a git push is rejected because it is not a fast-forward."""


class NotARepoError(Exception):
    """Raised when no .geoseeq/config.json is found in the directory tree."""


class GeoSeeqRepo:
    """Handle for a local geoseeq project repository.

    A geoseeq repo is a directory tree that contains a .geoseeq/ subdirectory
    which is itself a git repository tracking manifest.json.  The parent
    directory holds working copies of downloaded sample data.
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

    def commit(self, message: str) -> None:
        """Stage manifest.json and create a git commit inside .geoseeq/."""
        geoseeq_dir = self.root / ".geoseeq"
        subprocess.run(
            ["git", "-C", str(geoseeq_dir), "add", "manifest.json"],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(geoseeq_dir), "commit", "-m", message],
            check=True,
        )

    def git_push(self) -> None:
        """Push to origin main.

        Raises NonFastForwardError if the push is rejected as non-fast-forward.
        Raises subprocess.CalledProcessError for other failures.
        """
        geoseeq_dir = self.root / ".geoseeq"
        result = subprocess.run(
            ["git", "-C", str(geoseeq_dir), "push", "origin", "main"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            if "non-fast-forward" in result.stderr or "rejected" in result.stderr:
                raise NonFastForwardError(result.stderr)
            raise subprocess.CalledProcessError(
                result.returncode, result.args, result.stderr
            )

    def git_pull(self) -> None:
        """Run git pull --rebase origin main inside .geoseeq/."""
        geoseeq_dir = self.root / ".geoseeq"
        subprocess.run(
            ["git", "-C", str(geoseeq_dir), "pull", "--rebase", "origin", "main"],
            check=True,
        )
