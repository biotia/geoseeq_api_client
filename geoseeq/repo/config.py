"""Configuration dataclass for a local geoseeq repo."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RepoConfig:
    """Persisted configuration for a local geoseeq repository clone.

    Stored in .geoseeq/config.json, which is gitignored so credentials
    never leave the local machine.
    """

    project_uuid: str
    server_url: str

    @property
    def git_remote_url(self) -> str:
        """The git remote URL for this project's manifest repository.

        Derived from ``server_url`` and ``project_uuid`` rather than persisted,
        so it can never drift from the project it points at.
        """
        return f"{self.server_url.rstrip('/')}/api/projects/{self.project_uuid}/git"

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON output."""
        return {
            "project_uuid": self.project_uuid,
            "server_url": self.server_url,
        }

    @classmethod
    def from_dict(cls, data: dict) -> RepoConfig:
        """Deserialize from a dict, ignoring any unknown keys.

        Older on-disk config.json files may still contain ``auth_profile`` and
        ``git_remote_url`` keys; those are ignored here so loading them never
        breaks (back-compat).
        """
        return cls(
            project_uuid=data["project_uuid"],
            server_url=data["server_url"],
        )

    @classmethod
    def load(cls, path: Path) -> RepoConfig:
        """Load config from a JSON file at the given path."""
        with open(path, "r") as fh:
            data = json.loads(fh.read())
        return cls.from_dict(data)

    def save(self, path: Path) -> None:
        """Write this config as JSON to the given path."""
        with open(path, "w") as fh:
            fh.write(json.dumps(self.to_dict(), indent=2))
