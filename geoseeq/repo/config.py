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
    auth_profile: str
    git_remote_url: str

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON output."""
        return {
            "project_uuid": self.project_uuid,
            "server_url": self.server_url,
            "auth_profile": self.auth_profile,
            "git_remote_url": self.git_remote_url,
        }

    @classmethod
    def from_dict(cls, data: dict) -> RepoConfig:
        """Deserialize from a dict."""
        return cls(
            project_uuid=data["project_uuid"],
            server_url=data["server_url"],
            auth_profile=data.get("auth_profile", ""),
            git_remote_url=data["git_remote_url"],
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
