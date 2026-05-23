"""Dataclasses for the geoseeq repo manifest.json schema."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict


def _md5(path: Path) -> str:
    """Return the hex MD5 digest of the file at *path*."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class ManifestFile:
    """Represents a single file tracked in the manifest."""

    uuid: str
    brn: str
    checksum: str
    size_bytes: int
    local_path: str

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON output."""
        return {
            "uuid": self.uuid,
            "brn": self.brn,
            "checksum": self.checksum,
            "size_bytes": self.size_bytes,
            "local_path": self.local_path,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ManifestFile:
        """Deserialize from a dict."""
        return cls(
            uuid=data["uuid"],
            brn=data["brn"],
            checksum=data["checksum"],
            size_bytes=data["size_bytes"],
            local_path=data["local_path"],
        )


@dataclass
class ManifestResultFolder:
    """Represents a result folder (e.g. 'reads') tracked in the manifest."""

    uuid: str
    files: Dict[str, ManifestFile] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON output."""
        return {
            "uuid": self.uuid,
            "files": {name: f.to_dict() for name, f in self.files.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> ManifestResultFolder:
        """Deserialize from a dict."""
        files = {
            name: ManifestFile.from_dict(fdata)
            for name, fdata in data.get("files", {}).items()
        }
        return cls(uuid=data["uuid"], files=files)


@dataclass
class ManifestSample:
    """Represents a sample tracked in the manifest."""

    uuid: str
    metadata: dict = field(default_factory=dict)
    result_folders: Dict[str, ManifestResultFolder] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON output."""
        return {
            "uuid": self.uuid,
            "metadata": self.metadata,
            "result_folders": {
                name: rf.to_dict() for name, rf in self.result_folders.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> ManifestSample:
        """Deserialize from a dict."""
        result_folders = {
            name: ManifestResultFolder.from_dict(rfdata)
            for name, rfdata in data.get("result_folders", {}).items()
        }
        return cls(
            uuid=data["uuid"],
            metadata=data.get("metadata", {}),
            result_folders=result_folders,
        )


@dataclass
class Manifest:
    """Top-level manifest for a geoseeq repo clone."""

    version: int
    project_uuid: str
    project_name: str
    server_url: str
    samples: Dict[str, ManifestSample] = field(default_factory=dict)
    project_results: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON output."""
        return {
            "version": self.version,
            "project_uuid": self.project_uuid,
            "project_name": self.project_name,
            "server_url": self.server_url,
            "samples": {name: s.to_dict() for name, s in self.samples.items()},
            "project_results": self.project_results,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Manifest:
        """Deserialize from a dict."""
        samples = {
            name: ManifestSample.from_dict(sdata)
            for name, sdata in data.get("samples", {}).items()
        }
        return cls(
            version=data["version"],
            project_uuid=data["project_uuid"],
            project_name=data["project_name"],
            server_url=data["server_url"],
            samples=samples,
            project_results=data.get("project_results", {}),
        )

    @classmethod
    def load(cls, path: Path) -> Manifest:
        """Load a manifest from a JSON file at the given path."""
        with open(path, "r") as fh:
            data = json.loads(fh.read())
        return cls.from_dict(data)

    def save(self, path: Path) -> None:
        """Write this manifest as JSON to the given path."""
        with open(path, "w") as fh:
            fh.write(json.dumps(self.to_dict(), indent=2))
