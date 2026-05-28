"""Dataclasses for the geoseeq repo manifest.json schema."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from os.path import basename
from pathlib import Path
from typing import Dict, Iterator, NamedTuple, Optional


@dataclass
class ManifestFile:
    """Represents a single file tracked in the manifest.

    Mirrors the per-file payload the geoseeq_server writes into manifest.json:
    ``{uuid, checksum, size_bytes, stored_data}``.  ``stored_data`` is the
    server's storage descriptor and always carries a ``"uri"`` key pointing at
    the file's cloud location (e.g. ``s3://bucket/Sample1_R1.fastq.gz``).
    """

    uuid: str
    checksum: str
    size_bytes: int
    stored_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON output."""
        return {
            "uuid": self.uuid,
            "checksum": self.checksum,
            "size_bytes": self.size_bytes,
            "stored_data": self.stored_data,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ManifestFile:
        """Deserialize from a dict, ignoring any unknown keys."""
        return cls(
            uuid=data["uuid"],
            checksum=data["checksum"],
            size_bytes=data["size_bytes"],
            stored_data=data.get("stored_data", {}),
        )

    @property
    def filename(self) -> str:
        """The on-disk file name: basename of the stored cloud URI."""
        return basename(self.stored_data.get("uri", ""))


@dataclass
class ManifestResultFolder:
    """Represents a result folder (e.g. 'raw_reads') tracked in the manifest."""

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


class ManifestFileEntry(NamedTuple):
    """A flattened view of one file in the manifest plus its derived local path.

    ``local_path`` is the repo-root-relative on-disk location for the file and is
    computed in exactly one place (``Manifest.iter_files``) so every consumer
    agrees on where a file lives.
    """

    sample_name: Optional[str]  # None for project-level files
    module_name: str
    field_name: str
    mfile: ManifestFile
    local_path: str


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

    def iter_files(self) -> Iterator[ManifestFileEntry]:
        """Yield every file in the manifest with its derived local path.

        This is the single source of truth for a file's on-disk location.
        Sample files live under ``samples/<sample>/<module>/<filename>`` and
        project-level files under ``project_results/<module>/<filename>``, where
        ``filename`` is the basename of the file's stored cloud URI.
        """
        for sample_name, sample in self.samples.items():
            for module_name, folder in sample.result_folders.items():
                for field_name, mfile in folder.files.items():
                    # Entries without a cloud uri (e.g. inline JSON metric fields)
                    # are not downloadable on-disk files; skip them so they never
                    # produce a directory-like, filename-less local path.
                    if not mfile.filename:
                        continue
                    lp = f"samples/{sample_name}/{module_name}/{mfile.filename}"
                    yield ManifestFileEntry(
                        sample_name, module_name, field_name, mfile, lp
                    )
        for module_name, folder_dict in self.project_results.items():
            folder = ManifestResultFolder.from_dict(folder_dict)
            for field_name, mfile in folder.files.items():
                # Same as above: skip non-file (uri-less) project-level entries.
                if not mfile.filename:
                    continue
                lp = f"project_results/{module_name}/{mfile.filename}"
                yield ManifestFileEntry(None, module_name, field_name, mfile, lp)

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
