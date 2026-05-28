"""GeoSeeqRepo handle: wraps a local .geoseeq/ git repo."""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

from .config import RepoConfig
from .manifest import Manifest, ManifestFileEntry
from .state import RepoState, fast_hash, record_for, record_under_lock
from .status import RepoStatus

if TYPE_CHECKING:
    from geoseeq.knex import Knex
    from geoseeq.organization import Project


class NotARepoError(Exception):
    """Raised when no .geoseeq/config.json is found in the directory tree."""


class RepoExistsError(Exception):
    """Raised by ``GeoSeeqRepo.clone`` when the target already contains a repo.

    Kept CLI-agnostic so the SDK layer never depends on click; the CLI catches
    this and re-raises it as a ``click.ClickException``.
    """


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

        Raises RepoExistsError if ``.geoseeq/`` already exists.  Git clone
        failures propagate from ``git_clone``.
        """
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
            raise RepoExistsError("Directory already contains a geoseeq repo.")

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

    def pull(self) -> tuple[list[ManifestFileEntry], list[ManifestFileEntry]]:
        """git-pull the manifest, regenerate pipeline configs, and return (new, updated) entries.

        Snapshots each file's ``version_replicate`` keyed by local path, runs
        ``git_pull`` to fetch the latest manifest, invalidates the cached
        manifest, then classifies the refreshed entries: a file is *new* if its
        local path was not in the snapshot, and *updated* if its path was present
        but its ``version_replicate`` changed.  Finally regenerates the pipeline
        configs.  Does NOT download file content.
        """
        old_versions = {
            entry.local_path: entry.mfile.version_replicate
            for entry in self.manifest.iter_files()
        }

        self.git_pull()
        self._manifest = None  # invalidate the cached manifest

        new_files: list[ManifestFileEntry] = []
        updated_files: list[ManifestFileEntry] = []
        for entry in self.manifest.iter_files():
            if entry.local_path not in old_versions:
                new_files.append(entry)
            elif entry.mfile.version_replicate != old_versions[entry.local_path]:
                updated_files.append(entry)

        self.write_pipeline_configs()
        return new_files, updated_files

    def write_pipeline_configs(self) -> None:
        """Regenerate the per-sample pipeline config JSON files for this repo."""
        from .pipeline_config import write_pipeline_configs

        write_pipeline_configs(self)

    def compute_status(self) -> RepoStatus:
        """Compare the manifest + local state index against disk.

        For each manifest file (via ``iter_files``): absent if not on disk;
        otherwise classified using the recorded state in ``.geoseeq/state.json``
        — ``modified_local`` if the on-disk content differs from the recorded
        xxh3, ``downloaded`` if unchanged and current, and additionally
        ``outdated`` if the server's ``version_replicate`` has moved past the
        recorded one.  Finally scans ``samples/`` and ``project_results/`` for
        files not in the manifest (``new_local``).
        """
        status = RepoStatus()
        state = RepoState.load(self.root / ".geoseeq")
        manifest_paths: set[str] = set()
        state_dirty = False

        for entry in self.manifest.iter_files():
            manifest_paths.add(entry.local_path)
            disk_path = self.root / entry.local_path
            if not disk_path.exists():
                status.absent.append(entry.local_path)
                continue
            state_dirty |= self._classify_present(entry, disk_path, state, status)

        self._scan_new_local(manifest_paths, status)

        if state_dirty:
            state.save()
        return status

    def _classify_present(
        self,
        entry: ManifestFileEntry,
        disk_path: Path,
        state: RepoState,
        status: RepoStatus,
    ) -> bool:
        """Classify an on-disk manifest file as modified/downloaded and outdated.

        Mutates *status* in place and returns True if *state* was refreshed (a
        recovered mtime) so the caller knows to persist it.
        """
        record = state.get(entry.local_path)
        if record is None:
            # Present but never recorded (e.g. downloaded by an older client):
            # we can't verify it, so treat it as a plain downloaded file.
            status.downloaded.append(entry.local_path)
            return False

        modified, refreshed = self._is_modified(disk_path, record, state, entry.local_path)
        if modified:
            status.modified_local.append(entry.local_path)
        else:
            status.downloaded.append(entry.local_path)

        version = entry.mfile.version_replicate
        if version and version != record.get("version_replicate", ""):
            status.outdated.append(entry.local_path)
        return refreshed

    @staticmethod
    def _is_modified(
        disk_path: Path, record: dict, state: RepoState, rel_path: str
    ) -> tuple[bool, bool]:
        """Decide if *disk_path* has been edited since download.

        Stat fast-path: if size and mtime match the record, it is unchanged.
        Otherwise recompute the xxh3 hash; a matching hash means the file was
        only touched (refresh the recorded mtime), a differing hash means it was
        edited.  Returns ``(is_modified, state_refreshed)``.
        """
        stat = disk_path.stat()
        if stat.st_size == record.get("size_bytes") and stat.st_mtime == record.get("mtime"):
            return False, False
        if fast_hash(disk_path) != record.get("xxh3"):
            return True, False
        # Same content, different mtime/size metadata: refresh the record so the
        # cheap stat fast-path works next time.
        refreshed = dict(record)
        refreshed["size_bytes"] = stat.st_size
        refreshed["mtime"] = stat.st_mtime
        state.set(rel_path, refreshed)
        return False, True

    def _scan_new_local(self, manifest_paths: set[str], status: RepoStatus) -> None:
        """Append on-disk files under samples/ or project_results/ not in the manifest."""
        for scan_dir in ("samples", "project_results"):
            base = self.root / scan_dir
            if not base.exists():
                continue
            for p in base.rglob("*"):
                if p.is_file():
                    rel = str(p.relative_to(self.root))
                    if rel not in manifest_paths:
                        status.new_local.append(rel)

    def download_file(self, entry: ManifestFileEntry, knex: "Knex") -> None:
        """Download the file for *entry* and record its state locally.

        Looks up the result file by UUID via *knex*, downloads it to
        ``entry.local_path`` under repo.root, then records the downloaded
        content's state (server ``version_replicate``, size, mtime, xxh3) in
        ``.geoseeq/state.json``.

        No content-hash verification is performed against the manifest: the
        manifest ``checksum`` is an S3 ETag dict, not a trustworthy file content
        hash, so there is nothing to verify against (deferred until the server
        records a real content hash).  The recorded xxh3 is what later lets
        :meth:`compute_status` detect local edits.

        Note: this single-file call has no cross-process race, but it still
        records via the shared :func:`record_under_lock` path (rather than a
        bespoke ``RepoState.load``/``set``/``save``) so the SDK and the CLI's
        per-file download callback converge on one recording path.  Callers
        downloading MANY files should not call this in a tight loop; the CLI
        ``download`` command instead records each file via a per-file manager
        callback as it completes (resumable, and lock-serialized under
        ``--cores>1``).
        """
        from geoseeq.id_constructors.from_uuids import result_file_from_uuid

        local_path = self.root / entry.local_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        result_file = result_file_from_uuid(knex, entry.mfile.uuid)
        result_file.download(filename=str(local_path), cache=False)

        record_under_lock(
            self.root / ".geoseeq",
            entry.local_path,
            record_for(local_path, entry.mfile.version_replicate),
        )

    def offload_file(self, entry: ManifestFileEntry) -> bool:
        """Delete the local copy of *entry*; the manifest entry is preserved.

        After deleting the file its record is removed from the local state
        index (the content is no longer on disk, so any recorded hash is stale).

        Returns ``True`` if a file was deleted, ``False`` if it was already
        absent (a no-op).  Callers use the return value to count files that
        were actually offloaded.
        """
        local_path = self.root / entry.local_path
        if local_path.exists():
            local_path.unlink()
            state = RepoState.load(self.root / ".geoseeq")
            state.remove(entry.local_path)
            state.save()
            return True
        return False

    def is_fully_synced(self) -> bool:
        """Return True if no local files are new or modified relative to the manifest.

        A repo is "fully synced" for the purposes of safe deletion when nothing on
        disk is unpushed: no ``new_local`` files (present locally but not in the
        manifest) and no ``modified_local`` files (edited since download).  Absent
        and outdated files are ignored — those represent server content the user
        simply hasn't downloaded yet, which is not at risk of being lost.
        """
        s = self.compute_status()
        return not (s.new_local or s.modified_local)

    def new_sample(self, name: str, metadata: dict, knex: "Knex") -> str:
        """Create a new sample in this repo's project on the server.

        Creates the sample (with *metadata*) via the API, makes its local
        ``samples/<name>/`` directory, then ``git_pull``s the server's updated
        manifest (the server commits the new sample) and regenerates pipeline
        configs.  The client never writes the manifest itself.

        Returns the new sample's UUID.
        """
        from geoseeq.id_constructors.from_uuids import project_from_uuid

        project = project_from_uuid(knex, self.manifest.project_uuid)
        sample = project.sample(name, metadata=metadata).idem()

        sample_dir = self.root.joinpath("samples", name)
        sample_dir.mkdir(parents=True, exist_ok=True)

        self.git_pull()
        self._manifest = None

        self.write_pipeline_configs()
        return sample.uuid

    def push(
        self,
        knex: "Knex",
        sample: Optional[str] = None,
        cores: int = 1,
        log_level: int = logging.WARNING,
        progress_tracker_factory: Optional[Callable] = None,
    ) -> list[str]:
        """Upload new-local and modified-local files to GeoSeeq.

        Uploads every file that is new on disk or edited since download
        (optionally filtered to a single *sample*) to its server result folder,
        then ``git_pull``s the server's freshly-committed manifest and records the
        pushed files' local state against the refreshed ``version_replicate``.

        The client never writes or commits the manifest itself: it uploads via the
        API, the server commits on each write, and the client pulls the result.

        Args:
            knex: authenticated Knex client.
            sample: if set, only push files under ``samples/<sample>/``.
            cores: number of parallel uploads.
            log_level: log level passed to the upload manager.
            progress_tracker_factory: optional per-file progress-bar factory.

        Returns the list of pushed repo-root-relative paths (empty if nothing was
        queued).  Raises a plain ``ValueError`` if a candidate file belongs to a
        sample that is not yet in the manifest (the user must ``new-sample`` first).
        """
        from geoseeq.id_constructors.from_uuids import sample_from_uuid
        from geoseeq.upload_download_manager import GeoSeeqUploadManager

        status = self.compute_status()
        candidates = status.new_local + status.modified_local
        if sample:
            prefix = f"samples/{sample}/"
            candidates = [p for p in candidates if p.startswith(prefix)]

        mgr = GeoSeeqUploadManager(
            n_parallel_uploads=cores,
            use_atomic_upload=True,
            progress_tracker_factory=progress_tracker_factory,
            log_level=log_level,
        )
        pushed: list[str] = []
        # Cache resolved result folders keyed by (sample, module) so a sample/module
        # with many files only hits the API once for sample lookup + folder idem().
        folder_cache: dict[tuple[str, str], object] = {}
        for rel in candidates:
            parts = Path(rel).parts
            # The manifest is flat: a pushable file is EXACTLY
            # samples/<sample>/<module>/<filename> (4 parts).  Nested files cannot
            # round-trip (the manifest's derived local_path strips subdirectories to
            # the basename), so they would be uploaded mis-mapped and never recorded
            # in state.json — skip them rather than push a path we can't track.
            if len(parts) != 4 or parts[0] != "samples":
                logging.warning(
                    "Skipping %s: not a samples/<sample>/<module>/<file> path.", rel
                )
                continue
            s_name, module, filename = parts[1], parts[2], parts[3]
            if s_name not in self.manifest.samples:
                raise ValueError(
                    f"Sample '{s_name}' is not in the manifest. "
                    f"Run 'geoseeq repo new-sample {s_name}' before pushing its files."
                )
            rf = folder_cache.get((s_name, module))
            if rf is None:
                sample_obj = sample_from_uuid(knex, self.manifest.samples[s_name].uuid)
                rf = sample_obj.result_folder(module).idem()
                folder_cache[(s_name, module)] = rf
            mgr.add_local_file_to_result_folder(
                rf, str(self.root / rel), geoseeq_file_name=filename
            )
            pushed.append(rel)

        if not pushed:
            return []

        mgr.upload_files()

        self.git_pull()
        self._manifest = None

        entries_by_path = {e.local_path: e for e in self.manifest.iter_files()}
        for rel in pushed:
            entry = entries_by_path.get(rel)
            if entry is None:
                continue
            record_under_lock(
                self.root / ".geoseeq",
                rel,
                record_for(self.root / rel, entry.mfile.version_replicate),
            )

        self.write_pipeline_configs()
        return pushed
