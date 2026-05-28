"""CLI commands for managing local geoseeq project repositories."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import click

from geoseeq.repo import GeoSeeqRepo, RepoExistsError

from .progress_bar import PBarManager
from .shared_params import use_common_state, yes_option
from .shared_params.id_handlers import handle_project_id


@click.group("repo")
def cli_repo():
    """Manage local geoseeq project repositories."""


@cli_repo.command("clone")
@use_common_state
@click.argument("project_id")
@click.argument("path", default=None, required=False)
def clone(state, project_id, path):
    """Clone a GeoSeeq project repository.

    PROJECT_ID can be "Org/Project", a UUID, or a GRN.
    PATH defaults to the last component of the project name.

    ---

    Example Usage:

    \b
    # Clone a project into a directory named after the project
    $ geoseeq repo clone "My Org/My Project"

    \b
    # Clone a project into a specific directory
    $ geoseeq repo clone "My Org/My Project" ./my-local-dir

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex().set_auth_required()

    proj = handle_project_id(knex, project_id, create=False)

    clone_path = Path(path) if path else Path(project_id.split("/")[-1])

    try:
        repo = GeoSeeqRepo.clone(knex, proj, clone_path, profile=state.profile)
    except RepoExistsError as exc:
        raise click.ClickException(str(exc))

    n_samples = len(repo.manifest.samples)
    click.echo(f'Cloned "{project_id}" to {clone_path} ({n_samples} samples)')


@cli_repo.command("log")
@use_common_state
@click.option("--limit", default=20, show_default=True, help="Maximum number of entries to return.")
@click.option("--offset", default=0, show_default=True, help="Number of entries to skip.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Print raw API response as JSON.")
@click.argument("path", default=".")
def log(state, limit, offset, as_json, path):
    """Show manifest commit history for this geoseeq project.

    PATH defaults to the current directory. The command walks up the
    directory tree to find the nearest .geoseeq/config.json.

    ---

    Example Usage:

    \b
    # Show the last 20 entries
    $ geoseeq repo log

    \b
    # Show 5 entries starting at offset 10
    $ geoseeq repo log --limit 5 --offset 10

    \b
    # Emit raw JSON for scripting
    $ geoseeq repo log --json

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    repo = GeoSeeqRepo.find(Path(path))

    project_uuid = repo.config.project_uuid
    server_url = repo.config.server_url.rstrip("/")
    url = (
        f"{server_url}/api/v1/projects/{project_uuid}/manifest/history/"
        f"?limit={limit}&offset={offset}"
    )

    knex = state.get_knex().set_auth_required()
    response = knex.sess.get(url)
    data = knex._handle_response(response, json_response=False).json()

    if as_json:
        click.echo(json.dumps(data, indent=2))
        return

    results = data.get("results", [])
    if not results:
        click.echo("No manifest history yet.")
        return

    for entry in results:
        short_sha = entry["sha1"][:10]
        ts = _format_timestamp(entry.get("timestamp", ""))
        message = entry.get("message", "")
        click.echo(f"{short_sha}  {ts}  {message}")


def _format_timestamp(raw: str) -> str:
    """Parse an ISO-8601 timestamp string and return a human-readable form.

    Strips a trailing 'Z' before parsing so ``datetime.fromisoformat`` works
    on Python 3.10 and earlier.  Returns the raw string unchanged if parsing
    fails so that the output is never empty.
    """
    try:
        cleaned = raw.rstrip("Z")
        dt = datetime.fromisoformat(cleaned)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return raw


def _human_size(nbytes: int) -> str:
    """Format a byte count as a human-readable string (KB, MB, GB, etc.)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024.0:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024.0
    return f"{nbytes:.1f} PB"


def _filtered_entries(repo, sample=None, file_filter=None):
    """Yield manifest file entries, optionally filtered by sample and path.

    sample: if set, only yield entries whose ``sample_name`` matches.
    file_filter: if set, only yield entries whose ``local_path`` contains it.
    """
    for entry in repo.manifest.iter_files():
        if sample and entry.sample_name != sample:
            continue
        if file_filter and file_filter not in entry.local_path:
            continue
        yield entry


@cli_repo.command("status")
@use_common_state
@click.option("--sample", "-s", default=None, help="Filter to one sample")
@click.argument("path", default=".", required=False)
def status_cmd(state, path, sample):
    """Show sync status of a local project repo.

    Compares the manifest against local files and reports which files are
    absent, modified, or present locally but not in the manifest.

    ---

    Example Usage:

    \b
    # Show status for the current directory
    $ geoseeq repo status

    \b
    # Show status for a specific sample
    $ geoseeq repo status --sample MySample

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    repo = GeoSeeqRepo.find(Path(path))

    repo_status = repo.compute_status()
    project_name = repo.manifest.project_name

    if sample:
        sample_paths = {
            entry.local_path
            for entry in repo.manifest.iter_files()
            if entry.sample_name == sample
        }
        sample_prefix = f"samples/{sample}/"
        repo_status.absent = [p for p in repo_status.absent if p in sample_paths]
        repo_status.modified_local = [
            p for p in repo_status.modified_local if p in sample_paths
        ]
        repo_status.new_local = [
            p for p in repo_status.new_local if p.startswith(sample_prefix)
        ]

    if not (repo_status.absent or repo_status.new_local or repo_status.modified_local):
        click.echo(f'Project "{project_name}" is fully synced.')
        return

    click.echo(f"Project: {project_name}\n")
    if repo_status.absent:
        click.echo("absent (not downloaded):")
        for p in repo_status.absent:
            click.echo(f"  {p}")
    if repo_status.new_local:
        click.echo("\nnew-local (not in manifest):")
        for p in repo_status.new_local:
            click.echo(f"  {p}")
    if repo_status.modified_local:
        click.echo("\nmodified-local:")
        for p in repo_status.modified_local:
            click.echo(f"  {p}")


@cli_repo.command("pull")
@use_common_state
@click.option("--sample", "-s", default=None, help="Filter to one sample")
@click.option("--file", "-f", "file_filter", default=None, help="Filter to files whose path contains this string")
@click.argument("path", default=".", required=False)
def pull(state, path, sample, file_filter):
    """Pull latest manifest from server.

    Runs git pull on the .geoseeq/ sub-repo to fetch the latest manifest
    and regenerates pipeline configs.  Does NOT download file content;
    use ``geoseeq repo download`` to fetch new files.

    ---

    Example Usage:

    \b
    # Pull the latest manifest
    $ geoseeq repo pull

    \b
    # Pull and see new files for a specific sample
    $ geoseeq repo pull --sample MySample

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    repo = GeoSeeqRepo.find(Path(path))

    old_paths = {entry.local_path for entry in repo.manifest.iter_files()}

    repo.git_pull()
    repo._manifest = None  # invalidate the cached manifest

    new_files = [
        entry
        for entry in _filtered_entries(repo, sample, file_filter)
        if entry.local_path not in old_paths
    ]

    repo.write_pipeline_configs()

    if new_files:
        n = len(new_files)
        click.echo(
            f"Pulled manifest: {n} new file(s) available "
            "(use 'geoseeq repo download' to fetch them)"
        )
    else:
        click.echo("Already up to date.")


@cli_repo.command("download")
@use_common_state
@yes_option
@click.option("--sample", "-s", default=None, help="Filter to one sample")
@click.option("--file", "-f", "file_filter", default=None, help="Filter to files whose path contains this string")
@click.option("--all", "download_all", is_flag=True, help="Re-download files even if already present on disk")
@click.argument("path", default=".", required=False)
def download_cmd(state, yes, path, sample, file_filter, download_all):
    """Download absent files from the manifest.

    By default only files not yet present on disk are downloaded.
    Pass --all to re-download even files that appear to already be present.

    ---

    Example Usage:

    \b
    # Download all absent files
    $ geoseeq repo download

    \b
    # Download all absent files for a specific sample
    $ geoseeq repo download --sample MySample

    \b
    # Re-download every file in the manifest
    $ geoseeq repo download --all

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    from geoseeq.id_constructors.from_uuids import result_file_from_uuid
    from geoseeq.upload_download_manager import GeoSeeqDownloadManager

    repo = GeoSeeqRepo.find(Path(path))

    knex = state.get_knex().set_auth_required()

    repo_status = repo.compute_status()
    absent_set = set(repo_status.absent)

    targets = [
        entry
        for entry in _filtered_entries(repo, sample, file_filter)
        if download_all or entry.local_path in absent_set
    ]

    if not targets:
        click.echo("Nothing to download.")
        return

    download_manager = GeoSeeqDownloadManager(
        n_parallel_downloads=1,
        log_level=state.log_level,
        progress_tracker_factory=PBarManager().get_new_bar,
    )
    for entry in targets:
        rf = result_file_from_uuid(knex, entry.mfile.uuid)
        local_path = repo.root / entry.local_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        download_manager.add_download(rf, str(local_path))

    click.echo(download_manager.get_preview_string(), err=True)
    if not yes:
        click.confirm(f"Download {len(targets)} file(s)?", abort=True)
    download_manager.download_files()


@cli_repo.command("offload")
@use_common_state
@yes_option
@click.option("--quiet", "-q", is_flag=True, help="Suppress file list output")
@click.option("--sample", "-s", default=None, help="Filter to one sample")
@click.option("--file", "-f", "file_filter", default=None, help="Filter to files whose path contains this string")
@click.argument("path", default=".", required=False)
def offload_cmd(state, yes, quiet, path, sample, file_filter):
    """Remove local file copies while keeping manifest entries.

    Refuses to offload files whose local copy has been modified (checksum
    mismatch) to prevent accidental data loss.

    ---

    Example Usage:

    \b
    # Offload all downloaded files for the current project
    $ geoseeq repo offload

    \b
    # Offload only files for a specific sample
    $ geoseeq repo offload --sample MySample

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    repo = GeoSeeqRepo.find(Path(path))

    repo_status = repo.compute_status()
    modified_set = set(repo_status.modified_local)

    targets = list(_filtered_entries(repo, sample, file_filter))

    blocked = [
        entry.local_path
        for entry in targets
        if entry.local_path in modified_set
    ]
    if blocked:
        paths_str = "\n  ".join(blocked)
        raise click.ClickException(
            f"Refusing to offload modified-local files:\n  {paths_str}\n"
            "Commit or revert your local changes before offloading."
        )

    total_bytes = 0
    for entry in targets:
        local_path = repo.root / entry.local_path
        if local_path.exists():
            size = local_path.stat().st_size
            total_bytes += size
            if not quiet:
                click.echo(f"  {entry.local_path} ({_human_size(size)})")

    if not quiet:
        click.echo(f"\nTotal disk space to free: {_human_size(total_bytes)}")

    if not yes:
        click.confirm(f"Offload {len(targets)} file(s)?", abort=True)

    count = 0
    for entry in targets:
        repo.offload_file(entry)
        count += 1

    click.echo(f"Offloaded {count} file(s).")
