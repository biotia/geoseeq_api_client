"""CLI commands for managing local geoseeq project repositories."""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import shutil

import click

from geoseeq.repo import GeoSeeqRepo, ManifestFile, ManifestResultFolder
from geoseeq.repo.config import RepoConfig
from geoseeq.repo.pipeline_config import write_pipeline_configs
from geoseeq.repo.repo import NotARepoError
from geoseeq.repo.sync import upload_file

from .shared_params import use_common_state
from .shared_params.id_handlers import handle_project_id


@click.group("repo")
def cli_repo():
    """Manage local geoseeq project repositories."""


@cli_repo.command("clone")
@use_common_state
@click.argument("project_name")
@click.argument("path", default=None, required=False)
def clone(state, project_name, path):
    """Clone a GeoSeeq project repository.

    PROJECT_NAME should be in "Org/Project" format.
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

    proj = handle_project_id(knex, project_name, create=False)

    server_url = state.endpoint.rstrip("/")
    git_remote_url = f"{server_url}/api/v1/projects/{proj.uuid}/git"

    clone_path = Path(path) if path else Path(project_name.split("/")[-1])

    geoseeq_dir = clone_path / ".geoseeq"
    if geoseeq_dir.exists():
        raise click.ClickException("Directory already contains a geoseeq repo.")

    _create_repo_directories(clone_path)
    _git_clone(git_remote_url, geoseeq_dir, state.api_token, server_url)
    _write_config(geoseeq_dir, proj.uuid, server_url, state.profile, git_remote_url)
    _ensure_config_gitignored(geoseeq_dir)

    repo = GeoSeeqRepo(clone_path)
    write_pipeline_configs(repo)

    n_samples = len(repo.manifest.samples)
    click.echo(f'Cloned "{project_name}" to {clone_path} ({n_samples} samples)')


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
    try:
        repo = GeoSeeqRepo.find(Path(path))
    except NotARepoError:
        raise click.ClickException("Error: not inside a GeoSeeq project directory")

    project_uuid = repo.config.project_uuid
    server_url = repo.config.server_url.rstrip("/")
    url = (
        f"{server_url}/api/v1/projects/{project_uuid}/manifest/history/"
        f"?limit={limit}&offset={offset}"
    )

    knex = state.get_knex()
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


def _create_repo_directories(clone_path: Path) -> None:
    """Create the standard subdirectory layout for a new clone."""
    clone_path.mkdir(parents=True, exist_ok=True)
    (clone_path / "sample_configs").mkdir(exist_ok=True)
    (clone_path / "samples").mkdir(exist_ok=True)
    (clone_path / "project_results").mkdir(exist_ok=True)


def _build_authenticated_url(remote_url: str, token: str) -> str:
    """Embed the API token into the remote URL for git clone."""
    parsed = urlparse(remote_url)
    authed = parsed._replace(netloc=f"x:{token}@{parsed.netloc}")
    return authed.geturl()


def _scrub_token(text: str, token: str | None) -> str:
    """Replace the API token in *text* with *** to prevent credential leaks."""
    if token:
        text = text.replace(f"x:{token}@", "x:***@")
    return text


def _git_clone(remote_url: str, geoseeq_dir: Path, token: str | None, server_url: str) -> None:
    """Run git clone to initialise the .geoseeq/ sub-repository."""
    clone_url = remote_url
    if token:
        clone_url = _build_authenticated_url(remote_url, token)

    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    result = subprocess.run(
        ["git", "clone", clone_url, str(geoseeq_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        safe_stderr = _scrub_token(result.stderr, token)
        raise click.ClickException(
            f"git clone failed:\n{safe_stderr}"
        )


def _write_config(
    geoseeq_dir: Path,
    project_uuid: str,
    server_url: str,
    auth_profile: str,
    git_remote_url: str,
) -> None:
    """Write config.json into the .geoseeq/ directory."""
    config = RepoConfig(
        project_uuid=project_uuid,
        server_url=server_url,
        auth_profile=auth_profile,
        git_remote_url=git_remote_url,
    )
    config.save(geoseeq_dir / "config.json")


def _ensure_config_gitignored(geoseeq_dir: Path) -> None:
    """Ensure config.json is listed in .geoseeq/.gitignore."""
    gitignore_path = geoseeq_dir / ".gitignore"
    entry = "config.json\n"

    if gitignore_path.exists():
        existing = gitignore_path.read_text()
        if "config.json" not in existing:
            with open(gitignore_path, "a") as fh:
                fh.write(entry)
    else:
        gitignore_path.write_text(entry)


def _iter_manifest_files(repo, sample_filter=None, file_filter=None):
    """Yield (sample_name, mfile) pairs from the manifest with optional filters.

    sample_filter: if set, only yield files from the sample with this name.
    file_filter: if set, only yield files whose local_path contains this string.
    """
    for sample_name, sample in repo.manifest.samples.items():
        if sample_filter and sample_name != sample_filter:
            continue
        for folder in sample.result_folders.values():
            for mfile in folder.files.values():
                if file_filter and file_filter not in mfile.local_path:
                    continue
                yield sample_name, mfile


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
    from geoseeq.repo.status import compute_status

    try:
        repo = GeoSeeqRepo.find(Path(path))
    except NotARepoError:
        raise click.ClickException("Error: not inside a GeoSeeq project directory")

    repo_status = compute_status(repo)
    project_name = repo.manifest.project_name

    if sample:
        repo_status.absent = [
            p for p in repo_status.absent
            if f"/{sample}/" in p or p.startswith(f"{sample}/")
        ]
        repo_status.modified_local = [
            p for p in repo_status.modified_local
            if f"/{sample}/" in p or p.startswith(f"{sample}/")
        ]
        repo_status.new_local = [
            p for p in repo_status.new_local
            if f"/{sample}/" in p or p.startswith(f"{sample}/")
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
    """Pull latest manifest from server and download newly-appeared files.

    Runs git pull on the .geoseeq/ sub-repo to fetch the latest manifest,
    then downloads any files that appear in the new manifest but were not
    in the previous one.  Pipeline configs are regenerated after the pull.

    ---

    Example Usage:

    \b
    # Pull the latest manifest and download new files
    $ geoseeq repo pull

    \b
    # Pull only new files for a specific sample
    $ geoseeq repo pull --sample MySample

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    from geoseeq.repo.sync import download_file

    try:
        repo = GeoSeeqRepo.find(Path(path))
    except NotARepoError:
        raise click.ClickException("Error: not inside a GeoSeeq project directory")

    knex = state.get_knex().set_auth_required()

    old_paths = {
        mfile.local_path
        for _sname, sample_obj in repo.manifest.samples.items()
        for folder in sample_obj.result_folders.values()
        for mfile in folder.files.values()
    }

    repo.git_pull()
    repo._manifest = None  # invalidate the cached manifest

    new_files = [
        (sname, mfile)
        for sname, mfile in _iter_manifest_files(repo, sample, file_filter)
        if mfile.local_path not in old_paths
    ]

    for _sname, mfile in new_files:
        download_file(repo, mfile, knex)

    write_pipeline_configs(repo)

    if new_files:
        sample_names = sorted({s for s, _ in new_files})
        click.echo(f"Pulled {len(new_files)} new files from {', '.join(sample_names)}")
    else:
        click.echo("Already up to date.")


@cli_repo.command("download")
@use_common_state
@click.option("--sample", "-s", default=None, help="Filter to one sample")
@click.option("--file", "-f", "file_filter", default=None, help="Filter to files whose path contains this string")
@click.option("--all", "download_all", is_flag=True, help="Re-download files even if already present on disk")
@click.argument("path", default=".", required=False)
def download_cmd(state, path, sample, file_filter, download_all):
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
    from geoseeq.repo.status import compute_status
    from geoseeq.repo.sync import download_file

    try:
        repo = GeoSeeqRepo.find(Path(path))
    except NotARepoError:
        raise click.ClickException("Error: not inside a GeoSeeq project directory")

    knex = state.get_knex().set_auth_required()

    repo_status = compute_status(repo)
    absent_set = set(repo_status.absent)

    targets = [
        (sname, mfile)
        for sname, mfile in _iter_manifest_files(repo, sample, file_filter)
        if download_all or mfile.local_path in absent_set
    ]

    if not targets:
        click.echo("Nothing to download.")
        return

    for _sname, mfile in targets:
        download_file(repo, mfile, knex)

    click.echo(f"Downloaded {len(targets)} file(s).")


@cli_repo.command("offload")
@use_common_state
@click.option("--sample", "-s", default=None, help="Filter to one sample")
@click.option("--file", "-f", "file_filter", default=None, help="Filter to files whose path contains this string")
@click.argument("path", default=".", required=False)
def offload_cmd(state, path, sample, file_filter):
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
    from geoseeq.repo.status import compute_status
    from geoseeq.repo.sync import offload_file

    try:
        repo = GeoSeeqRepo.find(Path(path))
    except NotARepoError:
        raise click.ClickException("Error: not inside a GeoSeeq project directory")

    repo_status = compute_status(repo)
    modified_set = set(repo_status.modified_local)

    targets = list(_iter_manifest_files(repo, sample, file_filter))

    blocked = [
        mfile.local_path
        for _sname, mfile in targets
        if mfile.local_path in modified_set
    ]
    if blocked:
        paths_str = "\n  ".join(blocked)
        raise click.ClickException(
            f"Refusing to offload modified-local files:\n  {paths_str}\n"
            "Commit or revert your local changes before offloading."
        )

    count = 0
    for _sname, mfile in targets:
        offload_file(repo, mfile)
        count += 1

    click.echo(f"Offloaded {count} file(s).")


@cli_repo.command("push")
@use_common_state
@click.option("--sample", "-s", default=None, help="Limit push to one sample")
@click.option("--message", "-m", default=None, help="Optional commit message suffix")
@click.argument("path", default=".", required=False)
def push(state, sample, message, path):
    """Upload new-local and modified-local files to GeoSeeq.

    Scans the repo status, uploads every file that is new or modified locally
    (optionally filtered to --sample), updates the manifest, then pulls the
    server's updated commit.

    ---

    Example Usage:

    \b
    # Push all new/modified files
    $ geoseeq repo push

    \b
    # Push only files for a specific sample
    $ geoseeq repo push --sample MySample

    \b
    # Push with a custom commit message
    $ geoseeq repo push --message "add baseline sequencing run"

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    from geoseeq.repo.status import compute_status

    try:
        repo = GeoSeeqRepo.find(Path(path))
    except NotARepoError:
        raise click.ClickException("Error: not inside a GeoSeeq project directory")

    knex = state.get_knex().set_auth_required()
    repo_status = compute_status(repo)

    candidates = repo_status.new_local + repo_status.modified_local
    if sample:
        candidates = [
            p for p in candidates
            if f"/{sample}/" in p or p.startswith(f"{sample}/")
        ]

    if not candidates:
        click.echo("Nothing to push.")
        return

    sample_names = set()
    for rel_path_str in candidates:
        local_path = repo.root / rel_path_str
        parts = Path(rel_path_str).parts
        # Expected structure: samples/<sample_name>/<folder_name>/<file_name>
        if len(parts) < 4 or parts[0] != "samples":
            click.echo(f"Skipping {rel_path_str}: unexpected path structure.")
            continue

        s_name = parts[1]
        f_name = parts[2]
        file_name = "/".join(parts[3:])

        mfile = upload_file(repo, local_path, s_name, f_name, file_name, knex)

        manifest_sample = repo.manifest.samples[s_name]
        if f_name not in manifest_sample.result_folders:
            # The folder was just created on the server; record a placeholder UUID.
            # A full manifest refresh (pull) will update this with the real UUID.
            from geoseeq.id_constructors.from_uuids import sample_from_uuid
            srv_sample = sample_from_uuid(knex, manifest_sample.uuid)
            srv_folder = srv_sample.result_folder(f_name).idem()
            manifest_sample.result_folders[f_name] = ManifestResultFolder(
                uuid=srv_folder.uuid,
                files={},
            )

        manifest_sample.result_folders[f_name].files[file_name] = mfile
        sample_names.add(s_name)

    if not sample_names:
        click.echo("Nothing to push.")
        return

    n = len(candidates)
    names_str = ", ".join(sorted(sample_names))
    commit_msg = f"push: uploaded {n} files for {names_str}"
    if message:
        commit_msg += f" — {message}"

    repo.manifest.save(repo.root / ".geoseeq" / "manifest.json")
    repo.git_pull()

    click.echo(f"Pushed {n} files for {names_str}")


@cli_repo.command("new-sample")
@use_common_state
@click.option(
    "--metadata-file",
    "metadata_file",
    default=None,
    type=click.Path(exists=True),
    help="Path to a JSON file with sample metadata",
)
@click.argument("name")
@click.argument("path", default=".", required=False)
def new_sample(state, name, metadata_file, path):
    """Create a new sample in a GeoSeeq project.

    Creates the sample on the server and adds it to the local manifest.  A
    sample directory is also created under samples/<name>/ in the repo root.

    ---

    Example Usage:

    \b
    # Add a sample with no metadata
    $ geoseeq repo new-sample "My Sample"

    \b
    # Add a sample with metadata from a file
    $ geoseeq repo new-sample "My Sample" --metadata-file meta.json

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    try:
        repo = GeoSeeqRepo.find(Path(path))
    except NotARepoError:
        raise click.ClickException("Error: not inside a GeoSeeq project directory")

    metadata = {}
    if metadata_file:
        with open(metadata_file, "r") as fh:
            metadata = json.load(fh)

    knex = state.get_knex().set_auth_required()
    repo.create_sample(name, metadata, knex)

    sample_dir = repo.root / "samples" / name
    sample_dir.mkdir(parents=True, exist_ok=True)

    repo.manifest.save(repo.root / ".geoseeq" / "manifest.json")
    repo.git_pull()

    click.echo(f"Created sample '{name}'")


@cli_repo.command("rm")
@use_common_state
@click.argument("path", default=".", required=False)
def rm(state, path):
    """Remove a local geoseeq repo directory.

    Refuses to remove the repo if any local files are out of sync (new-local or
    modified-local) to prevent accidental data loss.  Run 'geoseeq repo status'
    to see what is out of sync, then push or discard the changes before removing.

    ---

    Example Usage:

    \b
    # Remove the repo in the current directory
    $ geoseeq repo rm

    \b
    # Remove a repo at a specific path
    $ geoseeq repo rm /path/to/my-project

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    from geoseeq.repo.status import compute_status

    try:
        repo = GeoSeeqRepo.find(Path(path))
    except NotARepoError:
        raise click.ClickException("Error: not inside a GeoSeeq project directory")

    repo_status = compute_status(repo)
    if repo_status.new_local or repo_status.modified_local:
        raise click.ClickException(
            "Error: project is not fully synced. "
            "Run 'geoseeq repo status' to see what's out of sync."
        )

    root = repo.root
    shutil.rmtree(root)
    click.echo(f"Removed local repo at {root}")
