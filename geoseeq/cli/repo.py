"""CLI commands for managing local geoseeq project repositories."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import click

from geoseeq.repo import GeoSeeqRepo
from geoseeq.repo.config import RepoConfig
from geoseeq.repo.pipeline_config import write_pipeline_configs

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
