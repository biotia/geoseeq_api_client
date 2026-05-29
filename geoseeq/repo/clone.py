"""SDK helpers for cloning a GeoSeeq project repository."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import click

from .config import RepoConfig


def create_repo_directories(clone_path: Path) -> None:
    """Create the standard subdirectory layout for a new clone."""
    clone_path.mkdir(parents=True, exist_ok=True)
    (clone_path / "sample_configs").mkdir(exist_ok=True)
    (clone_path / "samples").mkdir(exist_ok=True)
    (clone_path / "project_results").mkdir(exist_ok=True)


def build_authenticated_url(remote_url: str, token: str) -> str:
    """Embed the API token into the remote URL for git clone."""
    parsed = urlparse(remote_url)
    authed = parsed._replace(netloc=f"x:{token}@{parsed.netloc}")
    return authed.geturl()


def scrub_token(text: str, token: str | None) -> str:
    """Replace the API token in *text* with *** to prevent credential leaks."""
    if token:
        text = text.replace(f"x:{token}@", "x:***@")
    return text


def git_clone(remote_url: str, geoseeq_dir: Path, token: str | None, server_url: str) -> None:
    """Run git clone to initialise the .geoseeq/ sub-repository."""
    clone_url = remote_url
    if token:
        clone_url = build_authenticated_url(remote_url, token)

    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    result = subprocess.run(
        ["git", "clone", clone_url, str(geoseeq_dir)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        safe_stderr = scrub_token(result.stderr, token)
        raise click.ClickException(
            f"git clone failed:\n{safe_stderr}"
        )


def write_config(geoseeq_dir: Path, project_uuid: str, server_url: str) -> None:
    """Write config.json into the .geoseeq/ directory.

    Only ``project_uuid`` and ``server_url`` are persisted; ``git_remote_url``
    is derived from those by ``RepoConfig`` and the auth profile is
    intentionally not stored.
    """
    config = RepoConfig(project_uuid=project_uuid, server_url=server_url)
    config.save(geoseeq_dir / "config.json")


def ensure_config_gitignored(geoseeq_dir: Path) -> None:
    """Ensure the client-private files are listed in .geoseeq/.gitignore.

    ``config.json`` (auth/server config), ``state.json`` (the local download
    index) and ``.state.lock`` (the flock file guarding parallel state writes)
    are all client-private and must never be committed to the manifest git
    repo.  Any of these entries already present is left as-is; only the missing
    ones are appended.
    """
    gitignore_path = geoseeq_dir / ".gitignore"
    private_files = ("config.json", "state.json", ".state.lock")

    existing = gitignore_path.read_text() if gitignore_path.exists() else ""
    existing_lines = set(existing.splitlines())
    missing = [name for name in private_files if name not in existing_lines]
    if not missing:
        return

    # Preserve a trailing newline so appended entries stay one-per-line.
    prefix = existing if existing.endswith("\n") or not existing else existing + "\n"
    gitignore_path.write_text(prefix + "".join(f"{name}\n" for name in missing))
