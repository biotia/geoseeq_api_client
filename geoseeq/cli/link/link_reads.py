# pylint: disable=line-too-long
"""``geoseeq link reads`` — bulk-register S3 fastq files as result-file links.

This command is the productionized form of the one-off
``link_b2_files.py`` script used for the biotiadx → B2 backfill. It
lists an S3 prefix, uses the server-side grouping endpoints
(``bulk_upload/{validate_filenames, group_files}``) to produce
sample/field assignments identical to what ``geoseeq upload reads``
would have produced, then calls ``result_file.link_s3`` per field —
no bytes are transferred.

Default is ``--dry-run``. Pass ``--commit`` to actually write.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

import click

from geoseeq.constants import FASTQ_MODULE_NAMES
from geoseeq.knex import GeoseeqNotFoundError
from geoseeq.sources import S3Source

from ..shared_params import (
    handle_project_id,
    module_option,
    project_id_arg,
    use_common_state,
    yes_option,
)


def _basename(key: str) -> str:
    """Return the file basename of an S3 key (everything after the last ``/``)."""
    return key.rsplit("/", 1)[-1]


def _build_filename_index(keys: Iterable[str]) -> Dict[str, str]:
    """Return a mapping ``basename -> full s3 key`` for the listed keys.

    Server grouping operates on basenames, so we need to recover the full
    key when constructing the final ``s3://bucket/key`` URI. Collisions
    (same basename under different prefixes) keep the last seen — this
    matches ``link_b2_files.py`` behaviour and is correct for the
    biotiadx layout where basenames are unique within a project prefix.
    """
    return {_basename(k): k for k in keys}


def _resolve_regex_project_uuid(proj, scratch_uuid):
    """Return a project UUID usable by ``bulk_upload/validate_filenames``.

    The endpoint enforces ``check_object_permissions`` on the supplied
    ``sample_group_id``; on dry-run with a new target the target's UUID
    isn't available yet, so callers must pass ``--scratch-project-uuid``
    pointing at any existing project they can write to.
    """
    if getattr(proj, "uuid", None):
        return proj.uuid
    return scratch_uuid


def _validate_and_group(knex, filename_to_key, module_name, regex_project_uuid, custom_regex):
    """Call ``validate_filenames`` then ``group_files`` and return ``(regex, groups)``.

    Mirrors the two-step flow used by ``upload reads`` (server picks a
    regex, then groups). Returns a (regex, groups) tuple, or raises
    ``click.ClickException`` if the server couldn't pick a regex.
    """
    seq_type = module_name.split("::", 1)[1]
    filenames = list(filename_to_key.keys())

    val_payload = {
        "filenames": filenames,
        "sequence_type": seq_type,
        "sample_group_id": regex_project_uuid,
    }
    if custom_regex:
        val_payload["custom_regex"] = custom_regex
    validation = knex.post("bulk_upload/validate_filenames", json=val_payload)
    regex = validation.get("regex_used")
    unmatched = validation.get("unmatched", []) or []
    click.echo(f'Using regex: "{regex}"', err=True)
    if unmatched:
        click.echo(
            f"{len(unmatched)} files could not be grouped and will be skipped.",
            err=True,
        )
        for fname in unmatched[:5]:
            click.echo(f"  {fname}", err=True)
        if len(unmatched) > 5:
            click.echo(f"  ... and {len(unmatched) - 5} more", err=True)
    if regex is None:
        raise click.ClickException(
            f"Server could not pick a regex for sequence_type={seq_type!r}. "
            "Try a different --module (paired_end vs single_end vs nanopore) "
            "or pass --regex explicitly."
        )

    matched = validation.get("matched", filenames) or filenames
    groups = knex.post(
        "bulk_upload/group_files",
        json={"filenames": matched, "sequence_type": seq_type, "regex": regex},
    )
    return regex, groups


def _build_actions(groups, filename_to_key, source) -> List[Tuple[str, str, str]]:
    """Flatten grouped output to ``(sample_name, field_name, s3_uri)`` tuples.

    The server's ``bulk_upload/group_files`` returns field names already
    seq-type-prefixed (e.g. ``paired_end::read_1::lane_001``); the caller
    (``_commit_actions``) builds the canonical fully-prefixed file name
    by prepending the ``seq_length`` from ``module_name``.
    """
    actions: List[Tuple[str, str, str]] = []
    for group in groups:
        sample_name = group["sample_name"]
        for field_name, filename in group["fields"].items():
            key = filename_to_key.get(filename)
            if key is None:
                continue
            actions.append((sample_name, field_name, source.to_s3_uri(key)))
    return actions


def _print_actions(actions, header_verb, limit=20):
    """Print a short, scannable preview of pending link actions."""
    n_samples = len({a[0] for a in actions})
    click.echo(
        f"\n{header_verb} {len(actions)} files across {n_samples} samples:",
        err=True,
    )
    for sample, field, uri in actions[:limit]:
        click.echo(f"  {sample} :: {field}  <- {uri}", err=True)
    if len(actions) > limit:
        click.echo(f"  ... and {len(actions) - limit} more", err=True)


def _commit_actions(proj, module_name, actions, endpoint_url):
    """Create the geoseeq objects and call ``link_s3`` per field.

    Groups actions by ``sample_name`` so each ``sample.idem`` and
    ``folder.idem`` runs exactly once per sample regardless of how
    many fields it has. This is the perf fix from the biotiadx
    tissuelyser scale check — naive per-file looping wastes ~75% of the
    API chatter on samples with multiple lanes.
    """
    by_sample: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for sample_name, field_name, uri in actions:
        by_sample[sample_name].append((field_name, uri))

    # The server's ``bulk_upload/group_files`` endpoint already prefixes
    # field names with the seq-type (e.g. ``single_end::read_1::lane_1``).
    # Build the canonical fully-prefixed file name from the top-level
    # ``seq_length`` (e.g. ``short_read``) here so we match the preview
    # rendered by ``_grouping.group_files`` exactly and avoid relying on
    # ``read_file``'s defensive normalization at the call site.
    seq_length = module_name.split('::')[0]

    linked = 0
    for sample_name, fields in by_sample.items():
        sample = proj.sample(sample_name).idem()
        folder = sample.result_folder(module_name).idem()
        for field_name, uri in fields:
            result_file = folder.result_file(f'{seq_length}::{field_name}')
            # idem before link_s3 — link_s3 calls save() which requires
            # the row to exist. New files get empty stored_data first,
            # then link_s3 overwrites with the s3 link payload.
            result_file.idem()
            result_file.link_s3(uri, endpoint_url=endpoint_url)
            linked += 1
            click.echo(f"  linked {sample_name} :: {field_name}", err=True)
    return linked


def _resolve_target_project(knex, project_id, yes, privacy_level, commit):
    """Fetch (or, on commit, create) the target project.

    On dry-run with a missing project we return a stub-shaped object
    without UUID; callers must supply ``--scratch-project-uuid`` for the
    grouping endpoints. On commit we create the project up-front using
    the existing ``handle_project_id`` flow.
    """
    is_public = privacy_level == "public"
    try:
        return handle_project_id(
            knex, project_id, yes=yes, private=not is_public, create=commit
        )
    except GeoseeqNotFoundError:
        if commit:
            raise
        return None


@click.command("reads")
@use_common_state
@yes_option
@module_option(FASTQ_MODULE_NAMES)
@click.option(
    "--endpoint-url",
    default=None,
    help="Custom S3 endpoint URL (required for non-AWS S3: B2, Wasabi, MinIO).",
)
@click.option(
    "--regex",
    default=None,
    help="Override the server-side filename grouping regex.",
)
@click.option(
    "--filter",
    "filter_pattern",
    default="*.fastq.gz",
    help="fnmatch pattern applied to listed S3 keys.",
)
@click.option(
    "--aws-profile",
    default=None,
    help="Boto3 named profile used for S3 listing credentials.",
)
@click.option(
    "--privacy-level",
    type=click.Choice(["public", "private"]),
    default="private",
    help=(
        "Privacy level for newly-created projects. Default 'private'. "
        "Do NOT pass 'public' for clinical or sample data without "
        "explicit approval. "
        "'shareable' will be available once the api-client's "
        "project-creation accepts privacy_level; tracked separately."
    ),
)
@click.option(
    "--scratch-project-uuid",
    default=None,
    help=(
        "UUID of any project the caller can write to. Used only by the "
        "server-side regex-picker on dry-run when the target project "
        "does not exist yet."
    ),
)
@click.option(
    "--dry-run/--commit",
    "dry_run",
    default=True,
    help="Print proposed (sample, field, uri) tuples without writing (default).",
)
@project_id_arg
@click.argument("source_uri", nargs=1)
def cli_link_reads(
    state,
    yes,
    module_name,
    endpoint_url,
    regex,
    filter_pattern,
    aws_profile,
    privacy_level,
    scratch_project_uuid,
    dry_run,
    project_id,
    source_uri,
):
    """Bulk-register fastq files under an S3 prefix as GeoSeeq result-file links.

    No bytes are transferred — each matched file is registered as a link
    to its existing ``s3://bucket/key`` location.

    \b
    Example:
      geoseeq link reads --commit --endpoint-url https://s3.us-east-005.backblazeb2.com \\
          MyOrg/MyProject s3://my-bucket/myproject/

    [PROJECT_ID] org/project path, UUID, or GRN (created on --commit if absent).
    [SOURCE_URI] s3://bucket/prefix/ to enumerate.
    """
    knex = state.get_knex()
    source = S3Source.from_uri(
        source_uri, endpoint_url=endpoint_url, aws_profile=aws_profile
    )

    commit = not dry_run
    proj = _resolve_target_project(knex, project_id, yes, privacy_level, commit)

    keys = list(source.list_keys(filter_pattern))
    if not keys:
        raise click.ClickException(
            f"No keys matching {filter_pattern!r} under {source_uri!r}."
        )
    click.echo(f"Found {len(keys)} keys under {source_uri}", err=True)

    filename_to_key = _build_filename_index(keys)
    regex_project_uuid = _resolve_regex_project_uuid(proj, scratch_project_uuid)
    if regex_project_uuid is None:
        raise click.ClickException(
            "Target project does not exist yet; the server's regex-picker "
            "needs a real sample_group_id. Either pass --commit (which "
            "creates the project first) or --scratch-project-uuid <any "
            "project you can write to>."
        )

    _, groups = _validate_and_group(
        knex, filename_to_key, module_name, regex_project_uuid, regex
    )
    actions = _build_actions(groups, filename_to_key, source)

    verb = "LINKING" if commit else "WOULD LINK"
    _print_actions(actions, verb)

    if not commit:
        click.echo("\n(dry-run) pass --commit to actually link files.", err=True)
        return

    # link_s3 raises for s3:// URIs without endpoint_url. Default to the
    # standard AWS S3 endpoint so the AWS-S3 case (the typical default)
    # works out of the box; explicit --endpoint-url still wins for B2,
    # Wasabi, MinIO, etc.
    link_endpoint = source.endpoint_url or "https://s3.amazonaws.com"
    linked = _commit_actions(proj, module_name, actions, link_endpoint)
    click.echo(f"\nLinked {linked} files.", err=True)
