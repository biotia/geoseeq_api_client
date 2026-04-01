import json
import logging
import os
import sys

import click

from geoseeq.knex import GeoseeqGeneralError
from .shared_params import handle_project_id, project_id_arg, use_common_state

logger = logging.getLogger("geoseeq_api")

_ENV_TEMPLATE = """\
# GeoSeeq S3 Staging Credentials
# Project: {project_name}
# Bucket: {bucket_name}
# Staging prefix: {staging_prefix}
# Endpoint: {endpoint_url}
export AWS_ACCESS_KEY_ID={access_key_id}
export AWS_SECRET_ACCESS_KEY={secret_access_key}
export AWS_DEFAULT_REGION={region}
# Example:
# aws s3 cp myfile.fastq.gz s3://{bucket_name}/{staging_prefix} --endpoint-url {endpoint_url}
"""

_INI_TEMPLATE = """\
[geoseeq-{project_name}]
aws_access_key_id = {access_key_id}
aws_secret_access_key = {secret_access_key}
region = {region}
"""


def _emit_warnings(creds: dict) -> None:
    """Print any inaccessible-sample warnings to stderr."""
    if creds.get("inaccessible_sample_count", 0) > 0:
        for warning in creds.get("warnings", []):
            click.echo(f"Warning: {warning}", err=True)


def _format_env(creds: dict) -> str:
    """Return shell-export credential block."""
    return _ENV_TEMPLATE.format(
        project_name=creds["project_name"],
        bucket_name=creds["bucket_name"],
        staging_prefix=creds["staging_prefix"],
        endpoint_url=creds["endpoint_url"],
        access_key_id=creds["access_key_id"],
        secret_access_key=creds["secret_access_key"],
        region=creds.get("region", "us-east-1"),
    )


def _format_ini(creds: dict) -> str:
    """Return AWS credentials file block."""
    return _INI_TEMPLATE.format(
        project_name=creds["project_name"],
        access_key_id=creds["access_key_id"],
        secret_access_key=creds["secret_access_key"],
        region=creds.get("region", "us-east-1"),
    )


def _fetch_staging_credentials(knex, project_pk: str) -> dict:
    """POST to the staging credentials endpoint and return the JSON response."""
    return knex.post(f"sample_groups/{project_pk}/staging_credentials")


@click.group("s3")
def cli_s3():
    """Commands for S3 direct-access credentials and uploads."""
    pass


@cli_s3.command("credentials")
@use_common_state
@click.option(
    "--output-format",
    "-f",
    type=click.Choice(["env", "json", "ini"], case_sensitive=False),
    default="env",
    show_default=True,
    help="Format for the credential output.",
)
@project_id_arg
def cli_s3_credentials(state, output_format, project_id):
    """Fetch S3 staging credentials for PROJECT.

    PROJECT can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.

    Credentials are written to stdout so they can be captured directly:

    \b
    # Source credentials into the current shell
    $ eval $(geoseeq s3 credentials "My Org/My Project")

    \b
    # Write an AWS credentials file block
    $ geoseeq s3 credentials "My Org/My Project" --output-format ini >> ~/.aws/credentials

    \b
    # Get raw JSON
    $ geoseeq s3 credentials <project-uuid> --output-format json

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex().set_auth_required()
    try:
        proj = handle_project_id(knex, project_id, create=False)
    except (GeoseeqGeneralError, ValueError) as exc:
        click.echo(f"Error looking up project: {exc}", err=True)
        sys.exit(1)

    try:
        creds = _fetch_staging_credentials(knex, proj.uuid)
    except GeoseeqGeneralError as exc:
        click.echo(f"Error fetching staging credentials: {exc}", err=True)
        sys.exit(1)

    _emit_warnings(creds)

    fmt = output_format.lower()
    if fmt == "json":
        click.echo(json.dumps(creds, indent=2), file=state.outfile)
    elif fmt == "ini":
        click.echo(_format_ini(creds).rstrip(), file=state.outfile)
    else:
        click.echo(_format_env(creds).rstrip(), file=state.outfile)


_S3_URI_PREFIX = "s3://"


def _normalize_staged_path(staged_path: str) -> str:
    """Strip an ``s3://<bucket>/`` prefix from *staged_path* if present.

    Accepts either a bare S3 key or a full S3 URI and always returns the
    bare key so the API receives a consistent value.

    Examples::

        >>> _normalize_staged_path("staging/abc/foo.fastq.gz")
        'staging/abc/foo.fastq.gz'
        >>> _normalize_staged_path("s3://my-bucket/staging/abc/foo.fastq.gz")
        'staging/abc/foo.fastq.gz'
    """
    if not staged_path.startswith(_S3_URI_PREFIX):
        return staged_path
    # Strip "s3://" then drop the bucket name (up to the first "/")
    without_scheme = staged_path[len(_S3_URI_PREFIX):]
    _, _, key = without_scheme.partition("/")
    return key


def _register_staged_file(knex, project_pk: str, payload: dict) -> dict:
    """POST to the register_staged_file endpoint and return the JSON response."""
    return knex.post(
        f"sample_groups/{project_pk}/register_staged_file",
        json=payload,
    )


def _format_register_summary(staged_key: str, result: dict) -> str:
    """Return a human-readable summary string for a successful registration."""
    lines = [
        f"Registered: {staged_key}",
        f"  Sample:   {result.get('sample_name', '')}",
        f"  Module:   {result.get('module_name', '')}",
        f"  Field:    {result.get('field_name', '')}",
        f"  UUID:     {result.get('uuid', '')}",
        f"  URI:      {result.get('s3_uri', '')}",
    ]
    return "\n".join(lines)


@cli_s3.command("register")
@use_common_state
@click.option("--sample", required=True, help="Sample name.")
@click.option("--module", required=True, help="Module name.")
@click.option("--field", required=True, help="Field name.")
@click.option(
    "--file-size",
    type=int,
    default=None,
    help="File size in bytes.",
)
@click.option(
    "--replicate",
    type=int,
    default=1,
    show_default=True,
    help="Replicate number.",
)
@project_id_arg
@click.argument("staged_path")
def cli_s3_register(state, sample, module, field, file_size, replicate, project_id, staged_path):
    """Register a staged S3 file as a sample result field for PROJECT.

    STAGED_PATH may be a bare S3 key or a full S3 URI:

    \b
    # Bare key
    $ geoseeq s3 register "My Org/My Project" staging/abc/foo.fastq.gz \\
        --sample my-sample --module kraken2 --field report

    \b
    # Full URI
    $ geoseeq s3 register "My Org/My Project" \\
        s3://geoseeq-v0-<uuid>/staging/abc/foo.fastq.gz \\
        --sample my-sample --module kraken2 --field report

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex().set_auth_required()
    try:
        proj = handle_project_id(knex, project_id, create=False)
    except (GeoseeqGeneralError, ValueError) as exc:
        click.echo(f"Error looking up project: {exc}", err=True)
        sys.exit(1)

    bare_key = _normalize_staged_path(staged_path)

    payload = {
        "staged_path": bare_key,
        "sample_name": sample,
        "module_name": module,
        "field_name": field,
        "replicate": replicate,
    }
    if file_size is not None:
        payload["file_size"] = file_size

    try:
        result = _register_staged_file(knex, proj.uuid, payload)
    except GeoseeqGeneralError as exc:
        click.echo(f"Error registering staged file: {exc}", err=True)
        sys.exit(1)

    click.echo(_format_register_summary(bare_key, result), file=state.outfile)


def _build_s3_client(creds: dict):
    """Construct a boto3 S3 client from staging credentials.

    Uses the endpoint_url, access_key_id, and secret_access_key provided
    by the staging credentials response so all I/O goes to the correct
    object-storage endpoint rather than AWS proper.
    """
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=creds["endpoint_url"],
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        region_name=creds.get("region", "us-east-1"),
    )


def _upload_file(s3_client, local_path: str, bucket: str, key: str) -> None:
    """Upload *local_path* to *bucket*/*key* using multipart for large files.

    Files larger than 100 MB are automatically uploaded via multipart;
    smaller files use a single PUT.
    """
    from boto3.s3.transfer import TransferConfig

    config = TransferConfig(multipart_threshold=100 * 1024 * 1024)
    s3_client.upload_file(local_path, bucket, key, Config=config)


@cli_s3.command("upload-and-register")
@use_common_state
@click.option("--sample", required=True, help="Sample name.")
@click.option("--module", required=True, help="Module name.")
@click.option("--field", required=True, help="Field name.")
@click.option(
    "--replicate",
    type=int,
    default=1,
    show_default=True,
    help="Replicate number.",
)
@project_id_arg
@click.argument("local_file")
def cli_s3_upload_and_register(state, sample, module, field, replicate, project_id, local_file):
    """Upload LOCAL_FILE to S3 staging and register it as a result field for PROJECT.

    Fetches temporary S3 credentials, uploads the file directly via boto3
    (no aws CLI required), then registers the staged object with the API.

    If the upload succeeds but registration fails the staged S3 key is
    printed to stdout so you can retry with ``geoseeq s3 register``.

    \b
    # Upload and register a FASTQ file
    $ geoseeq s3 upload-and-register "My Org/My Project" reads.fastq.gz \\
        --sample my-sample --module kraken2 --field report

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex().set_auth_required()

    try:
        proj = handle_project_id(knex, project_id, create=False)
    except (GeoseeqGeneralError, ValueError) as exc:
        click.echo(f"Error looking up project: {exc}", err=True)
        sys.exit(1)

    try:
        creds = _fetch_staging_credentials(knex, proj.uuid)
    except GeoseeqGeneralError as exc:
        click.echo(f"Error fetching staging credentials: {exc}", err=True)
        sys.exit(1)

    _emit_warnings(creds)

    filename = os.path.basename(local_file)
    staged_key = creds["staging_prefix"] + filename
    bucket = creds["bucket_name"]

    s3_client = _build_s3_client(creds)

    try:
        click.echo(f"Uploading {filename}...", file=state.outfile)
        _upload_file(s3_client, local_file, bucket, staged_key)
        click.echo("Upload complete.", file=state.outfile)
    except OSError as exc:
        click.echo(f"Error uploading file: {exc}", err=True)
        sys.exit(1)
    except Exception as exc:  # covers botocore.exceptions.ClientError and network errors
        click.echo(f"Error uploading file: {exc}", err=True)
        sys.exit(1)

    payload = {
        "staged_path": staged_key,
        "sample_name": sample,
        "module_name": module,
        "field_name": field,
        "replicate": replicate,
    }

    try:
        result = _register_staged_file(knex, proj.uuid, payload)
    except GeoseeqGeneralError as exc:
        click.echo(f"Error registering staged file: {exc}", err=True)
        click.echo(f"Staged key (use 'geoseeq s3 register' to retry): {staged_key}", err=True)
        click.echo(staged_key, file=state.outfile)
        sys.exit(1)

    click.echo(_format_register_summary(staged_key, result), file=state.outfile)


def _format_ls_row(size_width: int, entry: dict) -> str:
    """Return a single formatted row for the ls listing.

    Format mirrors ``aws s3 ls``:
        YYYY-MM-DD HH:MM:SS  <right-aligned size>  <s3_key>

    ``size_width`` is the width of the widest size value across all rows so
    that the size column is right-aligned and consistent.
    """
    raw_ts = entry.get("last_modified", "")
    timestamp = raw_ts[:19].replace("T", " ") if raw_ts else " " * 19
    size = entry.get("size", 0)
    key = entry.get("key", "")
    return f"{timestamp}  {size:>{size_width}}  {key}"


@cli_s3.command("ls")
@use_common_state
@click.option(
    "--all-users",
    is_flag=True,
    default=False,
    help="List files for all users (not yet supported by the API).",
)
@project_id_arg
def cli_s3_ls(state, all_users, project_id):
    """List staged S3 files for PROJECT.

    Calls GET /sample_groups/{pk}/list_staged_files and prints a tabular
    listing similar to ``aws s3 ls``.

    \b
    # List staged files for "My Org/My Project"
    $ geoseeq s3 ls "My Org/My Project"

    ---

    Command Arguments:

    [PROJECT_ID] is the name or ID of the project whose staged files to list.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    if all_users:
        click.echo(
            "--all-users is not yet supported; listing your files only.",
            err=True,
        )

    knex = state.get_knex().set_auth_required()
    try:
        proj = handle_project_id(knex, project_id, create=False)
    except (GeoseeqGeneralError, ValueError) as exc:
        click.echo(f"Error looking up project: {exc}", err=True)
        sys.exit(1)

    try:
        entries = knex.get(f"sample_groups/{proj.uuid}/list_staged_files")
    except GeoseeqGeneralError as exc:
        click.echo(f"Error listing staged files: {exc}", err=True)
        sys.exit(1)

    if not entries:
        click.echo("No files in staging area.", file=state.outfile)
        return

    size_width = max(len(str(e.get("size", 0))) for e in entries)
    for entry in entries:
        click.echo(_format_ls_row(size_width, entry), file=state.outfile)
