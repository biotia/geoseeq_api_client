import click

from .upload import (
    cli_upload_file,
    cli_metadata,
)
from .upload_reads import cli_upload_reads_wizard


@click.group('upload')
def cli_upload():
    """Upload files to GeoSeeq."""
    pass

cli_upload.add_command(cli_upload_reads_wizard)
cli_upload.add_command(cli_upload_file)
cli_upload.add_command(cli_metadata)
