import click

from .upload import (
    cli_upload_file,
    cli_upload_folder,
    cli_metadata,
    cli_upload_smart_table,
    cli_upload_smart_tree,
)
from .upload_reads import cli_upload_reads_wizard
from .upload_advanced import cli_find_urls_for_reads, cli_upload_from_config

@click.group('upload')
def cli_upload():
    """Upload files to GeoSeeq."""
    pass

cli_upload.add_command(cli_upload_reads_wizard)
cli_upload.add_command(cli_upload_file)
cli_upload.add_command(cli_upload_folder)
cli_upload.add_command(cli_metadata)
cli_upload.add_command(cli_upload_smart_table)
cli_upload.add_command(cli_upload_smart_tree)

@cli_upload.group('advanced')
def cli_upload_advanced():
    """Advanced tools to upload files to GeoSeeq."""
    pass

cli_upload_advanced.add_command(cli_find_urls_for_reads)
cli_upload_advanced.add_command(cli_upload_from_config)