
import logging

import click

from .add import cli_add
from .copy import cli_copy
from .create import cli_create
from .delete import cli_delete
from .download import cli_download
from .list import cli_list
from .upload import cli_upload
from .user import cli_user
from .view import cli_view
from geoseeq.vc.cli import cli_vc

logger = logging.getLogger('geoseeq_api')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(levelname)s] %(name)s :: %(message)s'))
logger.addHandler(handler)


@click.group()
def main():
    pass

main.add_command(cli_download)
main.add_command(cli_upload)


@main.command()
def version():
    """Print the version of the Geoseeq API being used."""
    click.echo('0.2.2')  # remember to update setup


@main.group('advanced')
def cli_advanced():
    """Advanced commands."""
    pass

cli_advanced.add_command(cli_view)
cli_advanced.add_command(cli_add)
cli_advanced.add_command(cli_create)
cli_advanced.add_command(cli_list)
cli_advanced.add_command(cli_delete)
cli_advanced.add_command(cli_copy)
cli_advanced.add_command(cli_user)

@cli_advanced.group('experimental')
def cli_experimental():
    """Experimental commands."""
    pass

cli_experimental.add_command(cli_vc)

