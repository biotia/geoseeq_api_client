
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
logger.addHandler(logging.StreamHandler())


@click.group()
def main():
    pass


@main.command()
def version():
    """Print the version of the Geoseeq API being used."""
    click.echo('0.1.6')  # remember to update setup



main.add_command(cli_add)
main.add_command(cli_create)
main.add_command(cli_download)
main.add_command(cli_list)
main.add_command(cli_upload)
main.add_command(cli_user)
main.add_command(cli_delete)
main.add_command(cli_copy)
main.add_command(cli_vc)
main.add_command(cli_view)
