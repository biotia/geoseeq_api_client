
import logging

import click


from .copy import cli_copy
from .manage import cli_manage
from .download import cli_download
from .upload import cli_upload, cli_upload_advanced
from .user import cli_user
from .view import cli_view
from .search import cli_search
from geoseeq.vc.cli import cli_vc
from geoseeq.knex import DEFAULT_ENDPOINT
from .shared_params.config import set_profile
from .shared_params.opts_and_args import overwrite_option
from .detail import cli_detail

logger = logging.getLogger('geoseeq_api')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(levelname)s] %(name)s :: %(message)s'))
logger.addHandler(handler)


@click.group()
def main():
    pass

main.add_command(cli_download)
main.add_command(cli_upload)
main.add_command(cli_manage)
main.add_command(cli_view)
main.add_command(cli_search)

@main.command()
def version():
    """Print the version of the Geoseeq API being used."""
    click.echo('0.3.1')  # remember to update setup


@main.group('advanced')
def cli_advanced():
    """Advanced commands."""
    pass

cli_advanced.add_command(cli_copy)
cli_advanced.add_command(cli_user)
cli_advanced.add_command(cli_detail)
cli_advanced.add_command(cli_upload_advanced)

@cli_advanced.group('experimental')
def cli_experimental():
    """Experimental commands."""
    pass

cli_experimental.add_command(cli_vc)

@main.command('config')
@click.option('-p', '--profile', default=None, help='The profile name to use.')
@overwrite_option
def cli_config(profile, overwrite):
    """Configure the GeoSeeq API."""
    if not profile:
        profile = click.prompt(f'Set custom profile name? (Leave blank for default)', default="").strip(' \"\'')
    endpoint = click.prompt(f'Enter the URL to use for GeoSeeq (Most users can use the default)', default=DEFAULT_ENDPOINT).strip(' \"\'')
    api_token = click.prompt(f'Enter your GeoSeeq API token', hide_input=True).strip(' \"\'')
    set_profile(api_token, endpoint=endpoint, profile=profile, overwrite=overwrite)
    click.echo(f'Profile configured.')