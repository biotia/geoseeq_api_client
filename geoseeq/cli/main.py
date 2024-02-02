
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
from .run import cli_app
from .get_eula import cli_eula

logger = logging.getLogger('geoseeq_api')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(levelname)s] %(name)s :: %(message)s'))
logger.addHandler(handler)


@click.group()
def main():
    """Command line interface for the GeoSeeq API.
    
    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    pass

main.add_command(cli_download)
main.add_command(cli_upload)
main.add_command(cli_manage)
main.add_command(cli_view)
main.add_command(cli_search)
main.add_command(cli_app)
main.add_command(cli_eula)

@main.command()
def version():
    """Print the version of the Geoseeq API being used.

    ---
    
    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    click.echo('0.4.1')  # remember to update setup


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
    """Configure the GeoSeeq API.

    ---
    
    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    if not profile:
        profile = click.prompt(f'Set custom profile name? (Leave blank for default)', default="").strip(' \"\'')
    endpoint = click.prompt(f'Enter the URL to use for GeoSeeq (Most users can use the default)', default=DEFAULT_ENDPOINT).strip(' \"\'')
    api_token = click.prompt(f'Enter your GeoSeeq API token', hide_input=True).strip(' \"\'')
    eula_accepted = click.confirm(f'Have you read and accepted the GeoSeeq End User License Agreement? Use `geoseeq eula show` to view the EULA.')
    if not eula_accepted:
        click.echo('You must accept the EULA to use the GeoSeeq API.')
        return
    set_profile(api_token, endpoint=endpoint, profile=profile, overwrite=overwrite)
    click.echo(f'Profile configured.')