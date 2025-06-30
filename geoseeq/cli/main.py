import logging

import click

from geoseeq.knex import DEFAULT_ENDPOINT
from geoseeq.utils import set_profile

from .copy import cli_copy
from .detail import cli_detail
from .download import cli_download
from .find_grn import cli_find_grn
from .get_eula import cli_eula
from .manage import cli_manage
from .run import cli_app
from .search import cli_search
from .shared_params.opts_and_args import overwrite_option, yes_option
from .upload import cli_upload, cli_upload_advanced
from .user import cli_user
from .view import cli_view

logger = logging.getLogger("geoseeq_api")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s :: %(message)s"))
logger.addHandler(handler)


@click.group(context_settings={"show_default": True})
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
main.add_command(cli_find_grn)


@main.command()
def version():
    """Print the version of the Geoseeq API being used.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    click.echo("0.7.3dev0")  # remember to update pyproject.toml


@main.group("advanced")
def cli_advanced():
    """Advanced commands."""
    pass


cli_advanced.add_command(cli_copy)
cli_advanced.add_command(cli_user)
cli_advanced.add_command(cli_detail)
cli_advanced.add_command(cli_upload_advanced)


@cli_advanced.group("experimental")
def cli_experimental():
    """Experimental commands."""
    pass


try:
    from geoseeq.vc.cli import cli_vc

    from .project import cli_project

    cli_experimental.add_command(cli_vc)
    cli_advanced.add_command(cli_project)

except (ModuleNotFoundError, ImportError):
    pass


@main.command("config")
@yes_option
@click.option("--api-token", default=None, help="The API token to use.")
@click.option("--endpoint", default=None, help="The endpoint to use.")
@click.option("-p", "--profile", default=None, help="The profile name to use.")
@overwrite_option
def cli_config(yes, api_token, endpoint, profile, overwrite):
    """Configure the GeoSeeq API.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    if not profile and not yes:
        profile = click.prompt(
            f"Set custom profile name? (Leave blank for default)", default=""
        ).strip(" \"'")
    if not endpoint:
        endpoint = DEFAULT_ENDPOINT
        if not yes:
            endpoint = click.prompt(
                f"Enter the URL to use for GeoSeeq (Most users can use the default)",
                default=DEFAULT_ENDPOINT,
            ).strip(" \"'")
    if not api_token:
        api_token = click.prompt(
            f"Enter your GeoSeeq API token", hide_input=True
        ).strip(" \"'")
    if not yes:
        eula_accepted = click.confirm(
            (
                "Have you read and accepted the GeoSeeq End User License "
                "Agreement? Use `geoseeq eula show` to view the EULA."
            )
        )
        if not eula_accepted:
            click.echo("You must accept the EULA to use the GeoSeeq API.")
            return
    set_profile(api_token, endpoint=endpoint, profile=profile, overwrite=overwrite)
    click.echo(f"Profile configured.")


@main.command("clear-cache")
@yes_option
def cli_clear_cache(yes):
    """Clear the local cache.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    import shutil

    from geoseeq.file_system_cache import GEOSEEQ_CACHE_DIR

    if yes or click.confirm("Are you sure you want to clear the cache?"):
        shutil.rmtree(GEOSEEQ_CACHE_DIR, ignore_errors=True)
