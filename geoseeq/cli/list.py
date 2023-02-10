import click

from .. import Organization
from .utils import use_common_state


@click.group('list')
def cli_list():
    """List objects on GeoSeeq."""
    pass


@cli_list.command('samples')
@use_common_state
@click.argument('org_name')
@click.argument('grp_name')
def cli_list_samples(state, org_name, grp_name):
    """Print a list of samples in the specified group."""
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    grp = org.sample_group(grp_name).get()
    for sample in grp.get_samples():
        print(sample, file=state.outfile)
