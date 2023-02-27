import logging

import click

from .. import Organization
from .utils import use_common_state

logger = logging.getLogger('geoseeq_api')


@click.group('delete')
def cli_delete():
    """Delete objects from GeoSeeq."""
    pass


@cli_delete.command('samples')
@use_common_state
@click.option('-c/-n', '--confirm/--no-confirm', default=True)
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('sample_names', nargs=-1)
def cli_delete_samples(state, confirm, org_name, grp_name, sample_names):
    """Delete a list of samples in the specified group."""
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    grp = org.sample_group(grp_name).get()
    for sample_name in sample_names:
        sample = grp.sample(sample_name).get()
        if confirm and click.confirm(f'Delete Sample {sample}?'):
            sample.delete()
            logger.info(f'Deleted Sample {sample}')
