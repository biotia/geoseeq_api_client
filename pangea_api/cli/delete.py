import click
import logging
import json
import pandas as pd

from requests.exceptions import HTTPError
from os import environ
from os.path import join, dirname
from os import makedirs

from .. import (
    Knex,
    User,
    Organization,
)
from .utils import use_common_state


logger = logging.getLogger('pangea_api')


@click.group('delete')
def cli_delete():
    pass


@cli_delete.command('samples')
@use_common_state
@click.option('-c/-n', '--confirm/--no-confirm', default=True)
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('sample_names', nargs=-1)
def cli_delete_samples(state, confirm, org_name, grp_name, sample_names):
    """Print a list of samples in the specified group."""
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    grp = org.sample_group(grp_name).get()
    for sample_name in sample_names:
        sample = grp.sample(sample_name).get()
        if confirm and click.confirm(f'Delete Sample {sample}?'):
            sample.delete()
            logger.info(f'Deleted Sample {sample}')
        
