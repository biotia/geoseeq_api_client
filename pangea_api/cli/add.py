import click
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


@click.group('add')
def cli_add():
    pass


@cli_add.command('samples-to-group')
@use_common_state
@click.option('--sample-manifest', type=click.File('r'),
              help='List of sample names to download from')
@click.argument('source_org_name')
@click.argument('source_grp_name')
@click.argument('destination_org_name')
@click.argument('destination_grp_name')
@click.argument('sample_names', nargs=-1)
def cli_add_samples(state, sample_manifest,
                    source_org_name, source_grp_name,
                    destination_org_name, destination_grp_name,
                    sample_names):
    """Add samples from the source group to the destination group."""
    knex = state.get_knex()
    source_org = Organization(knex, source_org_name).get()
    source_grp = source_org.sample_group(source_grp_name).get()
    dest_org = Organization(knex, destination_org_name).get()
    dest_grp = dest_org.sample_group(destination_grp_name).get()
    if sample_manifest:
        sample_names = set(sample_names) | set([el.strip() for el in sample_manifest if el])
    for sname in sample_names:
        sample = source_grp.sample(sname).get()
        dest_grp.add_sample(sample)
    dest_grp.save()
    click.echo(f'Added {len(sample_names)} to {destination_grp_name}')
