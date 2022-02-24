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


@click.group('copy')
def cli_copy():
    pass


@cli_copy.command('group')
@click.option('-l', '--log-level', type=int, default=20, envvar='PANGEA_CLI_LOG_LEVEL')
@click.option('-o', '--outfile', type=click.File('w'), default='-')
@click.option('--source-email', envvar='PANGEA_USER')
@click.option('--source-password', envvar='PANGEA_PASS')
@click.option('--target-email', envvar='PANGEA_USER')
@click.option('--target-password', envvar='PANGEA_PASS')
@click.option('--source-endpoint', default='https://pangeabio.io')
@click.option('--target-endpoint', default='https://pangeabio.io')
@click.argument('source_org_name')
@click.argument('source_grp_name')
@click.argument('target_org_name')
@click.argument('target_grp_name')
def cli_list_samples(log_level, outfile,
                     source_email, source_password, target_email, target_password,
                     source_endpoint, target_endpoint,
                     source_org_name, source_grp_name, target_org_name, target_grp_name):
    """Copy a group and its samples from one pangea instance to another."""
    source_knex = Knex(source_endpoint)
    if source_email and source_password:
        User(source_knex, source_email, source_password).login()
    source_org = Organization(source_knex, source_org_name).get()
    source_grp = source_org.sample_group(source_grp_name).get()

    target_knex = Knex(target_endpoint)
    if target_email and target_password:
        User(target_knex, target_email, target_password).login()
    target_org = Organization(target_knex, target_org_name).idem()
    target_grp = target_org.sample_group(target_grp_name).idem()

    for source_ar in source_grp.get_analysis_results():
        target_ar = source_ar.copy(target_grp, save=True)
        print(source_ar, target_ar, file=outfile)

    for source_sample in source_grp.get_samples():
        target_sample = source_sample.copy(target_grp, save=True)
        print(source_sample, target_sample, file=outfile)

