import logging
import json

import click
import pandas as pd
import requests
from geoseeq.knex import GeoseeqNotFoundError
from multiprocessing import Pool, current_process

from geoseeq import Organization
from geoseeq.cli.constants import *
from geoseeq.cli.fastq_utils import group_paired_end_paths, upload_fastq_pair, upload_single_fastq
from geoseeq.cli.utils import use_common_state

from .opts_and_args import *

logger = logging.getLogger('geoseeq_api')


@click.command('file')
@use_common_state
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@private_option
@link_option
@org_arg
@project_arg
@sample_arg
@module_arg
@field_name
@click.argument('file_path', type=click.Path(exists=True))
def cli_upload_file(state, yes, private, link_type, org_name, project_name, sample_name, module_name, field_name, file_path):
    """Upload a single file to a sample field."""
    knex = state.get_knex()
    try:
        org = Organization(knex, org_name).get()
    except GeoseeqNotFoundError:
        if not yes:
            click.confirm(f'Organization "{org_name}" does not exist. Create it?', abort=True)
        org = Organization(knex, org_name).create()
    try:
        lib = org.sample_group(project_name).get()
    except GeoseeqNotFoundError:
        if not yes:
            click.confirm(f'Project "{project_name}" does not exist. Create it?', abort=True)
        lib = org.sample_group(project_name, is_public=not private).create()
    sample = lib.sample(sample_name).idem()
    module = sample.analysis_result(module_name).idem()
    field = module.field(field_name).idem()
    if link_type == 'upload':
        field.upload_file(path)
    else:
        field.link_file(link_type, path)


@click.command('project-file')
@use_common_state
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@private_option
@link_option
@org_arg
@project_arg
@module_arg
@field_name
@click.argument('file_path', type=click.Path(exists=True))
def cli_upload_project_file(state, yes, private, link_type, org_name, project_name, module_name, field_name, file_path):
    """Upload a single file to a project field."""
    knex = state.get_knex()
    try:
        org = Organization(knex, org_name).get()
    except GeoseeqNotFoundError:
        if not yes:
            click.confirm(f'Organization "{org_name}" does not exist. Create it?', abort=True)
        org = Organization(knex, org_name).create()
    try:
        lib = org.sample_group(project_name).get()
    except GeoseeqNotFoundError:
        if not yes:
            click.confirm(f'Project "{project_name}" does not exist. Create it?', abort=True)
        lib = org.sample_group(project_name, is_public=not private).create()
    module = lib.analysis_result(module_name).idem()
    field = module.field(field_name).idem()
    if link_type == 'upload':
        field.upload_file(file_path)
    else:
        field.link_file(link_type, file_path)


@click.command('metadata')
@use_common_state
@overwrite_option
@click.option('--create/--no-create', default=False)
@click.option('--update/--no-update', default=False)
@click.option('--index-col', default=0)
@click.option('--encoding', default='utf_8')
@org_arg
@project_arg
@click.argument('table', type=click.File('rb'))
def cli_metadata(state, overwrite,
                 create, update, index_col, encoding,
                 org_name, project_name, table):
    knex = state.get_knex()
    tbl = pd.read_csv(table, index_col=index_col, encoding=encoding)
    tbl.index = tbl.index.to_series().map(str)
    org = Organization(knex, org_name).get()
    lib = org.sample_group(project_name).get()
    for sample_name, row in tbl.iterrows():
        sample = lib.sample(sample_name)
        if create:
            sample = sample.idem()
        else:
            try:
                sample = sample.get()
            except Exception as e:
                click.echo(f'Sample "{sample.name}" not found.', err=True)
                continue
        new_meta = json.loads(json.dumps(row.dropna().to_dict())) 
        if overwrite or (not sample.metadata):
            sample.metadata = new_meta
            sample.idem()
        elif update:
            old_meta = sample.metadata
            old_meta.update(new_meta)
            sample.metadata = old_meta
            sample.idem()
        click.echo(sample)
