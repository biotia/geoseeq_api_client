import logging
import json

import click
import pandas as pd
import requests
from os.path import basename

from multiprocessing import Pool, current_process

from geoseeq.cli.constants import *
from geoseeq.cli.fastq_utils import group_paired_end_paths, upload_fastq_pair, upload_single_fastq
from geoseeq.cli.utils import use_common_state
from geoseeq.cli.shared_params import handle_project_id


from .opts_and_args import *

logger = logging.getLogger('geoseeq_api')


def _upload_one_sample(args):
    group, module_name, link_type, lib, filepaths, overwrite, session, log_level = args
    seq_length, seq_type = module_name.split('::')[:2]
    logger = logging.getLogger('geoseeq_api')
    logger.setLevel(log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(levelname)s] %(name)s :: ' + current_process().name + ' :: %(message)s'))
    logger.addHandler(handler)
    sample = lib.sample(group['sample_name']).idem()
    module = sample.analysis_result(module_name).idem()
    for field_name, path in group['fields'].items():
        field = module.field(f'{seq_length}::{field_name}')
        if field.exists() and not overwrite:  # TODO: check checksums to see if the file is the same
            continue
        field = field.idem()
        path = filepaths[path]
        try:
            if link_type == 'upload':
                field.upload_file(path, session=session)
            else:
                field.link_file(link_type, path)
        except Exception as e:
            return sample, False, e
    return sample, True, None


def _get_regex(knex, filepaths, module_name, lib, regex):
    """Return a regex that will group the files into samples
    
    Tell the user how many files did could not be matched using the regex.
    """
    seq_length, seq_type = module_name.split('::')[:2]
    args = {
        'filenames': list(filepaths.keys()),
        'sequence_type': seq_type,
        'sample_group_id': lib.uuid,
    }
    if regex:
        args['custom_regex'] = regex
    result = knex.post('bulk_upload/validate_filenames', json=args)
    regex = result['regex_used']
    click.echo(f'Using regex: "{regex}"', err=True)
    if result['unmatched']:
        click.echo(f'{len(result["unmatched"])} files could not be grouped.', err=True)
    else:
        click.echo('All files successfully grouped.', err=True)
    return regex


def _group_files(knex, filepaths, module_name, regex, yes):
    """Group the files into samples, confirm, and return the groups."""
    seq_length, seq_type = module_name.split('::')[:2]
    groups = knex.post('bulk_upload/group_files', json={
        'filenames': list(filepaths.keys()),
        'sequence_type': seq_type,
        'regex': regex
    })
    for group in groups:
        click.echo(f'sample_name: {group["sample_name"]}', err=True)
        click.echo(f'  module_name: {module_name}', err=True)
        for field_name, filename in group['fields'].items():
            path = filepaths[filename]
            click.echo(f'    {seq_length}::{field_name}: {path}', err=True)
    if not yes:
        click.confirm('Do you want to upload these files?', abort=True)
    return groups


def _do_upload(groups, module_name, link_type, lib, filepaths, overwrite, cores, state):
    def handle_upload(sample, success, error):
        if success:
            click.echo(f'Uploaded Sample: {sample.name}', err=True)
        else:
            click.echo(f'Failed to upload Sample: {sample.name}', err=True)
            click.echo(f'Error:\n{error}', err=True)

    with requests.Session() as session:
        args = [
            (group, module_name, link_type, lib, filepaths, overwrite, session, state.log_level)
            for group in groups
        ]
        if cores == 1:  # Don't use multiprocessing if we only have one core, makes debugging easier
            for arg in args:
                sample, success, error = _upload_one_sample(arg)
                handle_upload(sample, success, error)
        else:
            with Pool(cores) as p:
                for sample, success, error in p.imap_unordered(_upload_one_sample, args):
                    handle_upload(sample, success, error)


@click.command('reads')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@click.option('--overwrite/--no-overwrite', default=False, help='Overwrite existing files')
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@click.option('--regex', default=None, help='An optional regex to use to extract sample names from the file names')
@private_option
@link_option
@module_option(['short_read::paired_end', 'short_read::single_end', 'long_read::nanopore'])
@click.argument('project_id', nargs=-1)
@click.argument('file_list', type=click.File('r'))
def cli_upload_reads_wizard(state, cores, overwrite, yes, regex, private, link_type, module_name, project_id, file_list):
    """Upload fastq read files to GeoSeeq.

    This command automatically groups files by their sample name, lane number
    and read number. It asks for confirmation before creating any samples or
    data.
    
    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name pair.

    \b
    Examples: 
     - Name pair: "GeoSeeq" "Example CLI Project"
     - UUID: "ed59b913-91ec-489b-a1b9-4ea137a6e5cf"
     - GRN: "grn:gs1:project:ed59b913-91ec-489b-a1b9-4ea137a6e5cf"

    FILE_LIST is a file with a list of fastq filepaths, one per line
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes, private)
    filepaths = {basename(line): line for line in (l.strip() for l in file_list) if line}
    regex = _get_regex(knex, filepaths, module_name, proj, regex)
    groups = _group_files(knex, filepaths, module_name, regex, yes)
    _do_upload(groups, module_name, link_type, proj, filepaths, overwrite, cores, state)
