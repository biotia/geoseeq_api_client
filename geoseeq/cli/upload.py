
import json

import click
import pandas as pd

from geoseeq.knex import GeoseeqNotFoundError
from multiprocessing import Pool

from .. import Organization
from .constants import *
from .fastq_utils import group_paired_end_paths, upload_fastq_pair, upload_single_fastq
from .utils import use_common_state


@click.group('upload')
def cli_upload():
    """Upload files to GeoSeeq."""
    pass

dryrun_option = click.option('--dryrun/--wetrun', default=False, help='Print what will be created without actually creating it')
overwrite_option = click.option('--overwrite/--no-overwrite', default=False, help='Overwrite existing samples and data')
module_option = lambda x: click.option('-m', '--module-name', type=click.Choice(x), default=x[0], help='Name for the module that will store the data')
private_option = click.option('--private/--public', default=True, help='Make the reads private.')
link_option = click.option(
    '--link-type',
    default='upload',
    type=click.Choice(['upload', 's3', 'ftp', 'azure', 'sra']),
    help='Link the files from a cloud storage service instead of copying them'
)
org_arg = click.argument('org_name')
project_arg = click.argument('project_name')
sample_arg = click.argument('sample_name')
module_arg = click.argument('module_name')
field_name = click.argument('field_name')


def _upload_one_sample(args):
    group, module_name, link_type, lib, filepaths, seq_length, overwrite = args
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
                field.upload_file(path)
            else:
                field.link_file(link_type, path)
        except Exception as e:
            return sample, False, e
    return sample, True, None


@cli_upload.command('reads')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@click.option('--overwrite/--no-overwrite', default=False, help='Overwrite existing files')
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@click.option('--regex', default=None, help='An optional regex to use to extract sample names from the file names')
@private_option
@link_option
@module_option(['short_read::paired_end', 'short_read::single_end', 'long_read::nanopore'])
@org_arg
@project_arg
@click.argument('file_list', type=click.File('r'))
def cli_upload_reads_wizard(state, cores, overwrite, yes, regex, private, link_type, module_name, org_name, project_name, file_list):
    """Upload read files to GeoSeeq.

    This command automatically groups files by their sample name, lane number and read number.

    It asks for user confirmation before creating any samples or data.

    `file_list` is a file with a list of fastq filepaths, one per line
    """
    # Set up the organization and library
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
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

    # Find a regex that will group the files into samples and tell the user if files did not match
    
    filepaths = {line.strip().split('/')[-1]: line.strip() for line in file_list if line.strip()}
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

    # Group files into samples, show the user what will be uploaded, and confirm
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

    # Upload the files
    args = [(group, module_name, link_type, lib, filepaths, seq_length, overwrite) for group in groups]
    with Pool(cores) as p:
        for sample, success, error in p.imap_unordered(_upload_one_sample, args):
            if success:
                click.echo(f'Uploaded Sample: {sample.name}', err=True)
            else:
                click.echo(f'Failed to upload Sample: {sample.name}', err=True)
                click.echo(f'Error:\n{error}', err=True)


@cli_upload.command('file')
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


@cli_upload.command('project-file')
@use_common_state
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@private_option
@link_option
@org_arg
@project_arg
@module_arg
@field_name
@click.argument('file_path', type=click.Path(exists=True))
def cli_upload_file(state, yes, private, link_type, org_name, project_name, module_name, field_name, file_path):
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
        field.upload_file(path)
    else:
        field.link_file(link_type, path)


@cli_upload.command('metadata')
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
