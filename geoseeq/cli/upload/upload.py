import logging
import json

import click
import pandas as pd
import requests
from os.path import basename
from geoseeq.knex import GeoseeqNotFoundError
from multiprocessing import Pool, current_process

from geoseeq import Organization
from geoseeq.cli.constants import *
from geoseeq.cli.fastq_utils import group_paired_end_paths, upload_fastq_pair, upload_single_fastq
from geoseeq.cli.progress_bar import PBarManager
from geoseeq.cli.shared_params import (
    use_common_state,
    yes_option,
    private_option,
    link_option,
    folder_id_arg,
    handle_folder_id,
    overwrite_option,
    project_id_arg,
    handle_project_id,
)

logger = logging.getLogger('geoseeq_api')


@click.command('files')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@yes_option
@private_option
@link_option
@click.option('-n', '--geoseeq-file-name', default=None, multiple=True,
              help='Specify a different name for the file on GeoSeeq than the local file name')
@folder_id_arg
@click.argument('file_paths', type=click.Path(exists=True), nargs=-1)
def cli_upload_file(state, cores, yes, private, link_type, geoseeq_file_name, folder_id, file_paths):
    """Upload files to GeoSeeq.

    This command uploads files to either a sample or project on GeoSeeq. It can be used to upload
    multiple files to the same folder at once.
    
    ---

    Example Usage:

    \b
    # Upload file.txt to a sample
    $ geoseeq upload files "My Org/My Project/My Sample/My Folder" /path/to/file.txt

    \b
    # Upload file.txt to a sample but name it "My File" on GeoSeeq
    $ geoseeq upload files "My Org/My Project/My Sample/My Folder" /path/to/file.txt -n "My File"

    \b
    # Upload file.txt to a project
    $ geoseeq upload files "My Org/My Project/My Folder" /path/to/file.txt

    \b
    # Upload multiple files to a project
    $ geoseeq upload files "My Org/My Project/My Folder" /path/to/file1.txt /path/to/file2.txt

    \b
    # Upload multiple files to a project but name them differently on GeoSeeq
    $ geoseeq upload files "My Org/My Project/My Folder" /path/to/file1.txt /path/to/file2.txt -n "File 1" -n "File 2"

    ---

    Command Arguments:

    [FOLDER_ID] Can be a folder UUID, GeoSeeq Resource Number (GRN), or an
    names for an org, project, sample, folder separated by a slash. Can exclude
    the sample name if the folder is for a project.

    [FILE_PATHS]... One or more paths to files on your local machine.

    ---
    """
    knex = state.get_knex()
    result_folder = handle_folder_id(knex, folder_id, yes=yes, private=private, create=True)
    if geoseeq_file_name:
        if len(geoseeq_file_name) != len(file_paths):
            raise click.UsageError('Number of --geoseeq-file-name arguments must match number of file_paths')
        name_pairs = zip(geoseeq_file_name, file_paths)
    else:
        name_pairs = zip([basename(fp) for fp in file_paths], file_paths)
    
    pbars = PBarManager()
    for geoseeq_file_name, file_path in name_pairs:
        field = result_folder.result_file(geoseeq_file_name).idem()
        if link_type == 'upload':
            field.upload_file(file_path, progress_tracker=pbars.get_new_bar(file_path))
        else:
            field.link_file(link_type, file_path)



@click.command('metadata')
@use_common_state
@overwrite_option
@yes_option
@private_option
@click.option('--create/--no-create', default=False, help='Create samples if they have metadata but do not exist on GeoSeeq.')
@click.option('--index-col', default=0)
@click.option('--encoding', default='utf_8')
@project_id_arg
@click.argument('table', type=click.File('rb'))
def cli_metadata(state, overwrite, yes, private, create, index_col, encoding, project_id, table):
    """Upload sample metadata to a project on GeoSeeq.
    
    This command takes a CSV file with one row per sample and one column per metadata field.
    
    ---

    Example Usage:

    \b
    # Upload metadata from a metadata.csv to a project
    $ cat metadata.csv
        sample_name,collection_date,location
        sample1,2020-01-01,USA
        sample2,2020-01-02,USA
        sample3,2020-01-03,CAN
    $ geoseeq upload metadata "My Org/My Project" metadata.csv

    \b
    # Modify metadata for existing samples
    $ cat fixed_metadata.csv
        sample_name,collection_date,location
        sample1,2020-01-01,CAN
    $ geoseeq upload metadata --overwrite "My Org/My Project" fixed_metadata.csv

    ---

    Command Arguments:

    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.

    [TABLE] A CSV file with one row per sample and one column per metadata field.

    ---
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes, private)
    tbl = pd.read_csv(table, index_col=index_col, encoding=encoding)
    samples = []
    plan = {
        'overwrite': 0,
        'new': 0,
        'no_change': 0,
        'no_metadata': 0,
    }
    for sample_name, row in tbl.iterrows():
        sample = proj.sample(sample_name)
        if create:
            sample = sample.idem()
        else:
            sample = sample.get()
        new_meta = json.loads(json.dumps(row.dropna().to_dict()))
        if new_meta:
            if sample.metadata == new_meta:
                plan['no_change'] += 1
            elif overwrite and sample.metadata:
                plan['overwrite'] += 1
                samples.append((sample, new_meta))
            elif not sample.metadata:
                plan['new'] += 1
                samples.append((sample, new_meta))
        else:
            plan['no_metadata'] += 1


    
    if not yes:
        click.echo('Plan:')
        click.echo(f'{plan["overwrite"]} samples will have their metadata overwritten')
        click.echo(f'{plan["new"]} samples will have metadata added')
        click.echo(f'{plan["no_change"]} samples have metadata that already matches the new metadata to upload')
        click.echo(f'{plan["no_metadata"]} samples have no metadata to upload')
        click.confirm('Continue?', abort=True)

    for sample, new_meta in samples:
        if overwrite or (not sample.metadata):
            sample.metadata = new_meta
            sample.idem()
    click.echo(f'Wrote metadata for {len(samples)} samples')
