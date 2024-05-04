import logging
import json

import click
import pandas as pd
import requests
from os.path import basename, isdir, isfile, exists
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
    project_or_sample_id_arg,
    handle_project_or_sample_id,
    no_new_versions_option,
    ignore_errors_option,
)
from geoseeq.upload_download_manager import GeoSeeqUploadManager

logger = logging.getLogger('geoseeq_api')

recursive_option = click.option('--recursive/--no-recursive', default=True, help='Upload files in subfolders')
hidden_option = click.option('--hidden/--no-hidden', default=False, help='Upload hidden files in subfolders')


@click.command('files')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel', show_default=True)
@click.option('--threads-per-upload', default=4, help='Number of threads used to upload each file', show_default=True)
@click.option('--num-retries', default=3, help='Number of times to retry a failed upload', show_default=True)
@click.option('--chunk-size-mb', default=-1, help='Size of chunks to upload in MB', show_default=True)
@ignore_errors_option
@yes_option
@private_option
@link_option
@recursive_option
@hidden_option
@no_new_versions_option
@click.option('-n', '--geoseeq-file-name', default=None, multiple=True,
              help='Specify a different name for the file on GeoSeeq than the local file name.',
              show_default=True)
@folder_id_arg
@click.argument('file_paths', type=click.Path(exists=True), nargs=-1)
def cli_upload_file(state, cores, threads_per_upload, num_retries, chunk_size_mb, ignore_errors, yes, private, link_type, recursive, hidden, no_new_versions, geoseeq_file_name, folder_id, file_paths):
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

    \b
    # Upload all files in a local folder to a folder in a project
    $ geoseeq upload files "My Org/My Project/My Folder" /path/to/folder
    ---

    Command Arguments:

    [FOLDER_ID] Can be a folder UUID, GeoSeeq Resource Number (GRN), or an
    names for an org, project, sample, folder separated by a slash. Can exclude
    the sample name if the folder is for a project.

    [FILE_PATHS]... One or more paths to files on your local machine.

    ---
    """
    if num_retries < 1:
        raise click.UsageError('--num-retries must be at least 1')
    knex = state.get_knex()
    result_folder = handle_folder_id(knex, folder_id, yes=yes, private=private, create=True)
    if geoseeq_file_name:
        uploading_folders = sum([isdir(fp) for fp in file_paths])
        if uploading_folders:
            raise click.UsageError('Cannot use --geoseeq-file-name with recursive folder uploads')
        if len(geoseeq_file_name) != len(file_paths):
            raise click.UsageError('Number of --geoseeq-file-name arguments must match number of file_paths')
        name_pairs = zip(geoseeq_file_name, file_paths)
    else:
        name_pairs = zip([basename(fp) for fp in file_paths], file_paths)
    
    upload_manager = GeoSeeqUploadManager(
        n_parallel_uploads=cores,
        threads_per_upload=threads_per_upload,
        link_type=link_type,
        progress_tracker_factory=PBarManager().get_new_bar,
        log_level=state.log_level,
        no_new_versions=no_new_versions,
        use_cache=state.use_cache,
        num_retries=num_retries,
        ignore_errors=ignore_errors,
        use_atomic_upload=True,
        session=None, #knex.new_session(),
        chunk_size_mb=chunk_size_mb if chunk_size_mb > 0 else None,
    )
    for geoseeq_file_name, file_path in name_pairs:
        if isfile(file_path):
            upload_manager.add_local_file_to_result_folder(result_folder, file_path)
        elif isdir(file_path) and recursive:
            upload_manager.add_local_folder_to_result_folder(result_folder, file_path, recursive=recursive, hidden_files=hidden, prefix=file_path)
        elif isdir(file_path) and not recursive:
            raise click.UsageError('Cannot upload a folder without --recursive')
    click.echo(upload_manager.get_preview_string(), err=True)
    if not yes:
        click.confirm('Continue?', abort=True)
    logger.info(f'Uploading {len(upload_manager)} files to {result_folder}')
    upload_manager.upload_files()


@click.command('folders')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@yes_option
@private_option
@recursive_option
@hidden_option
@no_new_versions_option
@project_or_sample_id_arg
@click.argument('folder_names', type=click.Path(exists=True), nargs=-1)
def cli_upload_folder(state, cores, yes, private, recursive, hidden, no_new_versions, project_or_sample_id, folder_names):
    knex = state.get_knex()
    root_obj = handle_project_or_sample_id(knex, project_or_sample_id, yes=yes, private=private)
    upload_manager = GeoSeeqUploadManager(
        n_parallel_uploads=cores,
        link_type='upload',
        progress_tracker_factory=PBarManager().get_new_bar,
        log_level=logging.INFO,
        overwrite=True,
        use_cache=state.use_cache,
        no_new_versions=no_new_versions,
        use_atomic_upload=True,
    )
    for folder_name in folder_names:
        result_folder = root_obj.result_folder(folder_name).idem()
        upload_manager.add_local_folder_to_result_folder(result_folder, folder_name, recursive=recursive, hidden_files=hidden)
    click.echo(upload_manager.get_preview_string(), err=True)
    if not yes:
        click.confirm('Continue?', abort=True)
    logger.info(f'Uploading {len(upload_manager)} folders to {root_obj}')
    upload_manager.upload_files()


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
