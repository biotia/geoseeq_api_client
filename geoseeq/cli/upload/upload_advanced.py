import logging
import click
import requests
from os.path import basename, getsize
from .upload_reads import (
    _make_in_process_logger,
    _get_regex,
    _group_files,
    flatten_list_of_fastxs,
)

from multiprocessing import Pool, current_process

from geoseeq.cli.constants import *
from geoseeq.cli.shared_params import (
    handle_project_id,
    private_option,
    module_option,
    project_id_arg,
    overwrite_option,
    yes_option,
    use_common_state,
)

from geoseeq.constants import FASTQ_MODULE_NAMES
from geoseeq.cli.progress_bar import PBarManager
import pandas as pd
from typing import Dict, Optional
from geoseeq.id_constructors.from_ids import (
    org_from_id,
    project_from_id,
    sample_from_id,
    result_folder_from_id,
    result_file_from_id,
)
from geoseeq.upload_download_manager import GeoSeeqUploadManager

logger = logging.getLogger('geoseeq_api')


def _keep_only_authentication_url_args(url):
    """Return a url with only the S3 authentication args"""
    root, args = url.split('?')
    args = args.split('&')
    args = [arg for arg in args if arg.startswith('AWSAccessKeyId=') or arg.startswith('Signature=')]
    return root + '?' + '&'.join(args)


def _get_url_for_one_file(args):
    """Return a tuple of the filepath and the url to upload it to"""
    result_file, filepath, overwrite, log_level = args
    _make_in_process_logger(log_level)
    if result_file.exists() and not overwrite:  
        return
    result_file = result_file.idem()
    file_size = getsize(filepath)
    _, urls = result_file._prep_multipart_upload(filepath, file_size, file_size + 1, {})
    url = _keep_only_authentication_url_args(urls['1'])
    return filepath, url


def _find_target_urls(groups, module_name, lib, filepaths, overwrite, cores, state):
    """Use GeoSeeq to get target urls for a set of files"""
    with requests.Session() as session:
        find_url_args = []
        for group in groups:
            sample = lib.sample(group['sample_name']).idem()
            read_folder = sample.result_folder(module_name).idem()

            for field_name, path in group['fields'].items():
                result_file = read_folder.read_file(field_name)
                filepath = filepaths[path]
                find_url_args.append((
                    result_file, filepath, overwrite, state.log_level
                ))

        with Pool(cores) as p:
            for (file_name, target_url) in p.imap_unordered(_get_url_for_one_file, find_url_args):
                yield file_name, target_url


@click.command('read-links')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@overwrite_option
@yes_option
@click.option('--regex', default=None, help='An optional regex to use to extract sample names from the file names')
@private_option
@module_option(FASTQ_MODULE_NAMES)
@project_id_arg
@click.argument('fastq_files', type=click.Path(exists=True), nargs=-1)
def cli_find_urls_for_reads(state, cores, overwrite, yes, regex, private, module_name, project_id, fastq_files):
    """Print a two column list with filenames and a target storage URL
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes, private)
    filepaths = {basename(line): line for line in flatten_list_of_fastxs(fastq_files)}
    click.echo(f'Found {len(filepaths)} files to upload.', err=True)
    regex = _get_regex(knex, filepaths, module_name, proj, regex)
    groups = _group_files(knex, filepaths, module_name, regex, yes)
    for file_name, target_url in _find_target_urls(groups, module_name, proj, filepaths, overwrite, cores, state):
        print(f'{file_name}\t{target_url}', file=state.outfile)


def _get_result_file_from_record_with_ids(knex, record: Dict) -> Dict:
    """Get all relevant objects from a record, handling GRNs, UUIDs, and absolute names without requiring parent objects.
    
    Returns a dict with 'org', 'project', 'sample', 'folder', and 'result_file' keys.
    Objects may be None if not needed/specified.
    Guaranteed that at least org is not None.
    """
    objects = {
        'org': None,
        'project': None,
        'sample': None,
        'folder': None,
        'result_file': None
    }

    # Try to get file directly - if it's a GRN/UUID we don't need parent objects
    try:
        objects['result_file'] = result_file_from_id(knex, record['filename'])
        objects['folder'] = objects['result_file'].folder
        if hasattr(objects['folder'], 'sample'):
            objects['sample'] = objects['folder'].sample
            objects['project'] = objects['sample'].project
        else:
            objects['project'] = objects['folder'].project
        objects['org'] = objects['project'].org
        return objects
    except ValueError:
        pass  # Not a GRN, UUID or abs name. Continue with normal flow

    # Try to get folder directly - if it's a GRN/UUID we don't need parent objects
    try:
        objects['folder'] = result_folder_from_id(knex, record['folder'])
        # Get parent objects from folder
        if hasattr(objects['folder'], 'sample'):
            objects['sample'] = objects['folder'].sample
            objects['project'] = objects['sample'].project
        else:
            objects['project'] = objects['folder'].project
        objects['org'] = objects['project'].org
        return objects
    except ValueError:
        pass  # Not a GRN, UUID or abs name. Continue with normal flow

    # Try to get sample directly if specified
    if pd.notna(record['sample']):
        try:
            objects['sample'] = sample_from_id(knex, record['sample'])
            objects['project'] = objects['sample'].project
            objects['org'] = objects['project'].org
            return objects
        except ValueError:
            pass  # Not a GRN, UUID or abs name. Continue with normal flow

    # Try to get project directly
    try:
        objects['project'] = project_from_id(knex, record['project'])
        objects['org'] = objects['project'].org
        return objects
    except ValueError:
        pass  # Not a GRN/UUID, continue

    
    if objects['org'] is None: # Get org directly if we don't have one yet
        objects['org'] = org_from_id(knex, record['organization'])
    
    return objects


def _get_result_file_from_record(knex, record: Dict) -> Dict:
    """Get all relevant objects from a record, handling GRNs/UUIDs without requiring parent objects.
    
    Returns a dict with 'org', 'project', 'sample', 'folder', and 'result_file' keys.
    Objects may be None if not needed/specified.
    """
    objects = _get_result_file_from_record_with_ids(knex, record)

    if objects['project'] is None:
        objects['project'] = objects['org'].project(record['project'])

    if objects['sample'] is None:
        if pd.notna(record['sample']):
            objects['sample'] = objects['project'].sample(record['sample'])
            parent = objects['sample']
    else:
        parent = objects['project']

    if objects['folder'] is None:
        objects['folder'] = parent.result_folder(record['folder'])

    if objects['result_file'] is None:
        objects['result_file'] = objects['folder'].result_file(record['filename'])

    objects['result_file'].idem()
    print(objects)
    return objects


def _add_record_to_upload_manager_local_file(record: Dict, result_file, upload_manager: GeoSeeqUploadManager) -> None:
    """Add a local file upload to the upload manager."""
    upload_manager.add_result_file(result_file, record['path'], link_type='upload')


def _add_record_to_upload_manager_s3_file(record: Dict, result_file, upload_manager: GeoSeeqUploadManager) -> None:
    """Add an S3 file link to the upload manager.
    
    Handles two types of S3 URLs:
    1. https://endpoint/bucket/key - Full URL with endpoint included
    2. s3://bucket/key - S3 protocol URL that needs endpoint added
    """
    path = record['path']
    
    if path.startswith('s3://'):
        # Convert s3:// URL to https:// URL
        if not record['endpoint_url']:
            raise ValueError("endpoint_url is required for s3:// URLs")
        
        # Remove s3:// prefix and combine with endpoint
        bucket_and_key = path[5:]  # len('s3://') == 5
        path = f"{record['endpoint_url'].rstrip('/')}/{bucket_and_key}"
    elif not path.startswith('https://'):
        raise ValueError("S3 URLs must start with either 's3://' or 'https://'")

    upload_manager.add_result_file(result_file, path, link_type='s3')


def _upload_one_record(knex, record: Dict, overwrite: bool, upload_manager: GeoSeeqUploadManager) -> Dict:
    """Process a single record from the config file and add it to the upload manager."""
    objects = _get_result_file_from_record(knex, record)
    if not objects['result_file']:
        raise ValueError(f"Could not find or create result_file from record: {record}")

    # Add to upload manager based on type
    if record['type'].lower() == 'local':
        _add_record_to_upload_manager_local_file(record, objects["result_file"], upload_manager)
    elif record['type'].lower() == 's3':
        _add_record_to_upload_manager_s3_file(record, objects["result_file"], upload_manager)
    else:
        raise ValueError(f"Unknown file type: {record['type']}")

    return objects


REQUIRED_COLUMNS = [
    'organization', 'project', 'sample', 'folder', 
    'filename', 'path', 'type', 'endpoint_url'
]


@click.command('from-config')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@click.option('--sep', default=',', help='Separator character for the CSV file')
@overwrite_option
@yes_option
@click.argument('config_file', type=click.Path(exists=True))
def cli_upload_from_config(state, cores, sep, overwrite, yes, config_file):
    """Upload files to GeoSeeq based on a configuration CSV file.

    \b
    The CSV file must have the following columns:
    - organization: Organization name, GRN, or UUID (optional if project/sample/folder specified by GRN/UUID)
    - project: Project name, GRN, or UUID (optional if sample/folder specified by GRN/UUID)
    - sample: Sample name, GRN, or UUID (optional, also optional if folder specified by GRN/UUID)
    - folder: Folder name, GRN, or UUID
    - filename: Name to give the file on GeoSeeq
    - path: Path to local file or S3 URL
    - type: Either "local" or "s3"
    - endpoint_url: S3 endpoint URL (required for S3 files)

    \b
    When using GRNs or UUIDs, you can omit the parent object IDs. For example:
    - If folder is a GRN/UUID, organization/project/sample can be blank
    - If sample is a GRN/UUID, organization/project can be blank
    - If project is a GRN/UUID, organization can be blank

    \b
    Example config.csv:
    organization,project,sample,folder,filename,path,type,endpoint_url
    MyOrg,MyProject,Sample1,reads,file1.fastq,/path/to/file1.fastq,local,
    ,grn:project:uuid,Sample2,reads,file2.fastq,/path/to/file2.fastq,local,
    ,,grn:sample:uuid,reads,file3.fastq,/path/to/file3.fastq,local,
    ,,,grn:folder:uuid,file4.fastq,s3://bucket/file4.fastq,s3,https://s3.amazonaws.com

    \b
    Example with tab separator:
    $ geoseeq upload advanced from-config --sep $'\t' config.tsv
    """
    knex = state.get_knex()
    
    # Read and validate config file
    df = pd.read_csv(config_file, sep=sep)
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise click.UsageError(f"Config file missing required columns: {missing_cols}")
    
    # Create upload manager
    upload_manager = GeoSeeqUploadManager(
        n_parallel_uploads=cores,
        progress_tracker_factory=PBarManager().get_new_bar,
        log_level=state.log_level,
        overwrite=overwrite,
        use_cache=state.use_cache,
    )
    
    # Process records and add to upload manager
    objects_by_record = {}  # Store objects for human readable paths
    for _, record in df.iterrows():
        objects = _upload_one_record(knex, record, overwrite, upload_manager)
        objects_by_record[record['path']] = objects
    
    # Show preview with both technical and human readable paths
    click.echo(upload_manager.get_preview_string(), err=True)
    
    if not yes:
        click.confirm('Do you want to proceed with these uploads?', abort=True)
    
    # Perform uploads
    upload_manager.upload_files()
