import logging
import click
import requests
from os.path import basename, getsize
from .upload_reads import (
    _make_in_process_logger,
    _get_regex,
    _group_files,
    flatten_list_of_fastqs,
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
    filepaths = {basename(line): line for line in flatten_list_of_fastqs(fastq_files)}
    click.echo(f'Found {len(filepaths)} files to upload.', err=True)
    regex = _get_regex(knex, filepaths, module_name, proj, regex)
    groups = _group_files(knex, filepaths, module_name, regex, yes)
    for file_name, target_url in _find_target_urls(groups, module_name, proj, filepaths, overwrite, cores, state):
        print(f'{file_name}\t{target_url}', file=state.outfile)
