import logging
import click
import requests
from os.path import basename

from multiprocessing import Pool, current_process

from geoseeq.cli.constants import *
from geoseeq.cli.shared_params import (
    handle_project_id,
    private_option,
    link_option,
    module_option,
    project_id_arg,
    overwrite_option,
    yes_option,
    use_common_state,
    no_new_versions_option
)
from geoseeq.upload_download_manager import GeoSeeqUploadManager

from geoseeq.constants import FASTQ_MODULE_NAMES
from geoseeq.cli.progress_bar import PBarManager

logger = logging.getLogger('geoseeq_api')


def _make_in_process_logger(log_level):
    logger = logging.getLogger('geoseeq_api')
    logger.setLevel(log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(levelname)s] %(name)s :: ' + current_process().name + ' :: %(message)s'))
    logger.addHandler(handler)
    return logger


def _upload_one_file(args):
    result_file, filepath, session, progress_tracker, link_type, overwrite, log_level = args
    _make_in_process_logger(log_level)
    if link_type == 'upload':
        # TODO: check checksums to see if the file is the same
        result_file.upload_file(filepath, session=session, overwrite=overwrite,progress_tracker=progress_tracker, threads=4)
    else:
        result_file.link_file(link_type, filepath)


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


def _do_upload(groups, module_name, link_type, lib, filepaths, overwrite, no_new_versions, cores, state):

    with requests.Session() as session:
        upload_manager = GeoSeeqUploadManager(
            n_parallel_uploads=cores,
            session=session,
            link_type=link_type,
            log_level=state.log_level,
            overwrite=overwrite,
            progress_tracker_factory=PBarManager().get_new_bar,
            use_cache=state.use_cache,
            no_new_versions=no_new_versions,
            use_atomic_upload=True,
        )
        for group in groups:
            sample = lib.sample(group['sample_name']).idem()
            read_folder = sample.result_folder(module_name).idem()
            for field_name, path in group['fields'].items():
                result_file = read_folder.read_file(field_name)
                upload_manager.add_result_file(result_file, filepaths[path])
        upload_manager.upload_files()



def _is_fastq(path, fq_exts=['.fastq', '.fq'], compression_exts=['.gz', '.bz2', '']):
    for fq_ext in fq_exts:
        for compression_ext in compression_exts:
            if path.endswith(fq_ext + compression_ext):
                return True
    return False


def flatten_list_of_fastqs(filepaths):
    """Turn a list of fastq filepaths and txt files containing fastq filepaths into a single list of fastq filepaths."""
    flattened = []
    for path in filepaths:
        if _is_fastq(path):
            flattened.append(path)
        else:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        flattened.append(line)
    return flattened


def _is_bam(path):
    for ext in ['.bam', '.bai']:
        if path.endswith(ext):
            return True
    return False


def flatten_list_of_bams(filepaths):
    """Turn a list of bam filepaths and txt files containing bam filepaths into a single list of bam filepaths."""
    flattened = []
    for path in filepaths:
        if _is_bam(path):
            flattened.append(path)
        else:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        flattened.append(line)
    return flattened



@click.command('reads')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@overwrite_option
@yes_option
@click.option('--regex', default=None, help='An optional regex to use to extract sample names from the file names')
@private_option
@link_option
@no_new_versions_option
@module_option(FASTQ_MODULE_NAMES)
@project_id_arg
@click.argument('fastq_files', type=click.Path(exists=True), nargs=-1)
def cli_upload_reads_wizard(state, cores, overwrite, yes, regex, private, link_type, no_new_versions, module_name, project_id, fastq_files):
    """Upload fastq read files to GeoSeeq.

    This command automatically groups files by their sample name, lane number
    and read number. It asks for confirmation before creating any samples or
    data.

    ---

    Example Usage:

    \b
    # Upload a list of fastq files to a project, useful if you have hundreds of files
    $ ls -1 path/to/fastq/files/*.fastq.gz > file_list.txt
    $ geoseeq upload reads "GeoSeeq/Example CLI Project" file_list.txt

    \b
    # Upload all the fastq files in a directory to a project
    $ geoseeq upload reads ed59b913-91ec-489b-a1b9-4ea137a6e5cf path/to/fastq/files/*.fastq.gz

    \b
    # Upload all the fastq files in a directory to a project, performing 4 uploads in parallel
    $ geoseeq upload reads --cores 4 ed59b913-91ec-489b-a1b9-4ea137a6e5cf path/to/fastq/files/*.fastq.gz

    \b
    # Upload a list of fastq files to a project, automatically creating a new project and overwriting existing files
    $ ls -1 path/to/fastq/files/*.fastq.gz > file_list.txt
    $ geoseeq upload reads --yes --overwrite "GeoSeeq/Example CLI Project" file_list.txt

    ---

    Command Arguments:
    
    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.

    \b
    Examples: 
     - Name pair: "GeoSeeq/Example CLI Project"
     - UUID: "ed59b913-91ec-489b-a1b9-4ea137a6e5cf"
     - GRN: "grn:gs1:project:ed59b913-91ec-489b-a1b9-4ea137a6e5cf"

    \b
    [FASTQ_FILES...] can be paths to fastq files or a file containing a list of paths, or a mix of both.
    Example: "path/to/fastq/files/*.fastq.gz" "file_list.txt" "path/to/more/fastq/files/*.fastq.gz"

    ---
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes, private)
    filepaths = {basename(line): line for line in flatten_list_of_fastqs(fastq_files)}
    click.echo(f'Found {len(filepaths)} files to upload.', err=True)
    regex = _get_regex(knex, filepaths, module_name, proj, regex)
    groups = _group_files(knex, filepaths, module_name, regex, yes)
    _do_upload(groups, module_name, link_type, proj, filepaths, overwrite, no_new_versions, cores, state)


# @click.command('bam')
# @use_common_state
# @click.option('--genome', default=None, help='The genome aligned to the BAM files. Should be in 2bit format.')
# @click.option('--cores', default=1, help='Number of uploads to run in parallel')
# @overwrite_option
# @yes_option
# @click.option('--regex', default=None, help='An optional regex to use to extract sample names from the file names')
# @private_option
# @link_option
# @no_new_versions_option
# @project_id_arg
# @click.argument('files', type=click.Path(exists=True), nargs=-1)
# def cli_upload_bams(state, genome, cores, overwrite, yes, regex, private, link_type, no_new_versions, project_id, files):
    """Upload BAM files to GeoSeeq.

    This command automatically groups bams with their index files.

    ---

    Example Usage:

    \b
    # Upload a list of BAM files to a project, useful if you have hundreds of files
    $ ls -1 path/to/bam/files/*.bam > file_list.txt
    $ geoseeq upload bams "GeoSeeq/Example CLI Project" file_list.txt

    \b
    # Upload all the BAM files in a directory to a project with BAM indexes
    $ geoseeq upload bams ed59b913-91ec-489b-a1b9-4ea137a6e5cf path/to/bam/files/*.bam path/to/bam/files/*.bam.bai

    \b
    # Upload all the BAM files in a directory to a project, performing 4 uploads in parallel
    $ geoseeq upload bams --cores 4 ed59b913-91ec-489b-a1b9-4ea137a6e5cf path/to/bam/files/*.bam

    \b
    # Upload a list of BAM files to a project, automatically creating a new project and overwriting existing files
    $ ls -1 path/to/bam/files/*.bam > file_list.txt
    $ geoseeq upload bams --yes --overwrite "GeoSeeq/Example CLI Project" file_list.txt

    ---

    Command Arguments:
    
    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.

    \b
    Examples: 
     - Name pair: "GeoSeeq/Example CLI Project"
     - UUID: "ed59b913-91ec-489b-a1b9-4ea137a6e5cf"
     - GRN: "grn:gs1:project:ed59b913-91ec-489b-a1b9-4ea137a6e5cf"

    \b
    [FILES...] can be paths to BAM files or a file containing a list of paths, or a mix of both.
    Example: "path/to/bam/files
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes, private)
    filepaths = {basename(line): line for line in flatten_list_of_bams(files)}
    click.echo(f'Found {len(filepaths)} files to upload.', err=True)
    groups = _group_files(knex, filepaths, 'bam::bam', regex, yes)
    _do_upload(groups, 'bam::bam', link_type, proj, filepaths, overwrite, no_new_versions, cores, state)