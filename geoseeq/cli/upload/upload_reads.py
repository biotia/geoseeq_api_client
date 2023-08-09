import logging
from tqdm import tqdm
import click
import requests
from os.path import basename

from multiprocessing import Pool, current_process

from geoseeq.cli.constants import *
from geoseeq.cli.utils import use_common_state
from geoseeq.cli.shared_params import handle_project_id
from geoseeq.constants import FASTQ_MODULE_NAMES

from .opts_and_args import *

logger = logging.getLogger('geoseeq_api')


class TQBar:

    def __init__(self, pos, desc) -> None:
        self.n_bars = 0
        self.pos = pos
        self.desc = desc
        self.bar = None

    def set_num_chunks(self, n_chunks):
        self.n_bars = n_chunks
        self.bar = tqdm(total=n_chunks, position=self.pos, desc=self.desc, leave=False)

    def update(self, chunk_num):
        self.bar.update(chunk_num)


class PBarManager:

    def __init__(self):
        self.n_bars = 0
        self.pbars = []

    def get_new_bar(self, filepath):
        self.n_bars += 1
        return TQBar(self.n_bars, basename(filepath))


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
    if result_file.exists() and not overwrite:  
        return
    result_file = result_file.idem()
    if link_type == 'upload':
        # TODO: check checksums to see if the file is the same
        result_file.upload_file(filepath, session=session, progress_tracker=progress_tracker, threads=4)
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


def _do_upload(groups, module_name, link_type, lib, filepaths, overwrite, cores, state):
    def handle_upload(sample, success, error):
        if success:
            click.echo(f'Uploaded Sample: {sample.name}', err=True)
        else:
            click.echo(f'Failed to upload Sample: {sample.name}', err=True)
            click.echo(f'Error:\n{error}\n{error.traceback()}', err=True)

    pbars = PBarManager()
    with requests.Session() as session:
        upload_args = []
        for group in groups:
            sample = lib.sample(group['sample_name']).idem()
            read_folder = sample.result_folder(module_name).idem()

            for field_name, path in group['fields'].items():
                result_file = read_folder.read_file(field_name)
                filepath = filepaths[path]
                upload_args.append((
                    result_file, filepath, session, pbars.get_new_bar(filepath),
                    link_type, overwrite, state.log_level
                ))

        with Pool(cores) as p:
            for _ in p.imap_unordered(_upload_one_file, upload_args):
                pass

@click.command('reads')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@click.option('--overwrite/--no-overwrite', default=False, help='Overwrite existing files')
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@click.option('--regex', default=None, help='An optional regex to use to extract sample names from the file names')
@private_option
@link_option
@module_option(FASTQ_MODULE_NAMES)
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
