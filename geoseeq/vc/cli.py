import logging
import click
from os.path import isfile
from geoseeq.cli.utils import use_common_state
from .vc_dir import VCDir
from .clone import (
    clone_project,
    clone_sample,
)

logger = logging.getLogger('geoseeq_api')


@click.group('vc')
def cli_vc():
    """Experimental version control features."""
    pass


@cli_vc.command('clone')
@use_common_state
@click.option('--target-dir', default=".", help='Choose a target directory for stub files')
@click.option('--uuids/--names', default=False, help='Use UUIDs instead of names for objects')
@click.argument('brn')
def cli_vc_clone(state, target_dir, uuids, brn):
    """Clone GeoSeeq stub files from a project or sample to local storage.
    
    Only downloads stub files, not the files they link to.
    """
    knex = state.get_knex()
    object_type, obj = resolve_brn(knex, brn)
    if object_type == 'project':
        return clone_project(obj, target_dir, uuid=uuids)
    if object_type == 'sample':
        return clone_sample(obj, target_dir, uuid=uuids)
    pass


@cli_vc.command('download')
@use_common_state
@click.option('--extension', default='.gvcf', help='File extension for GeoSeeq version control files')
@click.argument('paths', nargs=-1)
def cli_vc_download(state, extension, paths):
    """Download files from GeoSeeq to local storage."""
    knex = state.get_knex()
    if len(paths) == 0: paths = ['.']
    for path in paths:
        for stub in VCDir(path, extension=extension).stubs():
            stub.download(knex)


@cli_vc.command('status')
@use_common_state
@click.option('--extension', default='.gvcf', help='File extension for GeoSeeq version control files')
@click.argument('paths', nargs=-1)
def cli_vc_status(state, extension, paths):
    """Check the status of all link files in the current folder or in specified paths. Recursive."""
    if len(paths) == 0: paths = ['.']
    for path in paths:
        for stub in VCDir(path, extension=extension).stubs():
            verified = 'checksum_matches' if stub.verify() else 'no_checksum_match'
            color = 'green' if verified == 'checksum_matches' else 'red'
            click.echo(click.style(f'{stub.brn}\t{stub.local_path}\t{verified}', fg=color))


@cli_vc.command('list')
@click.option('--outfile', type=click.File('w'), default='-', help='File to write paths to')
@click.option('--extension', default='.gvcf', help='File extension for GeoSeeq version control files')
@click.argument('paths', nargs=-1)
def cli_vc_list(outfile, extension, paths):
    """Recursively list all filepaths for files pointed to by stub files."""
    if len(paths) == 0: paths = ['.']
    for path in paths:
        for stub in VCDir(path, extension=extension).stubs():
            if isfile(stub.local_path):
                print(stub.local_path, file=outfile)