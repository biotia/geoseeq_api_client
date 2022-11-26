import logging
import click
from geoseeq_api.cli.utils import use_common_state
from geoseeq_api.blob_constructors import resolve_brn
from .vc_dir import VCDir
from .clone import (
    clone_project,
    clone_sample,
)

logger = logging.getLogger('geoseeq_api')


@click.group('vc')
def cli_vc():
    pass


@cli_vc.command('clone')
@use_common_state
@click.argument('brn')
def cli_vc_clone(state, brn):
    """Clone GeoSeeq stub files from a project or sample to local storage.
    
    Only downloads stub files, not the files they link to.
    """
    object_type, obj = resolve_brn(brn)
    if object_type == 'project':
        return clone_project(state.knex, obj)
    if object_type == 'sample':
        return clone_sample(state.knex, obj)
    pass


@cli_vc.command('download')
@use_common_state
@click.option('--extension', default='.gvcf', help='File extension for GeoSeeq version control files')
@click.argument('paths', nargs=-1)
def cli_vc_download(state, extension, paths):
    """Download files from GeoSeeq to local storage."""
    pass


@cli_vc.command('status')
@use_common_state
@click.option('--extension', default='.gvcf', help='File extension for GeoSeeq version control files')
@click.argument('paths', nargs=-1)
def cli_vc_status(state, extension, paths):
    """Check the status of all link files in the current folder or in specified paths. Recursive."""
    if len(paths) == 0: paths = ['.']
    for path in paths:
        for stub in VCDir(path, extension=extension):
            click.echo(f'{stub.brn}\t{stub.local_path}\t{stub.verify()}')

