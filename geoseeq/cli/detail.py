import click
import json
from .shared_params import (
    use_common_state,
    project_id_arg,
    sample_ids_arg,
    yes_option,
    private_option,
    org_id_arg,
    handle_project_id,
    handle_multiple_sample_ids,
    handle_org_id,
)
from geoseeq.id_constructors import resolve_id


@click.group('detail')
def cli_detail():
    """Detail objects on GeoSeeq."""
    pass


@cli_detail.command('folder')
@use_common_state
@click.argument('grn')
def detail_folder(state, grn):
    kind, rfolder = resolve_id(state.get_knex(), grn)
    assert kind == 'folder'
    click.echo('Folder:')
    click.echo(rfolder)
    click.echo('Created at: {}'.format(rfolder.created_at))
    click.echo('Updated at: {}'.format(rfolder.updated_at))
    click.echo('Files:')
    for rfile in rfolder.get_result_files():
        click.echo(rfile)
        click.echo('Created at: {}'.format(rfile.created_at))
        click.echo('Updated at: {}'.format(rfile.updated_at))
        click.echo(json.dumps(rfile.stored_data, indent=2))
        click.echo('--')