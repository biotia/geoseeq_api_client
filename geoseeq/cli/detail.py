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
from geoseeq.id_constructors import resolve_id, pipeline_from_uuid


@click.group('detail')
def cli_detail():
    """Detail objects on GeoSeeq."""
    pass


def print_folder(folder, recurse=False):
    click.echo('Folder:')
    click.echo(folder)
    click.echo('Created at: {}'.format(folder.created_at))
    click.echo('Updated at: {}'.format(folder.updated_at))
    if recurse:
        for rfile in folder.get_result_files():
            click.echo(rfile)
            click.echo('Created at: {}'.format(rfile.created_at))
            click.echo('Updated at: {}'.format(rfile.updated_at))
            click.echo(json.dumps(rfile.stored_data, indent=2))
            click.echo('--')


def print_sample(sample, recurse=False):
    click.echo('Sample:')
    click.echo(sample)
    click.echo('Created at: {}'.format(sample.created_at))
    click.echo('Updated at: {}'.format(sample.updated_at))
    if recurse:
        for rfile in sample.get_result_folders():
            print_folder(rfile, recurse=recurse)


@cli_detail.command('folder')
@use_common_state
@click.argument('grn')
def detail_folder(state, grn):
    """Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
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


@cli_detail.command('project')
@use_common_state
@click.option('--recurse/--no-recurse', default=False)
@click.argument('grn')
def detail_project(state, recurse, grn):
    """Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    kind, project = resolve_id(state.get_knex(), grn)
    assert kind == 'project'
    click.echo('Project:')
    click.echo(project)
    click.echo('Privacy: {}'.format(project.privacy_level))
    click.echo('Created at: {}'.format(project.created_at))
    click.echo('Folders:')
    for folder in project.get_result_folders():
        print_folder(folder, recurse=recurse)
        click.echo('--')
    click.echo('Samples:')
    for sample in project.get_samples():
        print_sample(sample, recurse=recurse)
        click.echo('--')


@cli_detail.command('pipeline')
@use_common_state
@click.argument('grn')
def detail_pipeline(state, grn):
    """Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    pipeline_uuid = grn.split(':')[-1]
    pipeline = pipeline_from_uuid(state.get_knex(), pipeline_uuid)
    click.echo('Pipeline:')
    click.echo(pipeline)
    click.echo('Arguments:')
    for pipeline_option in pipeline.options():
        click.echo(f'Name: {pipeline_option.name}')
        click.echo(f'Type: {pipeline_option.type}')
        click.echo(f'Options: {pipeline_option.options}')
        click.echo(f'Default: {pipeline_option.default_value}')
        click.echo(f'Description: {pipeline_option.description}')
        click.echo('--')


@cli_detail.command('pipeline-run')
@use_common_state
@click.argument('uuid')
def detail_pipeline_run(state, uuid):
    """Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    pipeline_run = state.get_knex().get(f'app_runs/{uuid}')
    click.echo('Pipeline Run:')
    for key, val in pipeline_run.items():
        click.echo(f'{key}: {val}')
