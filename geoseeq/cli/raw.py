import click
import json
from .shared_params import use_common_state, overwrite_option
from geoseeq import GeoseeqNotFoundError
from geoseeq.blob_constructors import (
    sample_result_file_from_uuid,
    project_result_file_from_uuid,
    sample_result_folder_from_uuid,
    project_result_folder_from_uuid,
)


@click.group('raw')
def cli_raw():
    """Low-level commands for interacting with the API."""
    pass


@cli_raw.command('get-file-data')
@use_common_state
@click.argument('file_ids', nargs=-1)
def cli_get_file_data(state, file_ids):
    """Print the raw stored data in a result file object."""
    knex = state.get_knex()
    for file_id in file_ids:
        file_id = file_id.split(':')[-1]
        try:
            result_file = sample_result_file_from_uuid(knex, file_id)
        except GeoseeqNotFoundError:
            result_file = project_result_file_from_uuid(knex, file_id)
        print(json.dumps(result_file.stored_data, indent=2), file=state.outfile)


@cli_raw.command('create-raw-file')
@use_common_state
@overwrite_option
@click.argument('folder_id')
@click.argument('result_filename')
@click.argument('filename', type=click.File('r'))
def cli_get_file_data(state, overwrite, folder_id, result_filename, filename):
    """Print the raw stored data in a result file object."""
    knex = state.get_knex()

    folder_id = folder_id.split(':')[-1]
    try:
        result_folder = sample_result_folder_from_uuid(knex, folder_id)
    except GeoseeqNotFoundError:
        result_folder = project_result_folder_from_uuid(knex, folder_id)
    blob = json.load(filename)
    result_file = result_folder.result_file(result_filename)
    if overwrite:
        result_file.idem()
        result_file.stored_data = blob
        result_file.save()
    else:
        result_file.create()
    click.echo(f'Created file {result_file.uuid}', file=state.outfile)
    

