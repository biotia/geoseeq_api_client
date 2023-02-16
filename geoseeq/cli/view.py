import click
import json
from geoseeq.blob_constructors import (
    sample_ar_from_uuid,
)
from .utils import use_common_state


@click.group('view')
def cli_view():
    """View an object on GeoSeeq."""
    pass


@cli_view.command('sample-result')
@use_common_state
@click.argument('uuid')
def cli_view_result(state, uuid):
    """Print a list of samples in the specified group."""
    knex = state.get_knex()
    result = sample_ar_from_uuid(knex, uuid)
    print(result)
    for field in result.get_fields():
        print('\t', field)
        for line in json.dumps(field.stored_data, indent=2).split('\n'):
            print('\t\t', line)
