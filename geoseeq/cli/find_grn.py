import click
from geoseeq.id_constructors import resolve_id
from .shared_params import (
    use_common_state,
)


@click.command('find-grn')
@use_common_state
@click.argument('grn')
def cli_find_grn(state, grn):
    """Find objects by id"""
    kind, obj = resolve_id(state.get_knex(), grn)
    print(obj)
    parent = getattr(obj, 'parent', None)
    while parent:
        print(parent)
        parent = getattr(parent, 'parent', None)

