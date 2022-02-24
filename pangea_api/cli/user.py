import click
import json
from .. import (
    Knex,
    User,
    Organization,
)
from .utils import use_common_state


@click.group('user')
def cli_user():
	pass


@cli_user.command('info')
@use_common_state
def cli_user_info(state):
	knex = state.get_knex()
	response = knex.get('/users/me')
	click.echo(json.dumps(response, indent=4, sort_keys=True))


@cli_user.command('change-password')
@use_common_state
@click.argument('new_password')
def cli_change_password(state, new_password):
    """Change a users password."""
    knex = state.get_knex()
    response = knex.post(
        '/auth/users/set_password',
        json={
            'new_password': new_password,
            'current_password': state.password,
        }
    )
    click.echo(f'changed password for user: {state.email}', err=True)
