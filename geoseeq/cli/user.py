import json

import click

from .utils import use_common_state


@click.group('user')
def cli_user():
    """Manage users on GeoSeeq."""
    pass


@cli_user.command('info', deprecated=True)
@use_common_state
def cli_user_info(state):
    knex = state.get_knex()
    response = knex.get('/users/me')
    click.echo(json.dumps(response, indent=4, sort_keys=True))


# New endpoint has to be created on server and can be done with clerk backend API

# @cli_user.command('change-password')
# @use_common_state
# @click.argument('new_password')
# def cli_change_password(state, new_password):
#     """Change a users password."""
#     knex = state.get_knex()
#     response = knex.post(
#         '/auth/users/set_password',
#         json={
#             'new_password': new_password,
#             'current_password': state.password,
#         }
#     )
#     click.echo(f'changed password for user: {state.email}', err=True)
