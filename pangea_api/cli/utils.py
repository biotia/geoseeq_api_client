import click
import logging

from .. import Knex, User

logger = logging.getLogger('pangea_api')


class State(object):

    def __init__(self):
        self.email = None
        self.api_token = None
        self.endpoint = 'https://pangeabio.io'
        self.outfile = None
        self.log_level = 20

    def get_knex(self):
        logger.setLevel(self.log_level)
        knex = Knex(self.endpoint)
        if self.api_token:
            knex.add_api_token(self.api_token)
        return knex


pass_state = click.make_pass_decorator(State, ensure=True)


def log_level_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.log_level = int(value)
        return value
    return click.option('-l', '--log-level',
                        type=int,
                        default=20,
                        envvar='PANGEA_CLI_LOG_LEVEL',
                        expose_value=False,
                        callback=callback)(f)


def email_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.email = str(value)
        return value
    return click.option('-e', '--email',
                        envvar='PANGEA_USER',
                        expose_value=False,
                        help='Your Pangea login email.',
                        callback=callback)(f)


def api_token_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.api_token = str(value)
        return value
    return click.option('-a', '--api-token',
                        envvar='PANGEA_API_TOKEN',
                        expose_value=False,
                        help='Your Pangea API token.',
                        callback=callback)(f)


def endpoint_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.endpoint = str(value)
        return value
    return click.option('--endpoint',
                        default='https://pangeabio.io',
                        expose_value=False,
                        help='The URL to use for Pangea.',
                        callback=callback)(f)


def outfile_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.outfile = value
        return value
    return click.option('-o', '--outfile',
                        default='-', type=click.File('w'),
                        expose_value=False,
                        help='The URL to use for Pangea.',
                        callback=callback)(f)


def common_options(f):
    f = outfile_option(f)
    f = email_option(f)
    f = api_token_option(f)
    f = log_level_option(f)
    f = endpoint_option(f)
    return f


def use_common_state(f):
    f = common_options(f)
    f = pass_state(f)
    return f


def is_uuid(name):
    chunks = name.split('-')
    return len(chunks) == 5
