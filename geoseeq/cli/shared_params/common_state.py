import logging

import click
from geoseeq.knex import DEFAULT_ENDPOINT

from geoseeq import Knex
from .config import load_profile

logger = logging.getLogger('geoseeq_api')


class State(object):

    def __init__(self):
        self.api_token = None
        self.endpoint = DEFAULT_ENDPOINT
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
        state.log_level = value
        return value
    return click.option('-l', '--log-level',
                        type=click.Choice(['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']),
                        default='WARNING',
                        envvar='GEOSEEQ_CLI_LOG_LEVEL',
                        expose_value=False,
                        callback=callback)(f)


def api_token_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        if not state.api_token:
            state.api_token = str(value)
        return value
    return click.option('-a', '--api-token',
                        envvar='GEOSEEQ_API_TOKEN',
                        expose_value=False,
                        help='Your GEOSEEQ API token.',
                        callback=callback)(f)


def endpoint_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        if not state.endpoint:
            state.endpoint = str(value)
        return value
    return click.option('--endpoint',
                        default=DEFAULT_ENDPOINT,
                        envvar='GEOSEEQ_API_ENDPOINT',
                        expose_value=False,
                        help='The URL to use for GEOSEEQ.',
                        callback=callback)(f)

def profile_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        endpoint, token = None, None
        if value:
            endpoint, token = load_profile(value)
        else:
            try:
                endpoint, token = load_profile()  # load default profile, if it exists
            except KeyError:
                pass
        if endpoint and token:
            state.endpoint = endpoint
            state.api_token = token
        return value
    return click.option('-p', '--profile',
                        envvar='GEOSEEQ_API_PROFILE',
                        expose_value=False,
                        help='The config profile to use for GEOSEEQ.',
                        callback=callback)(f)

def outfile_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.outfile = value
        return value
    return click.option('-o', '--outfile',
                        default='-', type=click.File('w'),
                        expose_value=False,
                        help='Output file path',
                        callback=callback)(f)


def common_options(f):
    f = outfile_option(f)
    f = api_token_option(f)
    f = log_level_option(f)
    f = endpoint_option(f)
    f = profile_option(f)
    return f


def use_common_state(f):
    f = common_options(f)
    f = pass_state(f)
    return f


def is_uuid(name):
    chunks = name.split('-')
    return len(chunks) == 5
