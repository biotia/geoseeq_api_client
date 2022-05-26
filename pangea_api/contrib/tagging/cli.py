
import click
from pyrsistent import T
from .tag import Tag
from ... import (
    Knex,
    User,
    Organization,
)
import logging

logger = logging.getLogger(__name__)
logger.setLevel(10)
logger.addHandler(logging.StreamHandler())


@click.group('tag')
def tag_main():
    pass


@tag_main.command('create')
@click.option('-a', '--api-token', envvar='PANGEA_API_TOKEN')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.argument('tag_names', nargs=-1)
def create_tags(api_token, endpoint, tag_names):
    knex = Knex(endpoint)
    if api_token:
        knex.add_api_token(api_token)
    for tag_name in tag_names:
        tag = Tag(knex, tag_name).idem()
        click.echo(tag, err=True)


@tag_main.command('samples-in-group')
@click.option('-a', '--api-token', envvar='PANGEA_API_TOKEN')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.argument('org_name')
@click.argument('group_name')
@click.argument('tag_names', nargs=-1)
def cli_tag_samples_in_group(api_token, endpoint, org_name, group_name, tag_names):
    knex = Knex(endpoint)
    if api_token:
        knex.add_api_token(api_token)
    tags = [Tag(knex, tag_name).get() for tag_name in tag_names]
    org = Organization(knex, org_name).get()
    grp = org.sample_group(group_name).get()
    for sample in grp.get_samples():
        for tag in tags:
            tag(sample)
        click.echo(sample, err=True)


@tag_main.command('group')
@click.option('-a', '--api-token', envvar='PANGEA_API_TOKEN')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.argument('org_name')
@click.argument('group_name')
@click.argument('tag_names', nargs=-1)
def cli_tag_samples_in_group(api_token, endpoint, org_name, group_name, tag_names):
    knex = Knex(endpoint)
    if api_token:
        knex.add_api_token(api_token)
    tags = [Tag(knex, tag_name).get() for tag_name in tag_names]
    org = Organization(knex, org_name).get()
    grp = org.sample_group(group_name).get()
    for tag in tags:
        tag(grp)


@tag_main.group('list')
def tag_list():
    pass


@tag_list.command('random-samples')
@click.option('-a', '--api-token', envvar='PANGEA_API_TOKEN')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-n', '--num-samples', default=100, help='maximum number of samples')
@click.argument('tag_name')
def cli_tag_samples_in_group(api_token, endpoint, num_samples, tag_name):
    knex = Knex(endpoint)
    if api_token:
        knex.add_api_token(api_token)
    tag = Tag(knex, tag_name).get()
    for sample in tag.get_random_samples(n=num_samples):
        click.echo(sample, err=True)


@tag_list.command('groups')
@click.option('-a', '--api-token', envvar='PANGEA_API_TOKEN')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-n', '--num-samples', default=100, help='maximum number of samples')
@click.argument('tag_name')
def cli_tag_samples_in_group(api_token, endpoint, num_samples, tag_name):
    knex = Knex(endpoint)
    if api_token:
        knex.add_api_token(api_token)
    tag = Tag(knex, tag_name).get()
    for sample_group in tag.get_sample_groups():
        click.echo(sample_group, err=True)
