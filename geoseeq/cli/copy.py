import click
from geoseeq.knex import DEFAULT_ENDPOINT

from .. import Knex, Organization


@click.group('copy')
def cli_copy():
    """Copy samples and analysis results from one geoseeq instance to another.
    
    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    pass


@cli_copy.command('group')
@click.option('-l', '--log-level', type=int, default=20, envvar='GEOSEEQ_CLI_LOG_LEVEL')
@click.option('-o', '--outfile', type=click.File('w'), default='-')
@click.option('--source-api-token', envvar='SOURCE_GEOSEEQ_API_TOKEN')
@click.option('--target-api-token', envvar='GEOSEEQ_API_TOKEN')
@click.option('--source-endpoint', default=DEFAULT_ENDPOINT)
@click.option('--target-endpoint', default="https://geoseeq.dev.biotia.io")
@click.argument('source_org_name')
@click.argument('source_grp_name')
@click.argument('target_org_name')
@click.argument('target_grp_name')
def cli_list_samples(log_level, outfile,
                     source_api_token, target_api_token,
                     source_endpoint, target_endpoint,
                     source_org_name, source_grp_name,
                     target_org_name, target_grp_name):
    """Copy a group and its samples from one geoseeq instance to another.
    
    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    source_knex = Knex(source_endpoint)
    if source_api_token:
        source_knex.add_api_token(source_api_token)
    source_org = Organization(source_knex, source_org_name).get()
    source_grp = source_org.sample_group(source_grp_name).get()

    target_knex = Knex(target_endpoint)
    if target_api_token:
        target_knex.add_api_token(target_api_token)
    target_org = Organization(target_knex, target_org_name).idem()
    target_grp = target_org.sample_group(target_grp_name).idem()

    for source_ar in source_grp.get_analysis_results():
        target_ar = source_ar.copy(target_grp, save=True)
        print(source_ar, target_ar, file=outfile)

    for source_sample in source_grp.get_samples():
        target_sample = source_sample.copy(target_grp, save=True)
        print(source_sample, target_sample, file=outfile)
