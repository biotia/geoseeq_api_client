
import click
import json
from Bio import Entrez
from ... import (
    Knex,
    User,
    Organization,
)
from .api import create_pangea_group_from_bioproj
from .bioproject import (
    BioProject,
    SRARecord,
)
from .setup_logging import logger
import logging

logger.setLevel(10)
logger.handlers = []
logger.addHandler(logging.StreamHandler())


@click.group('ncbi')
def ncbi_main():
    pass


@ncbi_main.group('link')
def cli_ncbi_link():
    pass


@cli_ncbi_link.command('bioproject')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.argument('org_name')
@click.argument('bioproj_accession')
def cli_ncbi_link_bioproject(email, password, endpoint, org_name, bioproj_accession):
    """Create a pangea group from an NCBI BioProject.

    Creates a Pangea SampleGroup corresponding to the given bioproject accession.
     - This SampleGroup is a Pangea Library that can contain samples
     - A PangeaSample is created in the group for each NCBI BioSample
     - raw read analysis results are created for each sample from the SRA

    All objects are tagged to flag they are from an NCBI database.
    """
    logger.info(f'Creating Pangea SampleGroup from BioProject "{bioproj_accession}"')
    Entrez.email = email
    knex = Knex(endpoint)
    if email and password:
        User(knex, email, password).login()
    org = Organization(knex, org_name).get()
    grp = create_pangea_group_from_bioproj(org, bioproj_accession)
    return grp


@ncbi_main.group('list')
def cli_ncbi_list():
    pass


@cli_ncbi_list.command('bioproject')
@click.option('-s', '--sleep', default=1)
@click.option('-o', '--outfile', type=click.File('w'), default='-')
@click.argument('accession')
def cli_ncbi_list_bioproject(sleep, outfile, accession):
    """Write sample info from an NCBI bioproject to a file."""

    bioproj = BioProject(accession)
    print(bioproj)
    print(
        json.dumps(bioproj.metadata(), indent=4, sort_keys=True),
        file=outfile
    )
    for biosample in bioproj.biosamples():
        print('\t', biosample)
        for sra_rec in biosample.sra():
            print('\t\t', sra_rec)
            assert False


@cli_ncbi_list.command('sra')
@click.option('-s', '--sleep', default=1)
@click.option('-o', '--outfile', type=click.File('w'), default='-')
@click.argument('accession')
def cli_ncbi_list_sra(sleep, outfile, accession):
    """Write sample info from an NCBI bioproject to a file."""
    sra = SRARecord(accession)
    tbl = sra.to_table(sleep=sleep)
    tbl.to_csv(outfile)
