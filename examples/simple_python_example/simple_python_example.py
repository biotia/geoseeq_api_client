
import gzip
from random import randint

import click
from geoseeq_api import Knex, Organization
from requests.exceptions import HTTPError

"""
Note: ordinarily we would not want a module name
to be random. We only make the module name random
for this example so that it can be run many times
by many different people on the same dataset.
"""
UPLOAD_MODULE_NAME = f'example::simple_python_example_{randint(0, 1000 * 1000)}'
UPLOAD_FIELD_NAME = 'basic_read_stats'


def get_fastq_stats(fastq_filehandle):
    """Return some basic statistics for a fastq file."""
    read_count, gc_count, total_base_count = 0, 0, 0
    for i, line in enumerate(fastq_filehandle):
        if i != 1:  # only process the reads from the fastq
            continue
        read_count += 1
        for base in line.strip().upper():
            total_base_count += 1
            if base in ['G', 'C']:
                gc_count += 1
    gc_fraction = gc_count / total_base_count
    return read_count, gc_fraction


def process_fastq(sample, local_filepath):
    click.echo(f'Calculating stats for {sample.name}')
    with gzip.open(local_filepath, 'rt') as fastq:
        read_count, gc_fraction = get_fastq_stats(fastq)
    output_filename = f'{sample.name}_example_stats.tsv'
    with open(output_filename, 'w') as f:
        f.write(f'read_count\t{read_count}\n')
        f.write(f'gc_fraction\t{gc_fraction}\n')
    return output_filename


def upload_result_to_geoseeq(sample, result_filepath):
    """Upload the local result to Geoseeq.

    Note that field.upload_file will automatically handle 
    uploads for large files.
    """
    ar = sample.analysis_result(UPLOAD_MODULE_NAME).create()
    field = ar.field(UPLOAD_FIELD_NAME).create()
    field.upload_file(result_filepath)


def download_reads_from_geoseeq(sample, module_name, field_name):
    """Return the local filepath for reads from the sample after downloading."""
    filename = f'{sample.name}_reads.fq.gz'
    ar = sample.analysis_result(module_name).get()
    field = ar.field(field_name).get()
    field.download_file(filename=filename)
    return filename


def process_sample(sample, module_name, field_name):
    """Find some basic stats for reads in a sample and upload
    the results to Geoseeq.
    """
    try:
        local_filepath = download_reads_from_geoseeq(sample, module_name, field_name)
    except HTTPError:
        click.echo(f'Reads not found for sample {sample.name}')
        return
    local_result = process_fastq(sample, local_filepath)
    upload_result_to_geoseeq(sample, local_result)


def handle_sample(sample, module_name, field_name):
    """Check if a sample has already been processed and if not process it."""
    if sample.analysis_result(UPLOAD_MODULE_NAME).exists():
        click.echo(f'Sample {sample.name} has already been processed')
    else:
        click.echo(f'Sample {sample.name} has not been processed')
        process_sample(sample, module_name, field_name)


@click.command()
@click.option('-a', '--api-token', help='Your Geoseeq API token')
@click.option('-m', '--module-name', default='raw::paired_short_reads')
@click.option('-f', '--field-name', default='read_1')
@click.argument('organization_name')
@click.argument('sample_group_name')
def main(api_token, module_name, field_name, organization_name, sample_group_name):
    """Example script using Geoseeq and Python to analyze data.

    This script will process every sample in a given sample group.
    It will download reads from each sample then calculate some
    basic statistics for each read.
    """
    knex = Knex()
    knex.add_api_token(api_token)
    org = Organization(knex, organization_name).get()
    grp = org.sample_group(sample_group_name).get()
    click.echo(f'Using result module name: {UPLOAD_MODULE_NAME}')
    for sample in grp.get_samples():
        click.echo(f'Processing sample {sample.name}...', err=True)
        handle_sample(sample, module_name, field_name)


if __name__ == '__main__':
    main()
