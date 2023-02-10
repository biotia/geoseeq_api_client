import click

from .. import Organization
from ..blob_constructors import sample_ar_from_uuid, sample_from_uuid
from .constants import *
from .utils import use_common_state


@click.group('create')
def cli_create():
    """Create objects on Geoseeq."""
    pass


@cli_create.command('org')
@use_common_state
@click.argument('org_name')
def cli_create_org(state, org_name):
    """Create an organization on Geoseeq."""
    knex = state.get_knex()
    org = Organization(knex, org_name).create()
    click.echo(f'Created: {org}', err=True)


@cli_create.command('sample-group')
@use_common_state
@click.option('--private/--public', default=True)
@click.option('--library/--not-library', default=True)
@click.argument('org_name')
@click.argument('grp_name')
def cli_create_grp(state, private, library, org_name, grp_name):
    """Create a sample group on Geoseeq."""
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    grp = org.sample_group(grp_name, is_library=library, is_public=not private).create()
    click.echo(f'Created: {grp}', err=True)


def simplify_sample_names(name_file, name_list):
    all_names = [name for name in name_list]
    if name_file:
        for name in name_file:
            all_names.append(name)
    out = []
    for name in all_names:
        for sub_name in name.strip().split(','):
            out.append(sub_name)
    return out


@cli_create.command('samples')
@use_common_state
@click.option('-s', '--sample-name-list', default=None, type=click.File('r'), help="A file containing a list of sample names")
@click.argument('org_name')
@click.argument('library_name')
@click.argument('sample_names', nargs=-1)
def cli_create_samples(state, sample_name_list, org_name, library_name, sample_names):
    """Create samples in the specified group.

    `sample_names` is a list of samples with one line per sample
    """
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    lib = org.sample_group(library_name, is_library=True).get()
    for sample_name in simplify_sample_names(sample_name_list, sample_names):
        sample = lib.sample(sample_name).idem()
        print(sample, file=state.outfile)


@cli_create.command('sample-with-data')
@use_common_state
@click.option('-m', '--module-name', type=click.Choice(READ_MODULE_NAMES))
@click.argument('org_name')
@click.argument('library_name')
@click.argument('sample_name')
@click.argument('data_files', nargs=-1)
def cli_create_samples(state, module_name, org_name, library_name, sample_name, data_files):
    """Create one sample in the specified group with attached raw data.

    `module_name` is the type of data being uploaded
    `org_name` is the name of the organization where the sample should be created
    `library_name` is the name of the library sample group where the sample should be created
    `sample_name` is the name for the new sample
    `data_files` is either one or two gzipped fastq files depending on the module_name
    """
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    lib = org.sample_group(library_name, is_library=True).get()
    sample = lib.sample(sample_name).idem()
    ar = sample.analysis_result(module_name).create()
    if module_name in [SINGLE_END, NANOPORE]:
        field_name=f"{module_name}::read_1::lane_1"
        r1 = ar.field(field_name).create()
        r1.upload_file(data_files[0])
    elif module_name == PAIRED_END:
        field_name=f"{module_name}::read_1::lane_1"
        r1 = ar.field(field_name).create()
        r1.upload_file(data_files[0])
        field_name=f"{module_name}::read_2::lane_1"
        r2 = ar.field(field_name).create()
        r2.upload_file(data_files[1])
    click.echo('Sample with data successfully created', err=True)


@cli_create.command('sample-ar')
@use_common_state
@click.option('-r', '--replicate')
@click.argument('names', nargs=-1)
def cli_create_sample_ar(state, replicate, names):
    """Create new sample analysis result for a sample.

    Names must be one of:
    <org name> <library name> <sample name> <new module name>
    <sample uuid> <new module name>
    
    `org name` is the name of the organization where the sample exists
    `library name` is the name of the library where the sample exists
    `sample name` is the name of the sample
    `sample uuid` is the uuid of the sample
    `new module name` module name for the created analysis result
    """
    if len(names) not in [2, 4]:
        click.echo('''
            Names must be one of:
            <org name> <library name> <sample name> <new module name>
            <sample uuid> <new module name>
        ''', err=True)
        return
    knex = state.get_knex()
    module_name = names[-1]
    if len(names) == 2:
        sample = sample_from_uuid(knex, names[0])
    else:
        org = Organization(knex, names[0]).get()
        lib = org.sample_group(names[1]).get()
        sample = lib.sample(names[2]).get()
    ar = sample.analysis_result(module_name, replicate=replicate).create()
    click.echo(f'Created: {ar}', err=True)


@cli_create.command('field')
@use_common_state
@click.argument('names', nargs=-1)
def cli_create_field(state, names):
    """Create new sample analysis result field.

    Names must be one of:
    <org name> <library name> <sample name> <module name> <new field name> <filename>
    <module uuid> <new field name> <filename>
    
    `org name` is the name of the organization where the sample exists
    `library name` is the name of the library where the sample exists
    `sample name` is the name of the sample
    `module name` module name of the target sample analysis result
    `module uuid` uuid of the target sample analysis result
    `new field name` name of the created field
    `filename` route to the selected file

    """
    if len(names) not in [3, 6]:
        click.echo('''
            Names must be one of:
            <org name> <library name> <sample name> <module name> <new field name> <filename>
            <module uuid> <new field name> <filename>
        ''', err=True)
        return
    knex = state.get_knex()
    field_name = names[-2]
    if len(names) == 3:
        ar = sample_ar_from_uuid(knex, names[0])
    else:
        org = Organization(knex, names[0]).get()
        lib = org.sample_group(names[1]).get()
        sample = lib.sample(names[2]).get()
        ar = sample.analysis_result(names[3]).get()
    arf = ar.field(field_name).create()
    click.echo(f'Created: {arf}', err=True)
    click.echo(f'Uploading file: {names[-1]}', err=True)
    arf.upload_file(names[-1], logger=lambda x: click.echo(x, err=True))
    click.echo('File successfully uploaded', err=True)
