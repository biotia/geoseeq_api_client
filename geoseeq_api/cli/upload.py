
import json

import ftplib
import os
import click
import pandas as pd
from geoseeq_api.contrib.tagging.tag import Tag

from .. import Organization
from .. import SampleGroup
from geoseeq_api.constants import *
from geoseeq_api.cli.fastq_utils import group_paired_end_paths, upload_fastq_pair, upload_single_fastq
from geoseeq_api.cli.utils import use_common_state

@click.group('upload')
def cli_upload():
    pass

dryrun_option = click.option('--dryrun/--wetrun', default=False, help='Print what will be created without actually creating it')
overwrite_option = click.option('--overwrite/--no-overwrite', default=False, help='Overwrite existing samples and data')
module_option = lambda x: click.option('-m', '--module-name', type=click.Choice(x), default=x[0], help='Name for the module that will store the data')
private_option = click.option('--private/--public', default=True, help='Make the reads private.')
tag_option = click.option('-t', '--tag', multiple=True, help='Add tags to newly created samples.')

org_arg = click.argument('org_name')
lib_arg = click.argument('library_name')


@cli_upload.command('reads')
@use_common_state
@overwrite_option
@private_option
@tag_option
@module_option(['short_read::paired_end'])
@click.option('-d', '--delim', default=None, help='Split sample name on this string')
@click.option('-1', '--ext-1', default='.R1.fastq.gz')
@click.option('-2', '--ext-2', default='.R2.fastq.gz')
@org_arg
@lib_arg
#@click.argument('file_list', type=click.File('r'))
@click.argument('file_list')
def cli_upload_pe_reads(state, overwrite, private, tag, module_name, delim,
                        ext_1, ext_2, org_name, library_name, file_list):
    """Upload paired end reads to Geoseeq.

    This command will upload paired end reads to the specified
    sample library.

    Sample names will be automatically generated from the names
    of the fastq files for each pair of reads. Sample names will
    be determined by removing extensions from each file or, if
    a delimiter string is set by taking everything before that
    string. Both reads in a pair must resolve to the same sample
    name. 

    `file_list` is a file with a list of fastq filepaths, one per line

    TODO: support multi lane files.
    """
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    lib = org.sample_group(library_name).get()
    tags = [Tag(knex, tag_name).get() for tag_name in tag]
    samples = group_paired_end_paths(file_list, ext_1, ext_2, delim=delim)
    for sname, reads in samples.items():
        sample = lib.sample(sname).idem()
        for tag in tags:
            tag(sample)
        ar = sample.analysis_result(module_name)
        r1, r2 = upload_fastq_pair(
            ar, reads['read_1'], reads['read_2'], private, overwrite=overwrite
        )
        print(sample, ar, r1, r2, file=state.outfile)


@cli_upload.command('single-ended-reads')
@use_common_state
@overwrite_option
@private_option
@dryrun_option
@tag_option
@module_option(['short_read::single_end', 'long_read::nanopore'])
@click.option('-e', '--ext', default='.fastq.gz')
@org_arg
@lib_arg
@click.argument('file_list', type=click.File('r'))
def cli_upload_se_reads(state, overwrite, private, dryrun, tag, module_name,
                        ext, org_name, library_name, file_list):
    """Upload single ended reads to Geoseeq, including nanopore reads.
    This command will upload single reads to the specified
    sample library.
    Sample names will be automatically generated from the names
    of the fastq files. Sample names will be determined by removing
    extensions from each file or, if a delimiter string is set by
    taking everything before that string.
    `file_list` is a file with a list of fastq filepaths, one per line
    """
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    lib = org.sample_group(library_name).get()
    tags = [Tag(knex, tag_name).get() for tag_name in tag]
    for filepath in (l.strip() for l in file_list):
        assert ext in filepath
        sname = filepath.split('/')[-1].split(ext)[0]
        if dryrun:
            click.echo(f'Sample: {sname} {filepath}')
        sample = lib.sample(sname).idem()
        for tag in tags:
            tag(sample)
        ar = sample.analysis_result(module_name)
        reads = upload_single_fastq(ar, module_name, filepath, private, overwrite=overwrite)
        print(sample, ar, reads, file=state.outfile)


@cli_upload.command('single-ended-reads-ftp')
@use_common_state
@overwrite_option
@private_option
@dryrun_option
@tag_option
@module_option(['short_read::single_end', 'long_read::nanopore'])
@click.option('-e', '--ext', default='.fastq.gz')
@click.option('-h', '--hostname', default=None)
@click.option('-u', '--username', default=None)
@click.option('-p', '--password', default=None)
@click.option('-l', '--folder', default=None)
@org_arg
@lib_arg
@click.argument('file_list')
def cli_upload_se_reads_ftp(state, overwrite, private, dryrun, tag, module_name,
                        ext, org_name, library_name, file_list, hostname, username, password, folder):
    """Upload single ended reads to Geoseeq, including nanopore reads.

    This command will upload single reads to the specified
    sample library.

    Sample names will be automatically generated from the names
    of the fastq files. Sample names will be determined by removing
    extensions from each file or, if a delimiter string is set by
    taking everything before that string.

    `file_list` is a file with a list of fastq filepaths, one per line
    """
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    try:
        lib = org.sample_group(library_name).get()
    except:
        grp = SampleGroup(knex, org, library_name).create()
        lib = org.sample_group(library_name).get()
    tags = [Tag(knex, tag_name).get() for tag_name in tag]
    ftp_server = ftplib.FTP(hostname, username, password)
    ftp_server.encoding = "utf-8"
    ftp_server.cwd(folder)
    for filepath in (l.strip() for l in file_list.split(",")):
        with open(filepath, "wb") as file:
            ftp_server.retrbinary(f"RETR {filepath}", file.write)
        assert ext in filepath
        sname = filepath.split('/')[-1].split(ext)[0]
        print(sname)
        if dryrun:
            click.echo(f'Sample: {sname} {filepath}')
            continue
        sample = lib.sample(sname).idem()
        for tag in tags:
            tag(sample)
        ar = sample.analysis_result(module_name)
        reads = upload_single_fastq(ar, module_name, filepath, private, overwrite=overwrite)
        print(sample, ar, reads, file=state.outfile)
        os.remove(filepath) 
    ftp_server.quit()

@cli_upload.command('paired-end-reads-ftp')
@use_common_state
@overwrite_option
@private_option
@dryrun_option
@tag_option
@module_option(['short_read::paired_end'])
@click.option('-e', '--ext', default='.fastq.gz')
@click.option('-h', '--hostname', default=None)
@click.option('-u', '--username', default=None)
@click.option('-p', '--password', default=None)
@click.option('-l', '--folder', default=None)
@click.option('-d', '--delim', default=",", help='Split sample name on this string')
@click.option('-1', '--ext-1', default='.R1.fastq.gz')
@click.option('-2', '--ext-2', default='.R2.fastq.gz')
@org_arg
@lib_arg
@click.argument('file_list')
def cli_upload_se_reads_ftp(state, overwrite, private, dryrun, tag, module_name,
                        ext, org_name, library_name, file_list, hostname, username, password, folder, delim, ext_1,ext_2):
    """Upload single ended reads to Geoseeq, including nanopore reads.

    This command will upload single reads to the specified
    sample library.

    Sample names will be automatically generated from the names
    of the fastq files. Sample names will be determined by removing
    extensions from each file or, if a delimiter string is set by
    taking everything before that string.

    `file_list` is a file with a list of fastq filepaths, one per line
    """
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    try:
        lib = org.sample_group(library_name).get()
    except:
        grp = SampleGroup(knex, org, library_name).create()
        lib = org.sample_group(library_name).get()
    tags = [Tag(knex, tag_name).get() for tag_name in tag]
    ftp_server = ftplib.FTP(hostname, username, password)
    ftp_server.encoding = "utf-8"
    ftp_server.cwd(folder)
    for filepath in (l.strip() for l in file_list.split(",")):
        with open(filepath, "wb") as file:
            ftp_server.retrbinary(f"RETR {filepath}", file.write)
    samples = group_paired_end_paths(file_list, ext_1, ext_2, delim=delim)
    for sname, reads in samples.items():
        sample = lib.sample(sname).idem()
        for tag in tags:
            tag(sample)
        ar = sample.analysis_result(module_name)
        r1, r2 = upload_fastq_pair(
            ar, reads['read_1'], reads['read_2'], private, overwrite=overwrite
        )
        print(sample, ar, r1, r2, file=state.outfile)
    for filepath in (l.strip() for l in file_list.split(",")):
        os.remove(filepath)
    ftp_server.quit()

    


@cli_upload.command('metadata')
@use_common_state
@overwrite_option
@click.option('--create/--no-create', default=False)
@click.option('--update/--no-update', default=False)
@click.option('--overwrite/--no-overwrite', default=False)
@click.option('--index-col', default=0)
@click.option('--sep', default="\t")
@click.option('--encoding', default='utf_8')
@org_arg
@lib_arg
@click.argument('table', type=click.File('rb'))
def cli_metadata(state, overwrite,
                 create, update, index_col, sep, encoding,
                 org_name, library_name, table):
    knex = state.get_knex()
    tbl = pd.read_csv(table, index_col=index_col, encoding=encoding, sep=sep)
    tbl.index = tbl.index.to_series().map(str)
    org = Organization(knex, org_name).get()
    lib = org.sample_group(library_name).get()
    for sample_name, row in tbl.iterrows():
        sample = lib.sample(sample_name)
        print(sample)
        if create:
            sample = sample.idem()
        else:
            try:
                sample = sample.get()
            except Exception as e:
                click.echo(f'Sample "{sample.name}" not found.', err=True)
                continue
        new_meta = json.loads(json.dumps(row.dropna().to_dict())) 
        if overwrite or (not sample.metadata):
            sample.metadata = new_meta
            sample.idem()
        elif update:
            old_meta = sample.metadata
            old_meta.update(new_meta)
            sample.metadata = old_meta
            sample.idem()
        click.echo(sample)
