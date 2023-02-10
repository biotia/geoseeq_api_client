import click
from requests.exceptions import HTTPError


def group_paired_end_paths(file_list, ext_1, ext_2, delim=''):
    """Return a dict mapping sample names -> "read_[1|2]" -> filepath.
    
    Raise a ValueError if a sample does not have exactly 2 reads.

    TODO: support multi lane files.
    """
    samples = {}
    for filepath in (l.strip() for l in file_list):
        if ext_1 in filepath:
            sname = filepath.split(ext_1)[0]
            key = 'read_1'
        elif ext_2 in filepath:
            sname = filepath.split(ext_2)[0]
            key = 'read_2'
        else:
            continue
        sname = sname.split('/')[-1]
        if delim:
            sname = sname.split(delim)[0]
        if sname not in samples:
            samples[sname] = {}
        samples[sname][key] = filepath
    for sname, reads in samples.items():
        if len(reads) != 2:
            raise ValueError(f'Sample {sname} has wrong number of reads: {reads}')
    return samples


def upload_fastq_pair(ar, read_1, read_2, private, overwrite=False):
    """Return two result fields with uploaded fastq files.
    
    No-op if the result fields already contained data and overwrite was not set.

    TODO: support multi lane files.
    """
    try:
        if overwrite:  # don't bother checking if the fields exist in overwrite mode
            raise HTTPError()
        r1 = ar.field('short_read::paired_end::read_1::lane_1').get()
        r2 = ar.field('short_read::paired_end::read_2::lane_1').get()
    except HTTPError:
        ar.is_private = private
        r1 = ar.field('short_read::paired_end::read_1::lane_1').idem()\
            .upload_file(
                read_1,
                optional_fields={
                    'format': 'fastq',
                    'sequencing_type': 'short_read::paired_end::read_1::lane_1',
                    'format_schema_version': 'v1',
                },
                logger=lambda x: click.echo(x, err=True)
            )
        r2 = ar.field('short_read::paired_end::read_2::lane_1').idem()\
            .upload_file(
                read_2,
                optional_fields={
                    'format': 'fastq',
                    'sequencing_type': 'short_read::paired_end::read_2::lane_1',
                    'format_schema_version': 'v1',
                },
                logger=lambda x: click.echo(x, err=True)
            )
    return r1, r2


def upload_single_fastq(ar, sequencing_type, filepath, private, overwrite):
    try:
        if overwrite:
            raise HTTPError()
        reads = ar.field(f'{sequencing_type}::read_1::lane_1').get()
    except HTTPError:
        ar.is_private = private
        reads = ar.field(f'{sequencing_type}::read_1::lane_1').idem()
        reads.upload_file(
            filepath,
            optional_fields={
                'format': 'fastq',
                'sequencing_type': f'{sequencing_type}::read_1::lane_1',
                'format_schema_version': 'v1',
            },
            logger=lambda x: click.echo(x, err=True),
        ) 
    return reads
