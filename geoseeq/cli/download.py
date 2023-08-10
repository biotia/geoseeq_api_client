import json
import logging
from os import makedirs
from os.path import dirname, join

import click
import pandas as pd

from .shared_params import (
    handle_project_id,
    project_id_arg,
    sample_ids_arg,
    handle_multiple_sample_ids,
    use_common_state,
    flatten_list_of_els_and_files
)
from geoseeq.result.utils import _download_head
from geoseeq.utils import download_ftp
from geoseeq.blob_constructors import (
    sample_result_file_from_uuid,
    project_result_file_from_uuid,
)
from geoseeq.knex import GeoseeqNotFoundError
from .utils import convert_size

logger = logging.getLogger('geoseeq_api')


@click.group("download")
def cli_download():
    """Download data from GeoSeeq."""
    pass



@cli_download.command("metadata")
@use_common_state
@sample_ids_arg
def cli_download_metadata(state, sample_ids):
    """Download metadata for a set of samples as a CSV.
    
    ---

    Example Usage:

    \b
    # Download metadata for samples S1, S2, and S3 in project "My Org/My Project"
    $ geoseeq download metadata "My Org/My Project" S1 S2 S3 > metadata.csv

    \b
    # Download metadata for all samples in project "My Org/My Project"
    $ geoseeq download metadata "My Org/My Project" > metadata.csv

    \b
    # Download metadata for two samples by their UUIDs
    $ geoseeq download metadata 2b721a88-7387-4085-86df-4995d263b3f9 746424e7-2408-407e-a68d-786c7f5c5da6 > metadata.csv

    \b
    # Download metadata from a list of sample UUIDs in a file
    $ echo "2b721a88-7387-4085-86df-4995d263b3f9" > sample_ids.txt
    $ echo "746424e7-2408-407e-a68d-786c7f5c5da6" >> sample_ids.txt
    $ geoseeq download metadata sample_ids.txt > metadata.csv

    ---

    Command Arguments:

    \b
    [SAMPLE_IDS]... can be a list of sample names or IDs, files containing a list of sample names or IDs, or a mix of both.
    The first element in the list can optionally be a project ID or name.
    If a project ID is not provided, then sample ids must be UUIDs or GRNs, not names.
    If only a project ID is provided, then metadata for all samples in that project will be downloaded.

    ---
    """
    samples = handle_multiple_sample_ids(state.knex, sample_ids)
    click.echo(f"Found {len(samples)} samples.", err=True)
    metadata = {}
    for sample in samples:
        metadata[sample.name] = sample.metadata
    metadata = pd.DataFrame.from_dict(metadata, orient="index")
    metadata.to_csv(state.outfile)
    click.echo("Metadata successfully downloaded for samples.", err=True)


@cli_download.command("files")
@use_common_state
@click.option("--target-dir", default=".")
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@click.option("--folder-type", type=click.Choice(['all', 'sample', 'project'], case_sensitive=False), default="all", help='Download files from sample folders, project folders, or both')
@click.option("--folder-name", multiple=True, help='Filter folders for names that include this string. Case insensitive.')
@click.option("--sample-name-includes", multiple=True, help='Filter samples for names that include this string. Case insensitive.')
@click.option("--file-name", multiple=True, help="Filter files for names that include this string. Case insensitive.")
@click.option("--extension", multiple=True, help="Only download files with this extension. e.g. 'fastq.gz', 'bam', 'csv'")
@click.option("--with-versions/--without-versions", default=False, help="Download all versions of a file, not just the latest")
@project_id_arg
@sample_ids_arg
def cli_download_files(
    state,
    sample_name_includes,
    target_dir,
    yes,
    folder_type,
    folder_name,
    file_name,
    extension,
    with_versions,
    download,
    project_id,
    sample_ids,
):
    """Download files from a GeoSeeq project.
    
    This command will download multiple files from a GeoSeeq project. You can filter
    files by file extension, folder name, sample name, file name, and file extension.
    You can also choose to download all versions of a file, not just the latest.

    ---

    Example Usage:

    \b
    # Download all fastq files from all samples in "My Org/My Project"
    $ geoseeq download files "My Org/My Project" --extension fastq.gz --folder-type sample

    \b
    # Download fastq files from the MetaSUB Consortium CSD16 project that have been cleaned
    # e.g. "https://portal.geoseeq.com/samples/9c60aa67-eb3d-4b02-9c77-94e22361b2f3/analysis-results/b4ae15d2-37b9-448b-9946-3d716826eaa8"
    $ geoseeq download files "MetaSUB Consortium/CSD16" \\
        --folder-type sample `# Only download files from sample folders, not project folders` \\
        --folder-name "clean_reads"  # created by MetaSUB , not all projects will have this folder

    \b
    # Download a table of taxonomic abundances from the MetaSUB Consortium CSD17 project
    # produced by the GeoSeeq Search tool
    $ geoseeq download files "MetaSUB Consortium/CSD17" --folder-type project --folder-name "GeoSeeq Search" --file-name "Taxa Table"

    \b
    # Download assembly contigs from two samples in the MetaSUB Consortium CSD16 project
    $ geoseeq download files "MetaSUB Consortium/CSD16" `# specify the project` \ 
        haib17CEM4890_H2NYMCCXY_SL254769 haib17CEM4890_H2NYMCCXY_SL254773 `# specify the samples by name` \ 
        --folder-type sample --extension '.contigs.fasta' # filter for contig files

    ---

    Command Arguments:

    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.
    
    \b
    [SAMPLE_IDS]... can be a list of sample names or IDs, files containing a list of sample names or IDs, or a mix of both.
    ---
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id)
    samples = []
    if sample_ids:
        logger.info(f"Fetching info for {len(sample_ids)} samples.")
        samples = handle_multiple_sample_ids(knex, sample_ids, proj=proj)

    data = {
        "sample_uuids": [s.uuid for s in samples],
        "sample_names": sample_name_includes,
        "folder_type": folder_type,
        "folder_names": folder_name,
        "file_names": file_name,
        "extensions": extension,
        "with_versions": with_versions
    }
    url = f"sample_groups/{proj.uuid}/download"
    response = knex.post(url, data)

    if not download:
        data = json.dumps(response["links"])
        print(data, file=state.outfile)

    else:
        files_size = convert_size(response['file_size_bytes'])
        no_size_info = f"No size info was found for {response['no_size_info_count']} files." if response['no_size_info_count'] else ""
        click.echo(f"Found {len(response['links'])} files to download with total size of {files_size}. {no_size_info}\n")
        for fname, url in response["links"].items():
            clean_url = url.split("?")[0]
            click.echo(f'{clean_url} -> {target_dir}/{fname}')
        if not yes:
            click.confirm('Do you want to download these files?', abort=True)

        for fname, url in response["links"].items():
            click.echo(f"Downloading file {fname}")
            file_path = join(target_dir, fname)
            makedirs(dirname(file_path), exist_ok=True)
            if url.startswith("ftp"):
                download_ftp(url, file_path)
            else:
                _download_head(url, file_path)


@cli_download.command("ids")
@use_common_state
@click.option("--target-dir", default=".")
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@click.argument("ids", nargs=-1)
def cli_download_ids(state, target_dir, yes, download, ids):
    """Download a files from GeoSeeq based on their UUID or GeoSeeq Resource Number (GRN).

    This command downloads files directly based on their ID. This is used for "manual"
    downloads of relatively small amounts of data. For bulk downloads, use the
    `geoseeq download files` command.

    ---

    Example Usage:

    # Download a single file
    $ geoseeq download ids 9c60aa67-eb3d-4b02-9c77-94e22361b2f3

    # Download multiple files
    $ geoseeq download ids 9c60aa67-eb3d-4b02-9c77-94e22361b2f3 9c60aa67-eb3d-4b02-9c77-94e22361b2f3

    ---

    Command Arguments:

    [IDS]... can be a list of sample names or IDs, files containing a list of sample names or IDs, or a mix of both.

    ---
    """
    result_file_ids = flatten_list_of_els_and_files(ids)
    knex = state.get_knex()
    result_files = []
    for result_id in result_file_ids:
        result_uuid = result_id.split(':')[-1]
        # we guess that this is a sample file, TODO: use GRN if available
        try:
            result_file = sample_result_file_from_uuid(knex, result_uuid)
        except GeoseeqNotFoundError:
            result_file = project_result_file_from_uuid(knex, result_uuid)
        result_files.append(result_file)

    if not download:
        for result_file in result_files:
            print(result_file.get_download_url(), file=state.outfile)
        return
    
    for result_file in result_files:
        click.echo(f"{result_file} -> {target_dir}/{result_file.get_referenced_filename()}")
    if not yes:
        click.confirm('Do you want to download these files?', abort=True)

    for result_file in result_files:
        click.echo(f"Downloading file {result_file.get_referenced_filename()}")
        file_path = join(target_dir, result_file.get_referenced_filename())
        makedirs(dirname(file_path), exist_ok=True)
        result_file.download(file_path)
