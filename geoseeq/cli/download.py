import json
import logging
from os import makedirs
from os.path import dirname, join

import click
import pandas as pd
from multiprocessing import Pool
from .shared_params import (
    handle_project_id,
    project_id_arg,
    sample_ids_arg,
    handle_multiple_sample_ids,
    use_common_state,
    flatten_list_of_els_and_files,
    yes_option,
)
from geoseeq.result.file_download import download_url
from geoseeq.utils import download_ftp
from geoseeq.id_constructors import (
    result_file_from_uuid,
    result_file_from_name,
)
from geoseeq.knex import GeoseeqNotFoundError
from .progress_bar import PBarManager
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
    samples = handle_multiple_sample_ids(state.get_knex(), sample_ids)
    click.echo(f"Found {len(samples)} samples.", err=True)
    metadata = {}
    for sample in samples:
        metadata[sample.name] = sample.metadata
    metadata = pd.DataFrame.from_dict(metadata, orient="index")
    metadata.to_csv(state.outfile)
    click.echo("Metadata successfully downloaded for samples.", err=True)


def _download_one_file(args):
    url, file_path, pbar = args
    return download_url(url, filename=file_path, progress_tracker=pbar)


cores_option = click.option('--cores', default=1, help='Number of downloads to run in parallel')

@cli_download.command("files")
@use_common_state
@cores_option
@click.option("--target-dir", default=".")
@yes_option
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
    cores,
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
    logger.info(f"Found project \"{proj.name}\"")
    samples = []
    if sample_ids:
        logger.info(f"Fetching info for {len(sample_ids)} samples.")
        samples = handle_multiple_sample_ids(knex, sample_ids, proj=proj)

    response = proj.bulk_find_files(
        sample_uuids=[s.uuid for s in samples],
        sample_name_includes=sample_name_includes,
        folder_types=folder_type,
        folder_names=folder_name,
        file_names=file_name,
        extensions=extension,
        with_versions=with_versions,
    )


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

        download_args = []
        pbars = PBarManager()
        for fname, url in response["links"].items():
            click.echo(f"Downloading file {fname}")
            file_path = join(target_dir, fname)
            makedirs(dirname(file_path), exist_ok=True)
            pbar = pbars.get_new_bar(file_path)
            download_args.append((url, file_path, pbar))
            if cores == 1:
                download_url(url, filename=file_path, progress_tracker=pbar)

        if cores > 1:
            with Pool(cores) as p:
                for _ in p.imap_unordered(_download_one_file, download_args):
                    pass


@cli_download.command("ids")
@use_common_state
@cores_option
@click.option("--target-dir", default=".")
@click.option("-n", "--file-name", multiple=True, help="File name to use for downloaded files. If set you must specify once per ID.")
@yes_option
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@click.option('--head', default=None, type=int, help='Download the first N bytes of each file')
@click.argument("ids", nargs=-1)
def cli_download_ids(state, cores, target_dir, file_name, yes, download, head, ids):
    """Download a files from GeoSeeq based on their UUID or GeoSeeq Resource Number (GRN).

    This command downloads files directly based on their ID. This is used for "manual"
    downloads of relatively small amounts of data. For bulk downloads, use the
    `geoseeq download files` command.

    ---

    Example Usage:

    \b
    # Download a single file
    $ geoseeq download ids 9c60aa67-eb3d-4b02-9c77-94e22361b2f3

    \b
    # Download multiple files
    $ geoseeq download ids 9c60aa67-eb3d-4b02-9c77-94e22361b2f3 9c60aa67-eb3d-4b02-9c77-94e22361b2f3

    \b
    # Download a file by its name
    $ geoseeq download ids "My Project/My Sample/My File"

    \b 
    # Download a file by its name and specify a file name to use for the downloaded file
    $ geoseeq download ids "My Project/My Sample/My File" -n my_file.fastq.gz

    \b
    # Download multiple files by their names and specify a file name to use for the downloaded files
    $ geoseeq download ids "My Project/My Sample/My File" "My Project/My Sample/My File 2" \\
        -n my_file.fastq.gz -n my_file_2.fastq.gz

    ---

    Command Arguments:

    [IDS]... can be a list of result names or IDs, files containing a list of result names or IDs, or a mix of both.

    ---
    """
    result_file_ids = flatten_list_of_els_and_files(ids)
    cores = max(cores, len(result_file_ids))  # don't use more cores than files
    knex = state.get_knex()
    result_files = []
    for result_id in result_file_ids:
        # we guess that this is a sample file to start, TODO: use GRN if available
        if "/" in result_id:  # result name/path
            result_file = result_file_from_name(knex, result_id)
        else:  # uuid or grn
            result_uuid = result_id.split(':')[-1]
            result_file = result_file_from_uuid(knex, result_uuid)
        result_files.append(result_file)

    if not download:
        for result_file in result_files:
            print(result_file.get_download_url(), file=state.outfile)
        return
    
    if file_name:
        if len(file_name) != len(result_files):
            raise ValueError("If you specify file names then you must specify the same number of names and ids.")
        result_files_with_names = list(zip(result_files, file_name))
    else:
        result_files_with_names = [
            (result_file, result_file.get_referenced_filename()) for result_file in result_files
        ]
            

    for result_file, filename in result_files_with_names:
        click.echo(f"{result_file} -> {target_dir}/{filename}")
    if not yes:
        click.confirm('Do you want to download these files?', abort=True)

    download_args = []
    pbars = PBarManager()
    for result_file, filename in result_files_with_names:
        click.echo(f"Downloading file {filename}")
        file_path = join(target_dir, filename)
        makedirs(dirname(file_path), exist_ok=True)
        pbar = pbars.get_new_bar(file_path)
        download_args.append((result_file, file_path, pbar))
        if cores == 1:
            result_file.download(file_path, progress_tracker=pbar, head=head)

    if cores > 1:
        with Pool(cores) as p:
            for _ in p.imap_unordered(_download_one_file, download_args):
                pass
