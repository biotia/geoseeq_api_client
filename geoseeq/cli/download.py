import json
import logging
from os import makedirs
from os.path import dirname, join

import click
import pandas as pd
from multiprocessing import Pool
from .shared_params import (
    handle_project_id,
    handle_folder_id,
    project_id_arg,
    sample_ids_arg,
    handle_multiple_sample_ids,
    handle_multiple_result_file_ids,
    use_common_state,
    flatten_list_of_els_and_files,
    yes_option,
    module_option,
    ignore_errors_option,
    folder_ids_arg,
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
from geoseeq.constants import FASTQ_MODULE_NAMES
from geoseeq.result import ResultFile
from geoseeq.upload_download_manager import GeoSeeqDownloadManager

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

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex().set_auth_required()
    samples = handle_multiple_sample_ids(knex, sample_ids)
    click.echo(f"Found {len(samples)} samples.", err=True)
    metadata = {}
    for sample in samples:
        metadata[sample.name] = sample.metadata
    metadata = pd.DataFrame.from_dict(metadata, orient="index")
    metadata.to_csv(state.outfile)
    click.echo("Metadata successfully downloaded for samples.", err=True)


cores_option = click.option('--cores', default=1, help='Number of downloads to run in parallel')
head_option = click.option('--head', default=None, type=int, help='Download the first N bytes of each file')
alt_id_option = click.option('--alt-sample-id', default=None, help='Specify an alternate sample id from the project metadata to id samples')

@cli_download.command("files")
@use_common_state
@cores_option
@click.option("--target-dir", default=".")
@yes_option
@head_option
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@click.option("--folder-type", type=click.Choice(['all', 'sample', 'project'], case_sensitive=False), default="all", help='Download files from sample folders, project folders, or both')
@click.option("--folder-name", multiple=True, help='Filter folders for names that include this string. Case insensitive.')
@click.option("--sample-name-includes", multiple=True, help='Filter samples for names that include this string. Case insensitive.')
@click.option("--file-name", multiple=True, help="Filter files for names that include this string. Case insensitive.")
@click.option("--extension", multiple=True, help="Only download files with this extension. e.g. 'fastq.gz', 'bam', 'csv'")
@click.option("--with-versions/--without-versions", default=False, help="Download all versions of a file, not just the latest")
@ignore_errors_option
@alt_id_option
@project_id_arg
@sample_ids_arg
def cli_download_files(
    state,
    cores,
    sample_name_includes,
    target_dir,
    yes,
    head,
    folder_type,
    folder_name,
    file_name,
    extension,
    with_versions,
    download,
    ignore_errors,
    alt_sample_id,
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
    $ geoseeq download files "MetaSUB Consortium/CSD16" `# specify the project` \\ 
        haib17CEM4890_H2NYMCCXY_SL254769 haib17CEM4890_H2NYMCCXY_SL254773 `# specify the samples by name` \\ 
        --folder-type sample --extension '.contigs.fasta' # filter for contig files

    \b
    # Download files from a sample in the metasub project using an alternate sample id called "barcode"
    $ geoseeq download files 'MetaSUB Consortium/Cell Paper' `# specify the project` \\
        235183938 `# the alternate sample name (in this case a barcode number)` \\
        --alt-sample-id barcode `# specify the alternate sample id column name` \\
        --folder-type 'sample' `# only download files from sample folders`

    ---

    Command Arguments:

    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.
    
    \b
    [SAMPLE_IDS]... can be a list of sample names or IDs, files containing a list of sample names or IDs, or a mix of both.
    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex().set_auth_required()
    proj = handle_project_id(knex, project_id)
    logger.info(f"Found project \"{proj.name}\"")
    samples = []
    if sample_ids:
        logger.info(f"Fetching info for {len(sample_ids)} samples.")
        samples = handle_multiple_sample_ids(knex, sample_ids, proj=proj, alternate_id_col=alt_sample_id)

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
            click.confirm(f'Do you want to download {len(response["links"])} files?', abort=True)

        download_manager = GeoSeeqDownloadManager(
            n_parallel_downloads=cores,
            ignore_errors=ignore_errors,
            log_level=state.log_level,
            progress_tracker_factory=PBarManager().get_new_bar,
            head=head,
        )
        for fname, url in response["links"].items():
            download_manager.add_download(url, join(target_dir, fname))
        
        click.echo(download_manager.get_preview_string(), err=True)
        if not yes:
            click.confirm('Continue?', abort=True)
        logger.info(f'Downloading {len(download_manager)} files to {target_dir}')
        download_manager.download_files()



@cli_download.command("folders")
@use_common_state
@cores_option
@click.option("-t", "--target-dir", default=".")
@yes_option
@head_option
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@ignore_errors_option
@click.option('--hidden/--no-hidden', default=True, help='Download hidden files in folder')
@folder_ids_arg
def cli_download_folders(state, cores, target_dir, yes, head, download, ignore_errors, hidden, folder_ids):
    """Download entire folders from GeoSeeq.
    
    This command downloads folders directly based on their ID. This is used for "manual"
    downloads of GeoSeeq folders.

    ---

    Example Usage:

    \b
    # Download a single folder
    $ geoseeq download folders 9c60aa67-eb3d-4b02-9c77-94e22361b2f3

    \b
    # Download multiple folders
    $ geoseeq download folders 9c60aa67-eb3d-4b02-9c77-94e22361b2f3 "My Org/My Project/My Sample/My Folder"

    ---

    Command Arguments:

    [FOLDER_IDS]... a list of folder names, IDs, or GRNs
    """
    knex = state.get_knex()
    result_folders = [
        handle_folder_id(knex, folder_id, create=False) for folder_id in folder_ids
    ]
    download_manager = GeoSeeqDownloadManager(
        n_parallel_downloads=cores,
        ignore_errors=ignore_errors,
        log_level=state.log_level,
        progress_tracker_factory=PBarManager().get_new_bar,
        head=head,
    )
    for result_folder in result_folders:
        download_manager.add_result_folder_download(
            result_folder, join(target_dir, result_folder.name), hidden_files=hidden
        )
    click.echo(download_manager.get_preview_string(), err=True)
    if not yes:
        click.confirm('Continue?', abort=True)
    logger.info(f'Downloading {len(download_manager)} folders to {target_dir}')
    download_manager.download_files()


@cli_download.command("ids")
@use_common_state
@cores_option
@click.option("--target-dir", default=".")
@click.option("-n", "--file-name", multiple=True, help="File name to use for downloaded files. If set you must specify once per ID.")
@yes_option
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@head_option
@ignore_errors_option
@click.argument("ids", nargs=-1)
def cli_download_ids(state, cores, target_dir, file_name, yes, download, head, ignore_errors, ids):
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
    $ geoseeq download ids "My Org/My Project/My Sample/My Folder/My File"

    \b 
    # Download a file by its name and specify a file name to use for the downloaded file
    $ geoseeq download ids "My Org/My Project/My Sample/My Folder/My File" -n my_file.fastq.gz

    \b
    # Download multiple files by their names and specify a file name to use for the downloaded files
    $ geoseeq download ids "My Org/My Project/My Sample/My Folder/My File" "My Project/My Sample/My File 2" \\
        -n my_file.fastq.gz -n my_file_2.fastq.gz

    ---

    Command Arguments:

    [IDS]... can be a list of result names or IDs, files containing a list of result names or IDs, or a mix of both.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex().set_auth_required()
    result_files = handle_multiple_result_file_ids(knex, ids)
    cores = max(cores, len(result_files))  # don't use more cores than files

    if file_name:
        if len(file_name) != len(result_files):
            raise ValueError("If you specify file names then you must specify the same number of names and ids.")
        result_files_with_names = list(zip(result_files, file_name))
    else:
        result_files_with_names = [
            (result_file, result_file.get_local_filename()) for result_file in result_files
        ]
    download_manager = GeoSeeqDownloadManager(
        n_parallel_downloads=cores,
        ignore_errors=ignore_errors,
        log_level=state.log_level,
        head=head,
        progress_tracker_factory=PBarManager().get_new_bar,
    )
    if not download:
        print(download_manager.get_url_string(), file=state.outfile)
    else:
        for result_file, filename in result_files_with_names:
            download_manager.add_download(result_file, join(target_dir, filename))
        click.echo(download_manager.get_preview_string(), err=True)
        if not yes:
            click.confirm('Continue?', abort=True)
        logger.info(f'Downloading {len(download_manager)} files to {target_dir}')
        download_manager.download_files()


def _get_sample_result_files_with_names(sample, module_name=None, first=False):
    result_files_with_names = []
    for read_type, folder in sample.get_all_fastqs().items():
        if module_name and module_name != read_type:
            continue
        for folder_name, result_files in folder.items():
            for lane_num, result_file in enumerate(result_files):
                lane_num = lane_num + 1  # 1 indexed
                if read_type in ["short_read::paired_end"]:
                    key = (sample, read_type, 1, lane_num)  # sample name, read type, read number, lane number
                    result_files_with_names.append(
                        (result_file[0], result_file[0].get_referenced_filename(), key)
                    )
                    key = (sample, read_type, 2, lane_num)
                    result_files_with_names.append(
                        (result_file[1], result_file[1].get_referenced_filename(), key)
                    )
                else:
                    key = (sample, read_type, 1, lane_num)
                    result_files_with_names.append(
                        (result_file, result_file.get_referenced_filename(), key)
                    )
            if first:
                break

    return result_files_with_names


def _make_read_configs(download_results, config_dir="."):
    """Make JSON config files that look like this.
    
    {
        "sample_name": "small",
        "reads_1": ["small.fq.gz"],
        "reads_2": [],
        "fastq_checksum": "",
        "data_type": "short-read",
        "bdx_result_dir": "results",
        "geoseeq_uuid": "05bf22e9-9d25-42db-af25-31bc538a7006"
    }
    """
    config_blobs = {}  # sample ids -> config_blobs
    download_results = sorted(download_results, key=lambda x: x[1][3])  # sort by lane number
    for local_path, (sample, read_type, read_num, lane_num), _ in download_results:
        if sample.name not in config_blobs:
            config_blobs[sample.name] = {
                "sample_name": sample.name,
                "reads_1": [],
                "reads_2": [],
                "fastq_checksum": "",
                "data_type": "short-read",
                "bdx_result_dir": "results",
                "geoseeq_uuid": sample.uuid,
            }
        if read_num == 1:
            config_blobs[sample.name]["reads_1"].append(local_path)  # sorted by lane number
        else:
            config_blobs[sample.name]["reads_2"].append(local_path)

    for sample_name, config_blob in config_blobs.items():
        config_path = join(config_dir, f"{sample_name}.config.json")
        with open(config_path, "w") as f:
            json.dump(config_blob, f, indent=4)


@cli_download.command("fastqs")
@use_common_state
@cores_option
@click.option("--target-dir", default=".")
@yes_option
@click.option("--first/--all", default=False, help="Download only the first folder of fastq files for each sample.")
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@click.option("--config-dir", default=None, help="Directory to write read config files. If unset do not write config files.")
@module_option(FASTQ_MODULE_NAMES, use_default=False)
@ignore_errors_option
@alt_id_option
@project_id_arg
@sample_ids_arg
def cli_download_fastqs(state,
                        cores,
                        target_dir,
                        yes,
                        first,
                        download,
                        config_dir,
                        module_name,
                        ignore_errors,
                        alt_sample_id,
                        project_id,
                        sample_ids
):
    """Download fastq files from a GeoSeeq project.

    This command will download fastq files from a GeoSeeq project. You can filter
    files by sample name and by specific fastq read types.

    ---

    Example Usage:

    \b
    # Download all fastq files from all samples in "My Org/My Project"
    $ geoseeq download fastqs "My Org/My Project"

    \b
    # Download paired end fastq files from all samples in "My Org/My Project"
    $ geoseeq download fastqs "My Org/My Project" --module-name short_read::paired_end

    \b
    # Download all fastq files from two samples in "My Org/My Project"
    $ geoseeq download fastqs "My Org/My Project" S1 S2

    \b
    # Download all fastq files from a single sample using an alternate sample id called "barcode"
    $ geoseeq download fastqs 'MetaSUB Consortium/Cell Paper' 235183938 --alt-sample-id barcode

    ---

    Command Arguments:

    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.

    \b
    [SAMPLE_IDS]... can be a list of sample names or IDs, files containing a list of sample names or IDs, or a mix of both.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex().set_auth_required()
    proj = handle_project_id(knex, project_id)
    logger.info(f"Found project \"{proj.name}\"")
    samples = []
    if sample_ids:
        logger.info(f"Fetching info for {len(sample_ids)} samples.")
        samples = handle_multiple_sample_ids(knex, sample_ids, proj=proj, alternate_id_col=alt_sample_id)
    else:
        logger.info("Fetching info for all samples in project.")
        samples = proj.get_samples()

    result_files_with_names = []
    for sample in samples:
        try:
            result_files_with_names += _get_sample_result_files_with_names(sample, module_name, first)
        except Exception as e:
            logger.error(f"Error fetching fastq files for sample {sample.name}: {e}")
            if not ignore_errors:
                raise e

    if len(result_files_with_names) == 0:
        click.echo("No suitable fastq files found.")
        return
    
    download_manager = GeoSeeqDownloadManager(
        n_parallel_downloads=cores,
        ignore_errors=ignore_errors,
        log_level=state.log_level,
        progress_tracker_factory=PBarManager().get_new_bar,
    )
    for result_file, filename, key in result_files_with_names:
        download_manager.add_download(result_file, join(target_dir, filename), key=key)
    if not download:
        print(download_manager.get_url_string(), file=state.outfile)
    else:
        click.echo(download_manager.get_preview_string(), err=True)
        if not yes:
            click.confirm('Continue?', abort=True)
        logger.info(f'Downloading {len(download_manager)} files to {target_dir}')
        download_results = download_manager.download_files()
        if config_dir:
            _make_read_configs(download_results, config_dir)
