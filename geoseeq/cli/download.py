import json
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
)


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


@cli_download.command("sample-results")
@use_common_state
@click.option("--folder-name", multiple=True, help='Name of folder on GeoSeeq to download from')
@click.option("--file-name", help="Name of file on GeoSeeq to download from")
@click.option("--target-dir", default=".")
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@project_id_arg
@click.argument("sample_names", nargs=-1)
def cli_download_sample_results(
    state,
    folder_name,
    file_name,
    target_dir,
    sample_manifest,
    download,
    project_id,
    sample_names,
):
    """Download Sample Analysis Results for a set of samples."""
    grp = handle_project_id(state.knex, project_id, create=False)
    if sample_manifest:
        sample_names = set(sample_names) | set([el.strip() for el in sample_manifest if el])

    else:
        samples = grp.get_samples(cache=False)
    for sample in samples:
        if folder_name:
            result_folders = [sample.result_folder(name).get() for name in folder_name]
        else:
            result_folders = sample.get_result_folders()
        for ar in result_folders:
            for field in ar.get_fields(cache=False):
                if file_name and field.name != file_name:
                    continue
                if not download:  # download urls to a file, not actual files.
                    try:
                        print(
                            field.get_download_url(),
                            field.get_referenced_filename(),
                            file=state.outfile,
                        )
                    except TypeError:
                        pass
                    continue
                filename = join(target_dir, field.get_blob_filename()).replace("::", "__")
                makedirs(dirname(filename), exist_ok=True)
                click.echo(f"Downloading BLOB {sample} :: {ar} :: {field} to {filename}", err=True)
                with open(filename, "w") as blob_file:
                    blob_file.write(json.dumps(field.stored_data))
                try:
                    filename = join(target_dir, field.get_referenced_filename()).replace("::", "__")
                    click.echo(
                        f"Downloading FILE {sample} :: {ar} :: {field} to {filename}", err=True
                    )
                    field.download_file(filename=filename)
                except TypeError:
                    pass
                click.echo("done.", err=True)
