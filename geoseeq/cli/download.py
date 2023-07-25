import json
from os import makedirs
from os.path import dirname, join

import click
import pandas as pd

from geoseeq.result import _download_head
from geoseeq.utils import download_ftp

from .. import Organization
from .utils import convert_size, use_common_state


@click.group("download")
def cli_download():
    """Download objects from GeoSeeq."""
    pass


def _setup_download(state, sample_manifest, org_name, grp_name, sample_names):
    knex = state.get_knex()
    org = Organization(knex, org_name).get()
    grp = org.sample_group(grp_name).get()
    if sample_manifest:
        sample_names = set(sample_names) | set([el.strip() for el in sample_manifest if el])
    return grp, sample_names


@cli_download.command("metadata")
@use_common_state
@click.option(
    "--sample-manifest", type=click.File("r"), help="List of sample names to download from"
)
@click.argument("org_name")
@click.argument("grp_name")
@click.argument("sample_names", nargs=-1)
def cli_download_metadata(state, sample_manifest, org_name, grp_name, sample_names):
    """Download Sample Analysis Results for a set of samples."""
    grp, sample_names = _setup_download(state, sample_manifest, org_name, grp_name, sample_names)
    metadata = {}
    for sample in grp.get_samples(cache=False):
        if sample_names and sample.name not in sample_names:
            continue
        metadata[sample.name] = sample.metadata
    metadata = pd.DataFrame.from_dict(metadata, orient="index")
    metadata.to_csv(state.outfile)
    click.echo("Metadata successfully downloaded for samples.", err=True)


@cli_download.command("sample-results")
@use_common_state
@click.option("--folder-name", multiple=True, help='Name of folder on GeoSeeq to download from')
@click.option("--file-name", help="Name of file on GeoSeeq to download from")
@click.option("--target-dir", default=".")
@click.option(
    "--sample-manifest",
    default=None,
    type=click.File("r"),
    help="List of sample names to download from",
)
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@click.argument("org_name")
@click.argument("grp_name")
@click.argument("sample_names", nargs=-1)
def cli_download_sample_results(
    state,
    folder_name,
    file_name,
    target_dir,
    sample_manifest,
    download,
    org_name,
    grp_name,
    sample_names,
):
    """Download Sample Analysis Results for a set of samples."""
    grp, sample_names = _setup_download(state, sample_manifest, org_name, grp_name, sample_names)
    if sample_names:
        samples = [grp.sample(name).get() for name in sample_names]
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

@cli_download.command("files")
@use_common_state
@click.argument("org_name")
@click.argument("grp_name")
@click.argument("sample_names", nargs=-1)
@click.option(
    "--sample-manifest",
    default=None,
    type=click.File("r"),
    help="List of sample names to download from",
)
@click.option("--target-dir", default=".")
@click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
@click.option("--download/--urls-only", default=True, help="Download files or just print urls")
@click.option("--folder-type", type=click.Choice(['all', 'sample', 'project'], case_sensitive=False), default="sample", help='Name of folder on GeoSeeq to download from')
@click.option("--folder-name", multiple=True, help='Name of folder on GeoSeeq to download from')
@click.option("--sample-name-includes", multiple=True, help='A string which the sample name should include')
@click.option("--file-name", multiple=True, help="Name of file on GeoSeeq to download from")
@click.option("--extension", multiple=True, help="Extension of file to download from GeoSeeq")
@click.option("--with-versions/--without-versions", default=False, help="Download also versions not just the current file")
def cli_download_files(
    state,
    org_name,
    grp_name,
    sample_names,
    sample_manifest,
    sample_name_includes,
    target_dir,
    yes,
    folder_type,
    folder_name,
    file_name,
    extension,
    with_versions,
    download,
  
):
    grp, sample_names = _setup_download(state, sample_manifest, org_name, grp_name, sample_names)
    sample_uuids = []
    if sample_names:
        samples = [grp.sample(name).get() for name in sample_names]
        sample_uuids = [sample.uuid for sample in samples]
    data = {
        "sample_uuids": sample_uuids,
        "sample_names": sample_name_includes,
        "folder_type": folder_type,
        "folder_names": folder_name,
        "file_names": file_name,
        "extensions": extension,
        "with_versions": with_versions
    }
    knex = state.get_knex()
    url = f"sample_groups/{grp.uuid}/download"
    response = knex.post(url, data)

    if not download:
        file_path = join(target_dir, "links.json")
        makedirs(dirname(file_path), exist_ok=True)
        data = json.dumps(response["links"])
        with open(file_path, "w") as f:
            f.write(data)

    else:
        files_size = convert_size(response['file_size_bytes'])
        no_size_info = f"No size info was found for {response['no_size_info_count']} file{'s' if 1 < response['no_size_info_count'] else ''}." if response['no_size_info_count'] else ""
        click.echo(f"Found {len(response['links'])} files to download with total size of {files_size}. {no_size_info}\n")
        for fname, url in response["links"].items():
            click.echo(f'File name: {fname}', err=True)
            click.echo(f'Url: {url}\n', err=True)
        click.echo(f'Targer directory: "{target_dir}"\n', err=True)
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