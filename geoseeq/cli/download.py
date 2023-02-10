import json
from os import makedirs
from os.path import dirname, join

import click
import pandas as pd

from .. import Organization
from .utils import use_common_state


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
@click.option("--module-name")
@click.option("--field-name")
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
    module_name,
    field_name,
    target_dir,
    sample_manifest,
    download,
    org_name,
    grp_name,
    sample_names,
):
    """Download Sample Analysis Results for a set of samples."""
    grp, sample_names = _setup_download(state, sample_manifest, org_name, grp_name, sample_names)
    for sample in grp.get_samples(cache=False):
        if sample_names and sample.name not in sample_names:
            continue
        for ar in sample.get_analysis_results(cache=False):
            if module_name and ar.module_name != module_name:
                continue
            for field in ar.get_fields(cache=False):
                if field_name and field.name != field_name:
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
