import json
import logging
from os import makedirs, getcwd
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
from geoseeq.file_system.filesystem_objects import (
    ProjectOnFilesystem,
    ResultFileOnFilesystem,
    FILE_STATUSES,
    LOCAL_CHANGE_STATUSES,
    FILE_STATUS_IS_LOCAL_STUB,
    FILE_STATUS_ALL_OK,
)
from geoseeq.file_system.parallel_manager import AbnormalObjectListManager



logger = logging.getLogger('geoseeq_api')


@click.group("project")
def cli_project():
    """Download data from GeoSeeq."""
    pass


@cli_project.command("clone")
@use_common_state
@click.option('--use-stubs/--full-files', default=True, help='Download full files or stubs')
@click.option('--target-dir', '-d', default=None, help='Directory to download the project to')
@project_id_arg
def cli_clone_project(state, use_stubs, target_dir, project_id):
    """Clone a project to the local filesystem.
    """
    knex = state.get_knex().set_auth_required()
    proj = handle_project_id(knex, project_id)
    logger.info(f"Found project \"{proj.name}\"")
    if target_dir is None:
        target_dir = proj.name

    project = ProjectOnFilesystem(proj, target_dir)
    project.download(use_stubs=use_stubs)


@cli_project.command("unstub")
@use_common_state
@yes_option
@click.argument("paths", nargs=-1)
def cli_unstub_files(state, yes, paths):
    """Download the actual files that are stubs in a project on the local filesystem.
    """
    download_manger = GeoSeeqDownloadManager()
    for path in paths:
        fs_rf = ResultFileOnFilesystem.from_path(path)
        if not fs_rf.is_stub:
            click.echo(f"{fs_rf.local_path} is not a stub", err=True)
            continue
        fs_rf.add_self_to_download_manager(download_manger)
    if not yes:
        preview = download_manger.get_preview_string()
        click.confirm(f"Download the following files?\n{preview}", abort=True)
    download_manger.download_files()


@cli_project.command("stub")
@use_common_state
@yes_option
@click.argument("paths", nargs=-1)
def cli_stub_files(state, yes, paths):
    """Convert an actual file to a stub in a project on the local filesystem.
    """
    to_stub = []
    for path in paths:
        fs_rf = ResultFileOnFilesystem.from_path(path)
        if fs_rf.is_stub:
            click.echo(f"{fs_rf} is already a stub", err=True)
            continue
        _, status, _, _ = fs_rf.atomic_abnormal_status()[0]
        if status != FILE_STATUS_ALL_OK:
            click.echo(f"{fs_rf} is not in a clean state. Status: {status}", err=True)
            continue
        to_stub.append(fs_rf)
    if not to_stub:
        return
    if not yes:
        to_stub_preview = "\n".join([rf.path for rf in to_stub])
        click.confirm(f"Stub the following files?\n{to_stub_preview}", abort=True)
    for fs_rf in to_stub:
        fs_rf.to_stub()



@cli_project.command("status")
@use_common_state
@click.option("--list-stubs/--no-list-stubs", default=False, help="List files that are stubs in status report")
@click.option('--cores', default=1, help='Number of status checks to run in parallel')
def cli_project_status(state, list_stubs, cores):
    """Check the status of a project on the local filesystem.
    """
    project = ProjectOnFilesystem.from_path(getcwd(), recursive=True)

    objs_by_status = {
        file_status: [] for file_status in FILE_STATUSES
    }
    manager = AbnormalObjectListManager(threads=cores).add_object(project)
    for obj_type, status, local_path, obj in manager.list_abnormal_objects():
        objs_by_status[status].append((obj_type, local_path, obj))

    print(f"Project: {project.project.name}")
    for status, objs in objs_by_status.items():
        if not list_stubs and status == FILE_STATUS_IS_LOCAL_STUB:
            continue
        print(f"Status: {status}")
        for obj_type, local_path, obj in objs:
            if status in LOCAL_CHANGE_STATUSES:
                print(f"  {obj_type}: {project.path_from_project_root(local_path)} -> {obj}")
            else:
                print(f"  {obj_type}:  {obj} -> {project.path_from_project_root(local_path)}")


@cli_project.command("add")
@use_common_state
@click.argument('path', type=click.Path(exists=True))
def cli_project_add(state, path):
    """Add a local file to a geoSeeq project stored on the local filesystem.
    """
    print(path)
    if path[0] != "/":
        path = join(getcwd(), path)
    print(path)
    project = ProjectOnFilesystem.from_path(getcwd(), recursive=True)
    results = project.add_path_to_project_locally(path)
    for result in results:
        print(result)


