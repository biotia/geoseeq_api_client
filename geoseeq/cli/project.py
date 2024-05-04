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
from geoseeq.file_system.filesystem_download import (
    ProjectOnFilesystem,
    FILE_STATUS_MODIFIED_REMOTE,
    FILE_STATUS_MODIFIED_LOCAL,
    FILE_STATUS_NEW_LOCAL,
    FILE_STATUS_NEW_REMOTE,
    FILE_STATUS_IS_LOCAL_STUB,
)


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


@cli_project.command("status")
@use_common_state
def cli_project_status(state):
    """Check the status of a project on the local filesystem.
    """
    project = ProjectOnFilesystem.from_path(getcwd(), recursive=True)

    objs_by_status = {
        FILE_STATUS_MODIFIED_LOCAL: [],
        FILE_STATUS_MODIFIED_REMOTE: [],
        FILE_STATUS_NEW_LOCAL: [],
        FILE_STATUS_NEW_REMOTE: [],
        FILE_STATUS_IS_LOCAL_STUB: [],
    }
    for obj_type, status, local_path, obj in project.list_abnormal_objects():
        objs_by_status[status].append((obj_type, local_path, obj))

    print(f"Project: {project.project.name}")
    for status, objs in objs_by_status.items():
        print(f"Status: {status}")
        for obj_type, local_path, obj in objs:
            if status in (FILE_STATUS_MODIFIED_LOCAL, FILE_STATUS_NEW_LOCAL):
                print(f"  {obj_type}: {project.path_from_project_root(local_path)} -> {obj}")
            else:
                print(f"  {obj_type}:  {obj} -> {project.path_from_project_root(local_path)}")