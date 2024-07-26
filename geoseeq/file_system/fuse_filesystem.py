
import os
from .fuse_passthrough import FusePassthrough
from geoseeq.knex import with_knex
from .filesystem_objects import (
    ProjectOnFilesystem,
    ResultFileOnFilesystem,
)
from geoseeq.id_constructors import project_from_id


class GeoSeeqProjectFileSystem(FusePassthrough):
    """Mount a GeoSeeq project as a filesystem.
    
    The project will automatically have this directory structure:
    - <root>/project_results/<project_result_folder_name>/...
    - <root>/sample_results/<sample_name>/...
    - <root>/metadata/sample_metadata.csv
    - <root>/.config/config.json
    """

    def __init__(self, project_on_fs):
        super().__init__(project_on_fs.path)
        self.fs_project = project_on_fs

    def open(self, path, flags):
        try:
            fs_result_file = ResultFileOnFilesystem.from_path(path)
        except FileNotFoundError:
            # this indicates that the file is not a result file, so we can just pass through
            return os.open(path, flags)
        if fs_result_file.is_stub:
            print(f"Downloading {fs_result_file.name}...")
            # TODO: this could break workflows by downloading big files in the middle of things
            # In principle we can download the file chunk by chunk and hand them off to the user
            fs_result_file.download()
        return os.open(path, flags)

    @classmethod
    @with_knex
    def clone_mount(cls, knex, project_id, target_dir=None, use_stubs=True):
        knex = knex.set_auth_required()
        proj = project_from_id(knex, project_id)
        if target_dir is None:
            target_dir = proj.name

        project = ProjectOnFilesystem(proj, target_dir)
        project.download(use_stubs=use_stubs)
        return cls(project)