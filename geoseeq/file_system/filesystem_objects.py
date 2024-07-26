
import os
import json
from geoseeq import (
    result_file_from_id,
    result_folder_from_id,
    sample_from_id,
    project_from_id,
)
from geoseeq.utils import md5_checksum
from time import time
from geoseeq import Knex
from .parallel_manager import AbnormalObjectListManager

FILE_STATUS_MODIFIED_REMOTE = 'MODIFIED_REMOTE'  # file was modified on the server
FILE_STATUS_MODIFIED_LOCAL = 'MODIFIED_LOCAL'  # file was modified locally
FILE_STATUS_NEW_LOCAL = 'NEW_LOCAL'  # file is new locally and ahas been added to the project
FILE_STATUS_NEW_REMOTE = 'NEW_REMOTE'  # file is new on the server
FILE_STATUS_IS_LOCAL_STUB = 'IS_LOCAL_STUB'  # file is a stub locally
FILE_STATUS_ALL_OK = 'ALL_OK'  # file is up to date synced and added
FILE_STATUS_NOT_ADDED = 'NOT_ADDED'  # file is present locally but is not added to the project

FILE_STATUSES = [
    FILE_STATUS_MODIFIED_REMOTE,
    FILE_STATUS_MODIFIED_LOCAL,
    FILE_STATUS_NEW_LOCAL,
    FILE_STATUS_NEW_REMOTE,
    FILE_STATUS_IS_LOCAL_STUB,
    FILE_STATUS_ALL_OK,
    FILE_STATUS_NOT_ADDED,
]
LOCAL_CHANGE_STATUSES = [
    FILE_STATUS_MODIFIED_LOCAL,
    FILE_STATUS_NEW_LOCAL,
    FILE_STATUS_NOT_ADDED,
]


def dedupe_modified_files(modified_files):
    """Remove duplicates from a list of modified files.
    
    This function will remove duplicates from a list of modified files
    based on the path to the file. The first instance of the file will be
    kept and all others will be removed.
    """
    seen = set()
    deduped = []
    for x in modified_files:
        if x[2] not in seen:
            deduped.append(x)
            seen.add(x[2])
    return deduped


def iterate_non_gs_files(path):
    """Iterate over all files in a directory that are not GeoSeeq files.
    
    This function will iterate over all files in a directory that are not
    GeoSeeq files. This is useful when you want to iterate over a directory
    and ignore all GeoSeeq files.
    """
    for filename in os.listdir(path):
        if filename.startswith('.gs_'):
            continue
        yield filename


class GeoSeeqObjectOnFilesystem:
    
    def download(self, use_stubs=False, exists_ok=False):
        raise NotImplementedError('This function is not implemented')

    def status_is_ok(self):
        raise NotImplementedError('This function is not implemented')
    
    def atomic_abnormal_status(self):
        raise NotImplementedError('This function is not implemented')
    
    def list_abnormal_objects(self, abnormal_manager=None):
        raise NotImplementedError('This function is not implemented')
    
    @classmethod
    def local_basename(cls, obj):
        return obj.name


class ResultFileOnFilesystem(GeoSeeqObjectOnFilesystem):
    """

    Note: unlike other filesystem classes the `path` is a file, not
    a directory. This is because the file is downloaded directly to 
    the path.
    """

    def __init__(self, result_file, path, kind):
        self.result_file = result_file
        self.path = path
        self.kind = kind

    @property
    def info_filepath(self):
        dirpath = os.path.dirname(self.path)
        basename = os.path.basename(self.path)
        return os.path.join(dirpath, f'.gs_result_file__{basename}')
    
    @property
    def is_stub(self):
        return os.path.exists(self.path) and os.path.getsize(self.path) == 0

    def to_stub(self):
        _, status, _, _ = self.atomic_abnormal_status()[0]
        if status != FILE_STATUS_ALL_OK:
            raise ValueError('File is not in a clean state')
        open(self.path, 'w').close()
        self.write_info_file()
    
    def file_is_ok(self, stubs_are_ok=False):
        if self.is_stub:
            return stubs_are_ok
        return self.result_file.download_needs_update(self.path)

    def download(self, use_stubs=False, exists_ok=False):
        if os.path.exists(self.info_filepath):
            if exists_ok and self.file_is_ok(stubs_are_ok=use_stubs):
                return
            elif not exists_ok:
                raise ValueError('Result file already exists at path: {}'.format(self.info_filepath))
        
        # Download the file
        if use_stubs:
            open(self.path, 'w').close()
        else:
            self.result_file.download(self.path)

        self.write_info_file()

    def local_file_checksum(self):
        if self.is_stub:
            return "__STUB__"
        return md5_checksum(self.path)
    
    def locally_modified(self):
        return self.local_file_checksum() != self.stored_checksum

    def status_is_ok(self, stubs_are_ok=False):
        # check for an info file
        if not os.path.exists(self.info_filepath):
            return False
        if stubs_are_ok:
            return True
        return not self.result_file.download_needs_update(self.path)

    @classmethod
    def from_path(cls, path, kind=None):
        obj = cls(None, path, kind)
        try:
            with open(obj.info_filepath, 'r') as f:
                result_file_info = json.load(f)
            result_file_uuid = result_file_info['uuid']
            if result_file_uuid:
                obj.result_file = result_file_from_id(result_file_info['uuid'])
            obj.kind = result_file_info['kind']
            obj.stored_checksum = result_file_info['checksum']
        except FileNotFoundError:
            pass
        return obj
    
    def write_info_file(self):
        result_file_info = {
            "uuid": self.result_file.uuid if self.result_file else None,
            "kind": self.kind,
            "checksum": self.local_file_checksum(),
        }
        with open(self.info_filepath, 'w') as f:
            json.dump(result_file_info, f)

    def atomic_abnormal_status(self):
        """Return a list with the abnormal status of this result file or empty list.
        
        Do not return status of any child objects.
        """
        if self.result_file is None:
            if os.path.exists(self.info_filepath):
                return [('FILE', FILE_STATUS_NEW_LOCAL, self.path, None)]
            else:
                return [('FILE', FILE_STATUS_NOT_ADDED, self.path, None)]
        if not os.path.exists(self.path):
            return [('FILE', FILE_STATUS_NEW_REMOTE, self.path, self.result_file)]
        if self.is_stub:
            return [('FILE', FILE_STATUS_IS_LOCAL_STUB, self.path, self.result_file)]
        if self.result_file and self.result_file.download_needs_update(self.path, check_flag=False):
            return [('FILE', FILE_STATUS_MODIFIED_REMOTE, self.path, self.result_file)]
        if self.locally_modified():
            return [('FILE', FILE_STATUS_MODIFIED_LOCAL, self.path, self.result_file)]
        return [('FILE', FILE_STATUS_ALL_OK, self.path, self.result_file)]
    
    def yield_child_objects(self):
        return []
    
    def list_abnormal_objects(self, abnormal_manager=None):
        """Return a list of files that have been modified.
        
        Since this class is a single file the list will either be empty
        or have one element.

        Note that if a file was modified locally then uploaded to the server
        the file will be marked as modified remote.
        """
        if abnormal_manager is None:
            abnormal_manager = AbnormalObjectListManager()
        abnormal_manager.add_object(self)
        return dedupe_modified_files(abnormal_manager.list_abnormal_objects())

    def add_self_to_download_manager(self, download_manager):
        download_manager.add_download(self.result_file, file_path=self.path, callback=self.write_info_file)

    @classmethod
    def local_basename(cls, result_file):
        """Return a suitable local basename for a result file.
        
        Name must be unique at creation time according to geoseeq constraints.
        """
        basename = result_file.get_stored_basename()
        return basename

    
class ResultFolderOnFilesystem(GeoSeeqObjectOnFilesystem):

    def __init__(self, result_folder, path, kind):
        self.result_folder = result_folder
        self.path = path
        self.kind = kind

    @property
    def info_filepath(self):
        return os.path.join(self.path, '.gs_result_folder')

    def download(self, use_stubs=False, exists_ok=False):
        if os.path.exists(self.info_filepath) and not exists_ok:
            raise ValueError('Result folder already exists at path: {}'.format(self.info_filepath))
        
        # Download the files in the result folder
        for result_file in self.result_folder.get_fields():
            result_file_local_path = os.path.join(self.path, ResultFileOnFilesystem.local_basename(result_file))
            os.makedirs(os.path.dirname(result_file_local_path), exist_ok=True)
            ResultFileOnFilesystem(result_file, result_file_local_path, self.kind)\
                .download(use_stubs=use_stubs, exists_ok=exists_ok)
        
        self.write_info_file()

    def write_info_file(self):
        result_folder_info = {
            "uuid": self.result_folder.uuid if self.result_folder else None,
            "kind": self.kind
        }
        with open(self.info_filepath, 'w') as f:
            json.dump(result_folder_info, f)

    def status_is_ok(self):
        # check for an info file
        if not os.path.exists(self.info_filepath):
            return False
        
        # check that all files are downloaded
        for result_file in self.result_folder.get_files():
            result_file_path = os.path.join(self.path, ResultFileOnFilesystem.local_basename(result_file))
            if not os.path.exists(result_file_path):
                return False
        
        return True
    
    @classmethod
    def from_path(cls, path, kind=None):
        obj = cls(None, path, kind)
        try:
            with open(os.path.join(path, '.gs_result_folder'), 'r') as f:
                result_folder_info = json.load(f)
            result_folder_uuid = result_folder_info['uuid']
            if result_folder_uuid:
                obj.result_folder = result_folder_from_id(result_folder_info['uuid'])
            obj.kind = result_folder_info['kind']
        except FileNotFoundError:
            pass
        return obj
    
    def atomic_abnormal_status(self):
        """Return a list with the abnormal status of this result folder or empty list.
        
        Do not return status of any child objects.
        """
        modified_files = []
        if not self.result_folder:
            if os.path.exists(self.info_filepath):
                modified_files.append(('FOLDER', FILE_STATUS_NEW_LOCAL, self.path, None))
            else:
                modified_files.append(('FOLDER', FILE_STATUS_NOT_ADDED, self.path, None))
        if not os.path.exists(self.path):
            modified_files.append(('FOLDER', FILE_STATUS_NEW_REMOTE, self.path, self.result_folder))
        return modified_files
    
    def yield_child_objects(self):
        # list remote files
        if self.result_folder:  # if the result folder exists on the server
            for result_file in self.result_folder.get_fields():
                result_file_path = os.path.join(self.path, ResultFileOnFilesystem.local_basename(result_file))
                result_file_on_fs = ResultFileOnFilesystem(result_file, result_file_path, self.kind)
                yield result_file_on_fs

        # list local files
        if os.path.exists(self.path):  # if the result folder exists locally
            for local_file in iterate_non_gs_files(self.path):
                local_file_path = os.path.join(self.path, local_file)
                if not os.path.isfile(local_file_path):
                    continue
                result_file_on_fs = ResultFileOnFilesystem.from_path(local_file_path)
                yield result_file_on_fs
    
    def list_abnormal_objects(self, abnormal_manager=None):
        """Return a list of files that have been modified.
        
        This function will return a list of tuples where the first element
        is the status of the file and the second element is the path to the file.
        """
        if abnormal_manager is None:
            abnormal_manager = AbnormalObjectListManager()

        for obj in self.yield_child_objects():
            abnormal_manager.add_object(obj)
    
        modified_files = abnormal_manager.list_abnormal_objects()
        return dedupe_modified_files(modified_files)
    
    @classmethod
    def local_basename(cls, result_folder):
        """Return a suitable local basename for a result folder.
        
        Name must be unique at creation time according to geoseeq constraints.
        """
        basename = result_folder.name + "__" + result_folder.replicate
        return basename
    

class SampleOnFilesystem(GeoSeeqObjectOnFilesystem):

    def __init__(self, sample, path):
        self.sample = sample
        self.path = path if path[-1] != '/' else path[:-1]  # remove trailing slash

    @property
    def info_filepath(self):
        return os.path.join(self.path, '.gs_sample')

    def download(self, use_stubs=False, exists_ok=False):
        if os.path.exists(self.info_filepath) and not exists_ok:
            raise ValueError('Sample already exists at path: {}'.format(self.info_filepath))
        
        # download result folders
        for result_folder in self.sample.get_result_folders():
            result_folder_local_path = os.path.join(self.path,
                                                    ResultFolderOnFilesystem.local_basename(result_folder))
            os.makedirs(result_folder_local_path, exist_ok=True)
            ResultFolderOnFilesystem(result_folder, result_folder_local_path, "sample")\
                .download(use_stubs=use_stubs, exists_ok=exists_ok)

        self.write_info_file()

    def write_info_file(self):
        sample_info = {
            "uuid": self.sample.uuid if self.sample else None,
        }
        with open(self.info_filepath, 'w') as f:
            json.dump(sample_info, f)

    def status_is_ok(self):
        # check for an info file
        if not os.path.exists(self.info_filepath):
            return False
        
        # check that all result folders are downloaded
        for result_folder in self.sample.get_result_folders():
            result_folder_local_path = os.path.join(self.path, result_folder.name)
            result_folder_on_fs = ResultFolderOnFilesystem.from_path(result_folder_local_path, "sample")
            if not result_folder_on_fs.status_is_ok():
                return False
        
        return True

    @classmethod
    def from_path(cls, path):
        obj = cls(None, path)
        try:
            with open(os.path.join(path, '.gs_sample'), 'r') as f:
                sample_info = json.load(f)
            sample_uuid = sample_info['uuid']
            if sample_uuid:
                obj.sample = sample_from_id(sample_info['uuid'])
        except FileNotFoundError:
            pass
        return obj
    
    def atomic_abnormal_status(self):
        """Return a list with the abnormal status of this sample or empty list.
        
        Do not return status of any child objects.
        """
        modified_files = []
        if not self.sample:
            if os.path.exists(self.info_filepath):
                modified_files.append(('SAMPLE', FILE_STATUS_NEW_LOCAL, self.path, None))
            else:
                modified_files.append(('SAMPLE', FILE_STATUS_NOT_ADDED, self.path, None))
        if not os.path.exists(self.path):
            modified_files.append(('SAMPLE', FILE_STATUS_NEW_REMOTE, self.path, self.sample))
        return modified_files
    
    def yield_child_objects(self):
        # list remote result folders
        for result_folder in self.sample.get_result_folders():
            result_folder_path = os.path.join(self.path, ResultFolderOnFilesystem.local_basename(result_folder))
            result_folder_on_fs = ResultFolderOnFilesystem(result_folder, result_folder_path, "sample")
            yield result_folder_on_fs

        # list local result folders
        for local_result_folder in iterate_non_gs_files(self.path):
            local_result_folder_path = os.path.join(self.path, local_result_folder)
            if not os.path.isdir(local_result_folder_path):
                continue
            result_folder_on_fs = ResultFolderOnFilesystem.from_path(local_result_folder_path, "sample")
            yield result_folder_on_fs
    
    def list_abnormal_objects(self, abnormal_manager=None):
        """Return a list of files that have been modified.
        
        This function will return a list of tuples where the first element
        is the status of the file and the second element is the path to the file.
        """
        if abnormal_manager is None:
            abnormal_manager = AbnormalObjectListManager()
    
        for obj in self.yield_child_objects():
            abnormal_manager.add_object(obj)

        modified_files = abnormal_manager.list_abnormal_objects()
        return dedupe_modified_files(modified_files)


class ProjectOnFilesystem(GeoSeeqObjectOnFilesystem):

    def __init__(self, project, path):
        self.project = project
        self.path = path

    @property
    def info_filepath(self):
        return os.path.join(self.path, '.gs_project')

    def download(self, use_stubs=False, exists_ok=False):
        if os.path.exists(self.info_filepath) and not exists_ok:
            raise ValueError('Project already exists at path: {}'.format(self.info_filepath))
        
        # download samples
        sample_dir_path = os.path.join(self.path, "sample_results")
        os.makedirs(sample_dir_path, exist_ok=True)
        for sample in self.project.get_samples():
            sample_local_path = os.path.join(sample_dir_path,
                                             SampleOnFilesystem.local_basename(sample))
            os.makedirs(sample_local_path, exist_ok=True)
            SampleOnFilesystem(sample, sample_local_path)\
                .download(use_stubs=use_stubs, exists_ok=exists_ok)
        
        # download project result folders
        project_result_dir_path = os.path.join(self.path, "project_results")
        os.makedirs(project_result_dir_path, exist_ok=True)
        for result_folder in self.project.get_result_folders():
            result_folder_local_path = os.path.join(project_result_dir_path,
                                                    ResultFolderOnFilesystem.local_basename(result_folder)) 
            os.makedirs(result_folder_local_path, exist_ok=True)
            ResultFolderOnFilesystem(result_folder, result_folder_local_path, "project")\
                .download(use_stubs=use_stubs, exists_ok=exists_ok)

        # Write the project data
        self.write_info_file()


    def write_info_file(self):
        project_info = {
            "uuid": self.project.uuid,
            "knex_info": {
                "endpoint_url": self.project.knex.endpoint_url,
                "profile": self.project.knex.profile if self.project.knex.profile else "",
            }
        }
        with open(self.info_filepath, 'w') as f:
            json.dump(project_info, f)

    def status_is_ok(self):
        # check for an info file
        if not os.path.exists(self.info_filepath):
            return False
        
        # check that all samples are downloaded
        for sample in self.project.get_samples():
            sample_local_path = os.path.join(self.path, "sample_results",
                                             SampleOnFilesystem.local_basename(sample))
            sample_on_fs = SampleOnFilesystem.from_path(sample_local_path)
            if not sample_on_fs.status_is_ok():
                return False
        
        # check that all project result folders are downloaded
        for result_folder in self.project.get_result_folders():
            result_folder_local_path = os.path.join(self.path, "project_results",
                                                    ResultFolderOnFilesystem.local_basename(result_folder))
            result_folder_on_fs = ResultFolderOnFilesystem.from_path(result_folder_local_path, "project")
            if not result_folder_on_fs.status_is_ok():
                return False
        
        return True

    @classmethod
    def from_path(cls, path, recursive=False):
        try:
            with open(os.path.join(path, '.gs_project'), 'r') as f:
                project_info = json.load(f)
                knex_info = project_info.get('knex_info', {})
            knex_profile = knex_info.get('profile', '')
            if knex_profile:
                knex = Knex.load_profile(knex_profile)
                project = project_from_id(knex, project_info['uuid'])
            else:
                project = project_from_id(project_info['uuid'])
            return cls(project, path)
        except FileNotFoundError:
            if not recursive:
                raise ValueError('No project found in path or parent directories')
            updir = os.path.dirname(os.path.abspath(path))
            if updir == path:
                raise ValueError('No project found in path or parent directories')
            return cls.from_path(updir, recursive=recursive)
        
    def path_from_project_root(self, path):
        if path[0] == "/":
            return path.replace(self.path, "")[1:]
        return path
    
    def atomic_abnormal_status(self):
        """Return a list with the abnormal status of this project or empty list.
        
        Do not return status of any child objects.
        """
        modified_files = []
        return modified_files
    
    def yield_child_objects(self):
        # list remote samples
        for sample in self.project.get_samples():
            sample_path = os.path.join(self.path, "sample_results",
                                       SampleOnFilesystem.local_basename(sample))
            sample_on_fs = SampleOnFilesystem(sample, sample_path)
            yield sample_on_fs

        # list remote project result folders
        for result_folder in self.project.get_result_folders():
            result_folder_path = os.path.join(self.path, "project_results",
                                              ResultFolderOnFilesystem.local_basename(result_folder))

            result_folder_on_fs = ResultFolderOnFilesystem(result_folder, result_folder_path, "project")
            yield result_folder_on_fs

        # list local samples
        for local_sample in iterate_non_gs_files(os.path.join(self.path, "sample_results")):
            local_sample_path = os.path.join(self.path, "sample_results", local_sample)
            if not os.path.isdir(local_sample_path):
                continue
            sample_on_fs = SampleOnFilesystem.from_path(local_sample_path)
            yield sample_on_fs
    
        # list local project result folders
        for local_result_folder in iterate_non_gs_files(os.path.join(self.path, "project_results")):
            local_result_folder_path = os.path.join(self.path, "project_results", local_result_folder)
            if not os.path.isdir(local_result_folder_path):
                continue
            result_folder_on_fs = ResultFolderOnFilesystem.from_path(local_result_folder_path)
            yield result_folder_on_fs


    def list_abnormal_objects(self, abnormal_manager=None):
        """Return a list of files that have been modified.
        
        This function will return a list of tuples where the first element
        is the status of the file and the second element is the path to the file.
        """
        if abnormal_manager is None:
            abnormal_manager = AbnormalObjectListManager()

        for obj in self.yield_child_objects():
            abnormal_manager.add_object(obj)

        modified_files = abnormal_manager.list_abnormal_objects()
        return dedupe_modified_files(modified_files)
    
    def add_path_to_project_locally(self, path):
        """Add a file to this geoseeq project locally.
        
        Create .gs files for the file itself and for the result dir.
        """
        out = []
        path = self.path_from_project_root(path)
        tkns = path.split('/')
        print(tkns)
        if tkns[0] == "project_results":
            folder_name, file_name = tkns[1], '/'.join(tkns[2:])
            local_result_folder_path = os.path.join(self.path, "project_results", folder_name)
            result_folder_on_fs = ResultFolderOnFilesystem.from_path(local_result_folder_path, "project")
            result_folder_on_fs.write_info_file()
            out.append(('FOLDER', FILE_STATUS_NEW_LOCAL, local_result_folder_path, None))
            local_file_path = os.path.join(local_result_folder_path, file_name)
            result_file_on_fs = ResultFileOnFilesystem.from_path(local_file_path, "project")
            result_file_on_fs.write_info_file()
            out.append(('FILE', FILE_STATUS_NEW_LOCAL, local_file_path, None))
        elif tkns[0] == "sample_results":
            sample_name, folder_name, file_name = tkns[1], tkns[2], '/'.join(tkns[3:])
            local_sample_path = os.path.join(self.path, "sample_results", sample_name)
            sample_on_fs = SampleOnFilesystem.from_path(local_sample_path)
            sample_on_fs.write_info_file()
            out.append(('SAMPLE', FILE_STATUS_NEW_LOCAL, local_sample_path, None))
            local_result_folder_path = os.path.join(local_sample_path, folder_name)
            result_folder_on_fs = ResultFolderOnFilesystem.from_path(local_result_folder_path, "sample")
            result_folder_on_fs.write_info_file()
            out.append(('FOLDER', FILE_STATUS_NEW_LOCAL, local_result_folder_path, None))
            local_file_path = os.path.join(local_result_folder_path, file_name)
            result_file_on_fs = ResultFileOnFilesystem.from_path(local_file_path, "project")
            result_file_on_fs.write_info_file()
            out.append(('FILE', FILE_STATUS_NEW_LOCAL, local_file_path, None))
        return out

        