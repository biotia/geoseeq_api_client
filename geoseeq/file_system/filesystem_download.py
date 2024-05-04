
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

FILE_STATUS_MODIFIED_REMOTE = 'MODIFIED_REMOTE'
FILE_STATUS_MODIFIED_LOCAL = 'MODIFIED_LOCAL'
FILE_STATUS_NEW_LOCAL = 'NEW_LOCAL'
FILE_STATUS_NEW_REMOTE = 'NEW_REMOTE'
FILE_STATUS_IS_LOCAL_STUB = 'IS_LOCAL_STUB'


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


class ResultFileOnFilesystem:
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
        raise NotImplementedError('This function is not implemented')

    def status_is_ok(self, stubs_are_ok=False):
        # check for an info file
        if not os.path.exists(self.info_filepath):
            return False
        if stubs_are_ok:
            return True
        return not self.result_file.download_needs_update(self.path)
    
    def write_info_file(self):
        result_file_info = {
            "uuid": self.result_file.uuid,
            "kind": self.kind,
            "checksum": self.local_file_checksum(),
        }
        with open(self.info_filepath, 'w') as f:
            json.dump(result_file_info, f)

    @classmethod
    def from_path(cls, path):
        obj = cls(None, path, None)
        try:
            with open(obj.info_filepath, 'r') as f:
                result_file_info = json.load(f)
            obj.result_file = result_file_from_id(result_file_info['uuid'])
            obj.kind = result_file_info['kind']
            obj.stored_checksum = result_file_info['checksum']
        except FileNotFoundError:
            pass
        return obj
    
    def write_info_file(self):
        result_file_info = {
            "uuid": self.result_file.uuid,
            "kind": self.kind,
            "checksum": self.local_file_checksum(),
        }
        with open(self.info_filepath, 'w') as f:
            json.dump(result_file_info, f)
    
    def list_abnormal_objects(self):
        """Return a list of files that have been modified.
        
        Since this class is a single file the list will either be empty
        or have one element.

        Note that if a file was modified locally then uploaded to the server
        the file will be marked as modified remote.
        """
        if self.result_file is None:
            return [('FILE', FILE_STATUS_NEW_LOCAL, self.path, None)]
        if not os.path.exists(self.path):
            return [('FILE', FILE_STATUS_NEW_REMOTE, self.path, self.result_file)]
        if self.is_stub:
            return [('FILE', FILE_STATUS_IS_LOCAL_STUB, self.path, self.result_file)]
        if self.result_file and self.result_file.download_needs_update(self.path):
            return [('FILE', FILE_STATUS_MODIFIED_REMOTE, self.path, self.result_file)]
        if self.locally_modified():
            return [('FILE', FILE_STATUS_MODIFIED_LOCAL, self.path, self.result_file)]

        return []

    
class ResultFolderOnFilesystem:

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
            result_file_local_path = os.path.join(self.path, result_file.name)
            os.makedirs(os.path.dirname(result_file_local_path), exist_ok=True)
            ResultFileOnFilesystem(result_file, result_file_local_path, self.kind)\
                .download(use_stubs=use_stubs, exists_ok=exists_ok)
        
        # Write the result folder data
        result_folder_info = {
            "uuid": self.result_folder.uuid,
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
            result_file_path = os.path.join(self.path, result_file.name)
            if not os.path.exists(result_file_path):
                return False
        
        return True
    
    @classmethod
    def from_path(cls, path):
        obj = cls(None, path, None)
        try:
            with open(os.path.join(path, '.gs_result_folder'), 'r') as f:
                result_folder_info = json.load(f)
            obj.result_folder = result_folder_from_id(result_folder_info['uuid'])
            obj.kind = result_folder_info['kind']
        except FileNotFoundError:
            pass
        return obj
    
    def list_abnormal_objects(self):
        """Return a list of files that have been modified.
        
        This function will return a list of tuples where the first element
        is the status of the file and the second element is the path to the file.
        """
        modified_files = []
        if not self.result_folder:
            modified_files.append(('FOLDER', FILE_STATUS_NEW_LOCAL, self.path, None))
        if not os.path.exists(self.path):
            modified_files.append(('FOLDER', FILE_STATUS_NEW_REMOTE, self.path, self.result_folder))

        # list local files
        if os.path.exists(self.path):
            for local_file in os.listdir(self.path):
                if local_file.startswith('.gs_'):
                    continue
                local_file_path = os.path.join(self.path, local_file)
                result_file_on_fs = ResultFileOnFilesystem.from_path(local_file_path)
                modified_files.extend(result_file_on_fs.list_abnormal_objects())

        # list remote files
        if self.result_folder:
            for result_file in self.result_folder.get_fields():
                result_file_path = os.path.join(self.path, result_file.name)
                result_file_on_fs = ResultFileOnFilesystem(result_file, result_file_path, self.kind)
                modified_files.extend(result_file_on_fs.list_abnormal_objects())
    
        return dedupe_modified_files(modified_files)
    

class SampleOnFilesystem:

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
            result_folder_local_path = os.path.join(self.path, result_folder.name)
            os.makedirs(result_folder_local_path, exist_ok=True)
            ResultFolderOnFilesystem(result_folder, result_folder_local_path, "sample")\
                .download(use_stubs=use_stubs, exists_ok=exists_ok)

        # Write the sample data
        sample_info = {
            "uuid": self.sample.uuid
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
            obj.sample = sample_from_id(sample_info['uuid'])
        except FileNotFoundError:
            pass
        return obj
    
    def list_abnormal_objects(self):
        """Return a list of files that have been modified.
        
        This function will return a list of tuples where the first element
        is the status of the file and the second element is the path to the file.
        """
        modified_files = []
        if not self.sample:
            modified_files.append(('SAMPLE', FILE_STATUS_NEW_LOCAL, self.path, None))
        if not os.path.exists(self.path):
            modified_files.append(('SAMPLE', FILE_STATUS_NEW_REMOTE, self.path, self.sample))

        # list local folders
        if os.path.exists(self.path):
            for local_folder in os.listdir(self.path):
                local_folder_path = os.path.join(self.path, local_folder)
                if not os.path.isdir(local_folder_path):
                    continue
                result_folder_on_fs = ResultFolderOnFilesystem.from_path(local_folder_path)
                modified_files.extend(result_folder_on_fs.list_abnormal_objects())

        # list remote folders
        if self.sample:
            for result_folder in self.sample.get_result_folders():
                result_folder_path = os.path.join(self.path, result_folder.name)
                result_folder_on_fs = ResultFolderOnFilesystem(result_folder, result_folder_path, "sample")
                modified_files.extend(result_folder_on_fs.list_abnormal_objects())

        return dedupe_modified_files(modified_files)


class ProjectOnFilesystem:

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
        for sample in self.project.get_samples():
            sample_local_path = os.path.join(self.path, "sample_results", sample.name)
            os.makedirs(sample_local_path, exist_ok=True)
            SampleOnFilesystem(sample, sample_local_path)\
                .download(use_stubs=use_stubs, exists_ok=exists_ok)
        
        # download project result folders
        for result_folder in self.project.get_result_folders():
            result_folder_local_path = os.path.join(self.path, "project_results", result_folder.name)
            os.makedirs(result_folder_local_path, exist_ok=True)
            ResultFolderOnFilesystem(result_folder, result_folder_local_path, "project")\
                .download(use_stubs=use_stubs, exists_ok=exists_ok)

        # Write the project data
        project_info = {
            "uuid": self.project.uuid
        }
        with open(self.info_filepath, 'w') as f:
            json.dump(project_info, f)

    def status_is_ok(self):
        # check for an info file
        if not os.path.exists(self.info_filepath):
            return False
        
        # check that all samples are downloaded
        for sample in self.project.get_samples():
            sample_local_path = os.path.join(self.path, "sample_results", sample.name)
            sample_on_fs = SampleOnFilesystem.from_path(sample_local_path)
            if not sample_on_fs.status_is_ok():
                return False
        
        # check that all project result folders are downloaded
        for result_folder in self.project.get_result_folders():
            result_folder_local_path = os.path.join(self.path, "project_results", result_folder.name)
            result_folder_on_fs = ResultFolderOnFilesystem.from_path(result_folder_local_path, "project")
            if not result_folder_on_fs.status_is_ok():
                return False
        
        return True

    @classmethod
    def from_path(cls, path, recursive=False):
        try:
            with open(os.path.join(path, '.gs_project'), 'r') as f:
                project_info = json.load(f)
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

    def list_abnormal_objects(self):
        """Return a list of files that have been modified.
        
        This function will return a list of tuples where the first element
        is the status of the file and the second element is the path to the file.
        """
        modified_files = []

        # list remote samples
        for sample in self.project.get_samples():
            sample_path = os.path.join(self.path, "sample_results", sample.name)
            sample_on_fs = SampleOnFilesystem(sample, sample_path)
            modified_files.extend(sample_on_fs.list_abnormal_objects())

        # list remote project result folders
        for result_folder in self.project.get_result_folders():
            result_folder_path = os.path.join(self.path, "project_results", result_folder.name)

            result_folder_on_fs = ResultFolderOnFilesystem(result_folder, result_folder_path, "project")
            modified_files.extend(result_folder_on_fs.list_abnormal_objects())

        # list local samples
        for local_sample in os.listdir(os.path.join(self.path, "sample_results")):
            local_sample_path = os.path.join(self.path, "sample_results", local_sample)
            if not os.path.isdir(local_sample_path):
                continue
            sample_on_fs = SampleOnFilesystem.from_path(local_sample_path)
            modified_files.extend(sample_on_fs.list_abnormal_objects())

        # list local project result folders
        for local_result_folder in os.listdir(os.path.join(self.path, "project_results")):
            local_result_folder_path = os.path.join(self.path, "project_results", local_result_folder)
            if not os.path.isdir(local_result_folder_path):
                continue
            result_folder_on_fs = ResultFolderOnFilesystem.from_path(local_result_folder_path)
            modified_files.extend(result_folder_on_fs.list_abnormal_objects())
        return dedupe_modified_files(modified_files)
                


