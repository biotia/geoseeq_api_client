from fuse import FUSE, Operations
import os


class GeoSeeqProjectFileSystem(Operations):
    """Mount a GeoSeeq project as a filesystem.
    
    The project will automatically have this directory structure:
    - <root>/project_results/<project_result_folder_name>/...
    - <root>/sample_results/<sample_name>/...
    - <root>/metadata/sample_metadata.csv
    - <root>/.config/config.json
    """

    def __init__(self, root, project):
        self.root = root
        self.project = project

    def access(self, path, mode):
        pass

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def getattr(self, path, fh=None):
        pass

    def readdir(self, path, fh):
        pass

    def readlink(self, path):
        pass

    def mknod(self, path, mode, dev):
        pass

    def rmdir(self, path):
        pass

    def mkdir(self, path, mode):
        pass

    def statfs(self, path):
        pass

    def unlink(self, path):
        pass

    def symlink(self, name, target):
        pass

    def rename(self, old, new):
        pass

    def link(self, target, name):
        pass

    def utimens(self, path, times=None):
        pass

    def open(self, path, flags):
        tkns = path.split('/')
        if tkns[0] == 'project_results':
            result_folder_name, result_file_name = tkns[2], '/'.join(tkns[3:])
            result_folder = self.project.get_result_folder(result_folder_name).get()
            result_file = result_folder.get_file(result_file_name).get()
            result_file.download(path)
        elif tkns[0] == 'sample_results':
            sample_name, result_folder_name, result_file_name = tkns[2], tkns[3], '/'.join(tkns[4:])
            sample = self.project.get_sample(sample_name).get()
            result_folder = sample.get_result_folder(result_folder_name).get()
            result_file = result_folder.get_file(result_file_name).get()
            result_file.download(path)
        elif tkns[0] == 'metadata':
            raise NotImplementedError('TODO')
        
        return os.open(self._full_local_path(path), flags)

    def create(self, path, mode, fi=None):
        tkns = path.split('/')
        if tkns[0] == 'project_results':
            result_name, file_name = tkns[2], '/'.join(tkns[3:])
            result_folder = self.project.get_result_folder(result_name).idem()
            result_file = result_folder.get_file(file_name).create()
            result_file.download(path)  # nothing to download at this point
        elif tkns[0] == 'sample_results':
            sample_name, result_folder_name, result_file_name = tkns[2], tkns[3], '/'.join(tkns[4:])
            sample = self.project.get_sample(sample_name).idem()
            result_folder = sample.get_result_folder(result_folder_name).idem()
            result_file = result_folder.get_file(result_file_name).create()
            result_file.download(path)  # nothing to download at this point
        elif tkns[0] == 'metadata':
            raise NotImplementedError('TODO')

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        pass

    def truncate(self, path, length, fh=None):
        pass

    def flush(self, path, fh):
        pass

    def release(self, path, fh):
        pass

    def fsync(self, path, fdatasync, fh):
        pass
    
    def _full_local_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        return os.path.join(self.root, partial)

    
