
import json
from os.path import join, dirname
from .checksum import Checksum
from os import environ, symlink, makedirs
from .vc_cache import VCCache


class VCStub:
    """A version control stub."""

    def __init__(self, brn, local_path, checksum, stub_path=None):
        self.brn = brn
        self._local_path = local_path
        self.checksum = checksum if isinstance(checksum, Checksum) else Checksum.from_blob(checksum)
        self.stub_path = stub_path
        self.parent_info = {}

    def download(self, knex):
        """Download the linked file to local storage."""
        _, field = self.field(knex)
        cache_dir = environ.get('GEOSEEQ_VC_CACHE_DIR', None)  # TODO diff caches for diff projects
        if cache_dir:
            cache = VCCache(cache_dir)
            cache_path = cache.get_cache_filepath(field)
            makedirs(dirname(cache_path), exist_ok=True)
            field.download_file(cache_path)
            symlink(cache_path, self.local_path)
        else:
            field.download_file(self.local_path)

    def verify(self):
        """Return True iff the local file matches the linked checksum."""
        return self.checksum.verify(self.local_path)

    def remote_verify(self, knex):
        """Return True iff the stub file matches the info on the remote."""
        pass

    def field(self, knex):
        """Return the AnalysisResultField that this stub points to on the remote."""
        return resolve_brn(knex, self.brn)
    
    @property
    def local_path(self):
        """Return the path on the local machine that this stub points to."""
        return join(dirname(self.stub_path), self._local_path)

    def set_parent_info(self, parent_obj_brn, result_module_name, result_replicate):
        self.parent_info = {
            "parent_obj_brn": parent_obj_brn,
            "result_module_name": result_module_name,
            "result_module_replicate": result_replicate,
        }

    def save_to_file(self, stub_path=None):
        assert stub_path or self.stub_path
        stub_path = stub_path if stub_path else self.stub_path
        makedirs(dirname(stub_path), exist_ok=True)
        with open(stub_path, 'w') as f_out:
            f_out.write(json.dumps(self.to_blob(), indent=4))

    def to_blob(self, parent_obj_brn=None, result_module_name=None, result_replicate=None):
        assert self.parent_info or self.brn
        blob = {
            "checksum": self.checksum.to_blob(),
            "local_path": self._local_path,
        }
        if self.brn:
            blob['brn'] = self.brn
        if self.parent_info:
            blob['parent_info'] = self.parent_info
        return blob

    @classmethod
    def from_blob(cls, blob, stub_path=None):
        assert 'brn' in blob or 'parent_info' in blob
        return cls(
            blob['brn'],
            blob['local_path'],
            Checksum.from_blob(blob['checksum']),
            stub_path=stub_path,
        )
    
    @classmethod
    def from_path(self, path):
        blob = json.loads(open(path).read())
        return VCStub.from_blob(blob, stub_path=path)
