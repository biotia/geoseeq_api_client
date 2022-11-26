
import json
from geoseeq_api.blob_constructors import resolve_brn
from os.path import join, dirname
from .checksum import Checksum


class VCStub:
    """A version control stub."""

    def __init__(self, brn, local_path, checksum, stub_path=None):
        self.brn = brn
        self._local_path = local_path
        self.checksum = checksum
        self.stub_path = stub_path

    def download(self, knex):
        """Download the linked file to local storage."""
        pass

    def verify(self):
        """Return True iff the local file matches the linked checksum."""
        return self.checksum.verify(self.local_path)

    def remote_verify(self, knex):
        """Return True iff the stub file matches the info on the remote."""
        pass

    def field(self, knex):
        return resolve_brn(knex, self.brn)
    
    @property
    def local_path(self):
        return join(dirname(self.stub_path), self._local_path)

    @classmethod
    def from_blob(cls, blob, stub_path=None):
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
