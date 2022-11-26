
import json
from .checksum import Checksum


class VCStub:
    """A version control stub."""

    def __init__(self, brn, local_path, checksum):
        self.brn = brn
        self.local_path = local_path
        self.checksum = checksum

    def download(self, knex):
        """Download the linked file to local storage."""
        pass

    def verify(self):
        """Return True iff the local file matches the linked checksum."""
        return self.checksum.verify(self.local_path)

    def remote_verify(self, knex):
        """Return True iff the stub file matches the info on the remote."""
        pass

    @classmethod
    def from_blob(self, blob):
        return cls(
            blob['brn'],
            blob['local_path'],
            Checksum.from_blob(blob['checksum']),
        )
    
    @classmethod
    def from_path(self, path)
        blob = json.loads(open(path).read())
        return VCStub.from_blob(blob)
