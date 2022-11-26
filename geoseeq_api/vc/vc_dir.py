
import json
from .vc_stub import VCStub
from glob import glob


class VCDir:
    """A class to represent a directory with GeoSeeq Version Control files."""

    def __init__(self, root_path, extension='.gvcf'):
        self.path = root_path
        self.ext = extension

    def stubs(self):
        """Return an iterator over stub files."""
        for path in glob(f'{self.path}/**/*{self.ext}', recursive=True):
            yield VCStub.from_path(path)
