
import json
from .vc_stub import VCStub


class VCDir:
    """A class to represent a directory with GeoSeeq Version Control files."""

    def __init__(self, root_path, extension='.gvcf'):
        self.path = root_path
        self.ext = extension

    def stubs(self):
        """Return an iterator over stub files."""
        for path in glob(f'{self.path}/*{self.ext}') + glob(f'{self.path}/**/*{self.ext}'):
            yield VCStub.from_path(path)
