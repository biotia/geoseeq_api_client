

class Checksum:

    def __init__(self, value, method):
        self.value = value
        self.method = method
    
    def verify(self, path):
        """Return True iff the checksum for path matches the stored checksum."""
        pass

    @classmethod
    def from_blob(cls, blob):
        return cls(blob['value'], blob['method'])
