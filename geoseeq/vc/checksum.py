from geoseeq.utils import md5_checksum

class Checksum:

    def __init__(self, value, method):
        self.value = value
        self.method = method.lower()
    
    def verify(self, path):
        """Return True iff the checksum for path matches the stored checksum."""
        if self.method == 'none':
            return False
        if self.method == 'md5':
            return md5_checksum(path) == self.value
        raise NotImplementedError(f'Mehtod "{self.method}" not supported')

    def to_blob(self):
        return {
            "value": self.value,
            "method": self.method,
        }

    @classmethod
    def from_blob(cls, blob):
        return cls(blob['value'], blob['method'])
