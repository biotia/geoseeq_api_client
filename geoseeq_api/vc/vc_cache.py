from os.path import join


class VCCache:
    """A filesystem cache so that multiple users
    on the same machine can avoid excess downloads and storage.
    """

    def __init__(self, root_path):
        self.root = root_path

    def get_cache_filepath(self, field):
        """Return a filepath in the cache to be used for `field`.

         - `field ` must exist on the remote
         - Does not matter if the filepath exists or not.
        """
        field = field.get()
        checksum_blob = field.checksum()
        base = f'{field.uuid}.{checksum_blob["method"]}__{checksum_blob["value"]}'
        path = join(self.root, field.uuid[-2:], base)  # -2 in case uuids have a timestamp prefix
        return path
    


