import logging
from pandas import to_datetime

from requests.exceptions import HTTPError

from .file_system_cache import FileSystemCache

logger = logging.getLogger("geoseeq_api")  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program


class RemoteObjectError(Exception):
    pass


class RemoteObjectOverwriteError(RemoteObjectError):
    pass


class RemoteObject:
    optional_remote_fields = []

    def __init__(self, *args, **kwargs):
        self._already_fetched = False
        self._modified = False
        self._deleted = False
        self.blob = None
        self.uuid = None
        self.cache = FileSystemCache()
        self.url_options = {}

    def __setattr__(self, key, val):
        if hasattr(self, "deleted") and self._deleted:
            logger.error(f"Attribute cannot be set, RemoteObject has been deleted. {self}")
            raise RemoteObjectError("This object has been deleted.")
        super(RemoteObject, self).__setattr__(key, val)
        if key in self.remote_fields or key == self.parent_field:
            logger.debug(f'Setting RemoteObject modified. key "{key}"')
            super(RemoteObject, self).__setattr__("_modified", True)

    @property
    def inherited_url_options(self):
        opts = self.url_options.copy()
        if self.parent_field:
            parent = getattr(self, self.parent_field)
            opts.update(parent.inherited_url_options)
        return opts
    
    def get_remote_fields(self):
        for key in self.remote_fields:
            yield key, getattr(self, key), key in self.optional_remote_fields

    def invalidate_cache(self):
        self.cache.clear_blob(self)

    def get_cached_blob(self):
        return self.cache.get_cached_blob(self)

    def cache_blob(self, blob):
        return self.cache.cache_blob(self, blob)
    
    @property
    def updated_at_timestamp(self):
        """Return the updated_at field as a unix timestamp (in seconds).
        
        timestamp from servercomes as a string: '2024-03-13T09:06:56.582551Z' 
        """
        return to_datetime(self.updated_at).timestamp()

    def load_blob(self, blob, allow_overwrite=False):
        logger.debug(f"Loading blob. {blob}")
        if self._deleted:
            logger.error(f"Cannot load blob, RemoteObject has been deleted. {self}")
            raise RemoteObjectError("This object has been deleted.")
        for field in self.remote_fields:
            current = getattr(self, field, None)
            try:
                new = blob[field]
            except KeyError:
                if field not in self.optional_remote_fields:
                    logger.error(f"Blob being loaded is missing key. {field}")
                    raise KeyError(
                        f"Key {field} is missing for object {self} (type {type(self)})\
                             in blob: {blob}"
                    )
                new = None
            if not allow_overwrite and current and current != new:
                is_overwrite = True
                if isinstance(current, dict) and isinstance(new, dict):
                    append_only = True
                    for k, v in current.items():
                        if (k not in new) or (new[k] != v):
                            append_only = False
                        break
                    if append_only:
                        is_overwrite = False
                if is_overwrite:
                    logger.error(f"Loading blob would overwrite key. {field}")
                    raise RemoteObjectOverwriteError(
                        (
                            f'Loading blob would overwrite field "{field}":\n\t'
                            f'current: "{current}" (type: "{type(current)}")\n\t'
                            f'new:     "{new}" (type: "{type(new)}")'
                        )
                    )
            setattr(self, field, new)

    def get_blob(self):
        blob = {}
        for key, val, optional in self.get_remote_fields():
            if val is not None or not optional:
                blob[key] = val
        return blob

    def exists(self, allow_overwrite=False):
        try:
            self.get(allow_overwrite=allow_overwrite)
            return True
        except HTTPError:
            return False

    def get(self, allow_overwrite=False):
        """Fetch the object from the server."""
        if self._deleted:
            logger.error(f"Cannot GET blob, RemoteObject has been deleted. {self}")
            raise RemoteObjectError("This object has been deleted.")
        if not self._already_fetched:
            logger.debug(f"Fetching RemoteBlob. {self}")
            self._get(allow_overwrite=allow_overwrite)
            self._already_fetched = True
            self._modified = False
        else:
            logger.debug(f"RemoteObject has already been fetched. {self}")
        return self

    def create(self):
        """Create this object on the server."""
        if self._deleted:
            logger.error(f"Cannot create blob, RemoteObject has been deleted. {self}")
            raise RemoteObjectError("This object has been deleted.")
        if not self._already_fetched:
            logger.debug(f"Creating RemoteBlob. {self}")
            self.cache.clear_blob(self)
            self._create()
            self._already_fetched = True
            self._modified = False
        else:
            logger.debug(f"RemoteObject has already been fetched. {self}")
        return self

    def save(self):
        """Assuming the object exists on the server make the server-side object
        match the state of this object.
        """
        if self._deleted:
            logger.error(f"Cannot save blob, RemoteObject has been deleted. {self}")
            raise RemoteObjectError("This object has been deleted.")
        if not self._already_fetched:
            msg = "Attempting to SAVE an object which has not been fetched is disallowed."
            raise RemoteObjectError(msg)
        if self._modified:
            logger.debug(f"Saving RemoteBlob. {self}")
            self.cache.clear_blob(self)
            self._save()
            self._modified = False
        else:
            logger.debug(f"RemoteBlob has not been modified. Nothing to save. {self}")
        return self

    def idem(self):
        """Make the state of this object match the server."""
        if self._deleted:
            raise RemoteObjectError("This object has been deleted.")
        if not self._already_fetched:
            try:
                self.get()
            except HTTPError:
                self.create()
        else:
            self.save()
        return self

    def delete(self):
        logger.debug(f"Deleting RemoteBlob. {self}")
        self.knex.delete(self.nested_url())
        self._already_fetched = False
        self._deleted = True

    @classmethod
    def all_uuids(self, knex):
        """Return a list of all objects of this type."""
        results = knex.get(self.url_prefix)['results']
        return [result['uuid'] for result in results]



