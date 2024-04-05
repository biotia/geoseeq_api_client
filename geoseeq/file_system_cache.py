import json
import logging
import os
from os.path import join, abspath
from glob import glob
from hashlib import sha256
from random import randint
from time import time

logger = logging.getLogger("geoseeq_api")  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program
CACHED_BLOB_TIME = 5 * 60 # 5 minutes in seconds
CACHE_DIR = join(
    os.environ.get('XDG_CACHE_HOME', join(os.environ["HOME"], ".cache")),
    "geoseeq"
)
USE_GEOSEEQ_CACHE = None
GEOSEEQ_CACHE_DIR = abspath(f'{CACHE_DIR}/geoseeq_api_cache/v1/')

def hash_obj(obj):
    val = obj
    if not isinstance(obj, str):
        val = obj.pre_hash()
    result = sha256(val.encode())
    result = result.hexdigest()
    return result


def time_since_file_cached(blob_filepath):
    timestamp = int(blob_filepath.split('__')[-1].split('.json')[0])
    elapsed_time = int(time()) - timestamp
    return elapsed_time


class FileSystemCache:

    def __init__(self, timeout=CACHED_BLOB_TIME):
        self.timeout = timeout
        self._no_cache = False
        self.setup()

    @property
    def cache_dir_path(self):
        return GEOSEEQ_CACHE_DIR

    def setup(self):
        if self.no_cache:
            return
        try:
            os.makedirs(self.cache_dir_path, exist_ok=True)
            open(join(self.cache_dir_path, 'flag'), 'w').close()
        except Exception as e:
            logger.warning(f'Could not create cache directory. {e}')
            self._no_cache = True
    
    @property
    def no_cache(self):
        if self._no_cache or not USE_GEOSEEQ_CACHE:
            logger.debug('Cache is disabled.')
            return True
        logger.debug('Cache is enabled.')
        return not USE_GEOSEEQ_CACHE

    def clear_blob(self, obj):
        if self.no_cache:
            return
        blob_filepath, path_exists = self.get_cached_blob_filepath(obj)
        if path_exists:
            logger.debug(f'Clearing cached blob. {blob_filepath}')
            try:
                os.remove(blob_filepath)
            except FileNotFoundError:
                logger.debug(f'Blob was deleted before it could be removed. {obj}')
                pass

    def get_cached_blob_filepath(self, obj):
        path_base = join(self.cache_dir_path, f'geoseeq_api_cache__{hash_obj(obj)}')
        paths = sorted(glob(f'{path_base}__*.json'))
        if paths:
            return paths[-1], True
        timestamp = int(time())
        blob_filepath = f'{path_base}__{timestamp}.json'
        return blob_filepath, False

    def get_cached_blob(self, obj):
        if self.no_cache:
            return None
        logger.debug(f'Getting cached blob. {obj}')
        blob_filepath, path_exists = self.get_cached_blob_filepath(obj)
        if not path_exists:  # cache not found
            logger.debug(f'No cached blob found. {obj}')
            return None
        elapsed_time = time_since_file_cached(blob_filepath)
        if elapsed_time > (self.timeout + randint(0, self.timeout // 10)):  # cache is stale
            logger.debug(f'Found stale cached blob. {obj}')
            os.remove(blob_filepath)
            return None
        logger.debug(f'Found good cached blob. {obj}')
        try:
            blob = json.loads(open(blob_filepath).read())
            return blob
        except FileNotFoundError:
            logger.debug(f'Blob was deleted before it could be returned. {obj}')
            return None

    def cache_blob(self, obj, blob):
        if self.no_cache:
            return None
        logger.debug(f'Caching blob. {obj} {blob}')
        blob_filepath, path_exists = self.get_cached_blob_filepath(obj)
        if path_exists:  # save a new cache if an old one exists
            elapsed_time = time_since_file_cached(blob_filepath)
            if elapsed_time < ((self.timeout / 2) + randint(0, self.timeout // 10)):
                # Only reload a file if it is old enough
                return
            self.clear_blob(obj)
            return self.cache_blob(obj, blob)
        with open(blob_filepath, 'w') as f:
            f.write(json.dumps(blob))
