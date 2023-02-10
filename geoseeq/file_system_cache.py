import json
import logging
import os
from glob import glob
from hashlib import sha256
from random import randint
from time import time

logger = logging.getLogger(__name__)  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program
CACHED_BLOB_TIME = 3 * 60 * 60  # 3 hours in seconds
CACHE_DIR = os.environ.get('GEOSEEQ_API_CACHE_DIR', '.')


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
        self.no_cache = 'false' in os.environ.get('USE_GEOSEEQ_CACHE', 'TRUE').lower()
        self.timeout = timeout

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
        path_base = f'{CACHE_DIR}/.geoseeq_api_cache/v1/geoseeq_api_cache__{hash_obj(obj)}'
        os.makedirs(os.path.dirname(path_base), exist_ok=True)
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
