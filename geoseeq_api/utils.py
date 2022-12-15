import hashlib
import logging

from .file_system_cache import FileSystemCache

logger = logging.getLogger('geoseeq_api')  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program


def paginated_iterator(knex, initial_url, error_handler=None):
    cache = FileSystemCache()
    result = cache.get_cached_blob(initial_url)
    if not result:
        try:
            result = knex.get(initial_url)
        except Exception as e:
            logger.debug(f'Error fetching blob:\n\t{initial_url}\n\t{e}')
            if error_handler:
                error_handler(e)
            else:
                raise
        cache.cache_blob(initial_url, result)
    for blob in result['results']:
        yield blob
    next_page = result.get('next', None)
    if next_page:
        for blob in paginated_iterator(knex, next_page):
            yield blob


def md5_checksum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
