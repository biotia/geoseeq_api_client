import hashlib
import os
import logging
from ftplib import FTP
from threading import Timer
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



def download_ftp(url, local_file_path, head=None):
    tkns = url.split('ftp://')[1].split('/')
    host, folder_path, file_name = tkns[0], '/'.join(tkns[1:-1]), tkns[-1]
    ftp = FTP(host)
    ftp.login()
    ftp.cwd(folder_path)
    logger.debug(f'Logged into {host} and changed to {folder_path}')

    def handle_download(file_handle, block):
        logger.debug(f'Writing block of size {len(block)} to {local_file_path}')
        file_handle.write(block)
        if head and os.path.getsize(local_file_path) >= head:
            logger.debug(f'File {local_file_path} has reached head size of {head} bytes. Aborting download.')
            ftp.sock.close()
            with open(local_file_path, 'rb+') as f:
                f.seek(head)
                f.truncate()
            assert False  # this is the best way to abort a download
        
    with open(local_file_path, 'wb') as f:
        try:
            ftp.retrbinary('RETR ' + file_name, lambda block: handle_download(f, block))
        except AssertionError:
            pass
    ftp.close()

    # trim local file to head size
    if head:
        with open(local_file_path, 'rb+') as f:
            f.seek(head)
            f.truncate()
