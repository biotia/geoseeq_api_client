
import urllib.request
import logging
import requests
import os
from os.path import basename, getsize, join, isfile, getmtime, dirname
from pathlib import Path
from tempfile import NamedTemporaryFile
from math import ceil

from geoseeq.utils import download_ftp
from geoseeq.constants import FIVE_MB
from hashlib import md5
from .resumable_download_tracker import ResumableDownloadTracker

logger = logging.getLogger("geoseeq_api")  # Same name as calling module


def url_to_id(url):
    url = url.split("?")[0]
    return md5(url.encode()).hexdigest()[:16]


def _download_head(url, filename, head=None, start=0, progress_tracker=None):
    headers = None
    if head and head > 0:
        headers = {"Range": f"bytes={start}-{head}"}
    response = requests.get(url, stream=True, headers=headers)
    response.raise_for_status()
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    if progress_tracker: progress_tracker.set_num_chunks(total_size_in_bytes)
    if total_size_in_bytes > 10 * FIVE_MB:  # Use resumable download
        print("Using resumable download")
        return _download_resumable(response, filename, total_size_in_bytes, progress_tracker)
    else:
        block_size = FIVE_MB
        with open(filename, 'wb') as file:
            for data in response.iter_content(block_size):
                if progress_tracker: progress_tracker.update(len(data))
                file.write(data)
        return filename
    

def _download_resumable(response, filename, total_size_in_bytes, progress_tracker=None, chunk_size=5 * FIVE_MB, part_prefix=".gs_download_{}_{}."):
    target_id = url_to_id(response.url)
    tracker = ResumableDownloadTracker(chunk_size, target_id, filename)
    if not tracker.download_started: tracker.start_download(response.url)
    n_chunks = ceil(total_size_in_bytes / chunk_size)
    for i in range(n_chunks):
        bytes_start, bytes_end = i * chunk_size, min((i + 1) * chunk_size - 1, total_size_in_bytes - 1)
        if tracker.part_has_been_downloaded(i):
            logger.debug(f"Part {i} has already been downloaded.")
        else:
            logger.debug(f"Downloading part {i} of {n_chunks - 1}")
            part_filename = join(dirname(filename), part_prefix.format(i, n_chunks - 1) + basename(filename))
            _download_head(response.url, part_filename, head=bytes_end, start=bytes_start, progress_tracker=None)
            part_info = dict(part_number=i, start=bytes_start, end=bytes_end, part_filename=part_filename)
            tracker.add_part(part_info)
        if progress_tracker: progress_tracker.update(bytes_end - bytes_start + 1)
        
    # at this point all parts have been downloaded
    with open(filename, 'wb') as file:
        for i in range(n_chunks):
            part_info = tracker.get_part_info(i)
            part_filename = part_info["part_filename"]
            with open(part_filename, 'rb') as part_file:
                file.write(part_file.read())
    tracker.cleanup()
    return filename


def _download_generic(url, filename, head=None):
    urllib.request.urlretrieve(url, filename)
    return filename


def guess_download_kind(url):
    if 'azure' in url:
        return 'azure'
    elif 's3' in url:
        return 's3'
    elif 'ftp' in url:
        return 'ftp'
    elif 'http' in url:  # note works for https too
        return 'http'
    else:
        return 'generic'


def download_url(url, kind='guess', filename=None, head=None, progress_tracker=None, target_uuid=None):
    """Return a local filepath to the downloaded file. Download the file."""
    if filename and isfile(filename):
        file_size = getsize(filename)
        if file_size > 0:
            logger.info(f"File already exists: {filename}. Not overwriting.")
            return filename
    if kind == 'guess':
        kind = guess_download_kind(url)
        logger.info(f"Guessed download kind: {kind} for {url}")
    logger.info(f"Downloading {kind} file to {filename}")
    if kind == 'generic':
        return _download_generic(url, filename, head=head)
    elif kind == 's3':
        return _download_head(url, filename, head=head, progress_tracker=progress_tracker)
    elif kind == 'azure':
        return _download_head(url, filename, head=head)
    elif kind == 'ftp':
        return download_ftp(url, filename, head=head)
    elif kind == 'http':
        # for http[s] files we care about head is often respected in practice (e.g. by the ENA)
        if not url.startswith("http"):
            url = "https://" + url
        return _download_head(url, filename, head=head, progress_tracker=progress_tracker)
    else:
        raise ValueError(f"Unknown download kind: {kind}")


class ResultFileDownload:
    """Abstract class that handles download methods for result files."""

    def get_download_url(self):
        """Return a URL that can be used to download the file for this result."""
        blob_type = self.stored_data.get("__type__", "").lower()
        if blob_type not in ["s3", "sra", "ftp", "azure", "http"]:
            raise ValueError(f'Unknown URL type: "{blob_type}"')
        key = 'url' if 'url' in self.stored_data else 'uri'
        if blob_type in ["s3", "azure"]:
            try:
                url = self.stored_data["presigned_url"]
            except KeyError:
                url = self.stored_data[key]
            if url.startswith("s3://"):
                url = self.stored_data["endpoint_url"] + "/" + url[5:]
            return url
        else:
            return self.stored_data[key]
        
    def _download_flag_path(self, filename, flag_suffix='.gs_downloaded'):
        return filename + flag_suffix
        
    def download_needs_update(self, filename, flag_suffix='.gs_downloaded', slack=5):
        """Return True if the file needs to be downloaded, False otherwise.
        
        If either the file or the flag file does not exist, return True.
        If the flag file is older than `updated_at` in the result, return True.
        Otherwise, return False.
        """
        if isfile(filename) and isfile(self._download_flag_path(filename, flag_suffix)):
            if self.updated_at_timestamp - getmtime(self._download_flag_path(filename, flag_suffix)) > slack:
                return True
            return False
        return True

    def download(self, filename=None, flag_suffix='.gs_downloaded', cache=True, head=None, progress_tracker=None):
        """Return a local filepath to the file in this result. Download the file if necessary.
        
        When the file is downloaded, it is cached in the result object. Subsequent calls to download
        on this object will return the cached file unless cache=False is specified or the file is updated
        on the server.

        A flag file is created when the file download is complete. Subsequent calls to download
        will return the cached file if the flag file exists unless cache=False is specified.
        """
        if not filename and not self._cached_filename:
            self._temp_filename = True
            myfile = NamedTemporaryFile(delete=False)
            myfile.close()
            filename = myfile.name
        elif not filename and self._cached_filename:
            filename = self._cached_filename

        blob_type = self.stored_data.get("__type__", "").lower()
        needs_update = self.download_needs_update(filename, flag_suffix)
        if not needs_update:
            if cache and self._cached_filename:
                return self._cached_filename
            if cache and flag_suffix:
                # check if file and flag file exist, if so, return filename
                if isfile(filename) and isfile(self._download_flag_path(filename, flag_suffix)):
                    return filename

        url = self.get_download_url()
        filepath = download_url(
            url, kind=blob_type, filename=filename,
            head=head, progress_tracker=progress_tracker,
        )
        if cache and flag_suffix:
            # create flag file
            open(self._download_flag_path(filename, flag_suffix), 'a').close()
        if cache:
            self._cached_filename = filepath
        return filepath
