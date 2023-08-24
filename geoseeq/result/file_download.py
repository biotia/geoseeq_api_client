
import urllib.request
import logging
import requests
from os.path import basename, getsize, join, isfile
from pathlib import Path
from tempfile import NamedTemporaryFile

from geoseeq.utils import download_ftp
from geoseeq.constants import FIVE_MB

logger = logging.getLogger("geoseeq_api")  # Same name as calling module


def _download_head(url, filename, head=None, progress_tracker=None):
    headers = None
    if head and head > 0:
        headers = {"Range": f"bytes=0-{head}"}
    response = requests.get(url, stream=True, headers=headers)
    response.raise_for_status()
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    if progress_tracker: progress_tracker.set_num_chunks(total_size_in_bytes)
    block_size = FIVE_MB
    with open(filename, 'wb') as file:
        for data in response.iter_content(block_size):
            if progress_tracker: progress_tracker.update(len(data))
            file.write(data)
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
    else:
        return 'generic'


def download_url(url, kind='guess', filename=None, head=None, progress_tracker=None):
    """Return a local filepath to the downloaded file. Download the file."""
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
    else:
        raise ValueError(f"Unknown download kind: {kind}")



class ResultFileDownload:
    """Abstract class that handles download methods for result files."""

    def get_download_url(self):
        """Return a URL that can be used to download the file for this result."""
        blob_type = self.stored_data.get("__type__", "").lower()
        if blob_type not in ["s3", "sra", "ftp", "azure"]:
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

    def download(self, filename=None, flag_suffix='.gs_downloaded', cache=True, head=None, progress_tracker=None):
        """Return a local filepath to the file in this result. Download the file if necessary.
        
        When the file is downloaded, it is cached in the result object. Subsequent calls to download
        on this object will return the cached file unless cache=False is specified.

        A flag file is created when the file download is complete. Subsequent calls to download
        will return the cached file if the flag file exists unless cache=False is specified.
        """
        if not filename and not not self._cached_filename:
            self._temp_filename = True
            myfile = NamedTemporaryFile(delete=False)
            myfile.close()
            filename = myfile.name
        elif not filename and self._cached_filename:
            filename = self._cached_filename

        blob_type = self.stored_data.get("__type__", "").lower()
        if cache and self._cached_filename:
            return self._cached_filename
        flag_filename = filename + flag_suffix
        if cache and flag_suffix:
            # check if file and flag file exist, if so, return filename
            if isfile(filename) and isfile(flag_filename):
                return filename

        url = self.get_download_url()
        filepath = download_url(
            url, blob_type, filename,
            head=head, progress_tracker=progress_tracker
        )
        if cache and flag_suffix:
            # create flag file
            open(flag_filename, 'a').close()
        if cache:
            self._cached_filename = filepath
        return filepath
