
import urllib.request
import logging
from os.path import basename, getsize, join
from pathlib import Path
from tempfile import NamedTemporaryFile

from geoseeq.utils import download_ftp

from .utils import  _download_head

logger = logging.getLogger("geoseeq_api")  # Same name as calling module


class ResultFileDownload:
    """Abstract class that handles download methods for result files."""

    def get_download_url(self):
        """Return a URL that can be used to download the file for this result."""
        blob_type = self.stored_data.get("__type__", "").lower()
        if blob_type not in ["s3", "sra"]:
            raise TypeError("Cannot fetch a file for a BLOB type result field.")
        if blob_type == "s3":
            try:
                url = self.stored_data["presigned_url"]
            except KeyError:
                url = self.stored_data["uri"]
            if url.startswith("s3://"):
                url = self.stored_data["endpoint_url"] + "/" + url[5:]
            return url
        elif blob_type == "sra":
            url = self.stored_data["url"]
            return url

    def download_file(self, filename=None, cache=True, head=None):
        """Return a local filepath to the file this result points to."""
        if not filename:
            self._temp_filename = True
            myfile = NamedTemporaryFile(delete=False)
            myfile.close()
            filename = myfile.name
        blob_type = self.stored_data.get("__type__", "").lower()
        if cache and self._cached_filename:
            return self._cached_filename
        if blob_type == "s3":
            return self._download_s3(filename, cache, head=head)
        elif blob_type == "sra":
            return self._download_sra(filename, cache)
        elif blob_type == "ftp":
            return self._download_ftp(filename, cache)
        elif blob_type == "azure":
            return self._download_azure(filename, cache, head=head)
        else:
            raise TypeError("Cannot fetch a file for a BLOB type result field.")

    def _download_s3(self, filename, cache, head=None):
        logger.info(f"Downloading S3 file to {filename}")
        try:
            url = self.stored_data["presigned_url"]
        except KeyError:
            key = 'uri' if 'uri' in self.stored_data else 'url'
            url = self.stored_data[key]
        if url.startswith("s3://"):
            url = self.stored_data["endpoint_url"] + "/" + url[5:]
        _download_head(url, filename, head=head) 
        if cache:
            self._cached_filename = filename
        return filename

    def _download_azure(self, filename, cache, head=None):
        logger.info(f"Downloading Azure file to {filename}")
        try:
            url = self.stored_data["presigned_url"]
        except KeyError:
            key = 'uri' if 'uri' in self.stored_data else 'url'
            url = self.stored_data[key]
        _download_head(url, filename, head=head)
        if cache:
            self._cached_filename = filename
        return filename

    def _download_sra(self, filename, cache):
        return self._download_generic_url(filename, cache)

    def _download_ftp(self, filename, cache, head=None):
        logger.info(f"Downloading FTP file to {filename}")
        key = 'url' if 'url' in self.stored_data else 'uri'
        download_ftp(self.stored_data[key], filename, head=head)
        return filename

    def _download_generic_url(self, filename, cache):
        logger.info(f"Downloading generic URL file to {filename}")
        key = 'url' if 'url' in self.stored_data else 'uri'
        url = self.stored_data[key]
        urllib.request.urlretrieve(url, filename)
        if cache:
            self._cached_filename = filename
        return filename
