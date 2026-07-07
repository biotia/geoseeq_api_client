
import urllib.request
import logging
import requests
import os
import shutil
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
        return _download_resumable(response.url, filename, total_size_in_bytes, progress_tracker)
    else:
        block_size = FIVE_MB
        with open(filename, 'wb') as file:
            for data in response.iter_content(block_size):
                if progress_tracker: progress_tracker.update(len(data))
                file.write(data)
        return filename
    

def _ranged_get_part(url, part_filename, start, end):
    """Fetch one inclusive byte range via a ranged GET (works for S3/HTTP presigned URLs)."""
    _download_head(url, part_filename, head=end, start=start, progress_tracker=None)


def _concatenate_parts(filename, tracker, n_chunks):
    """Concatenate downloaded parts into ``filename`` atomically (temp + os.replace).

    Writing to a temp path and renaming keeps ``filename`` all-or-nothing: an interrupted
    concat never leaves a truncated file that ``download_url``'s isfile()+size>0 cache
    check would treat as a complete download.
    """
    tmp_path = filename + ".partial"
    with open(tmp_path, "wb") as out_file:
        for i in range(n_chunks):
            part_filename = tracker.get_part_info(i)["part_filename"]
            with open(part_filename, "rb") as part_file:
                shutil.copyfileobj(part_file, out_file)
    os.replace(tmp_path, filename)


def _download_resumable(url, filename, total_size_in_bytes, progress_tracker=None,
                        chunk_size=5 * FIVE_MB, part_prefix=".gs_download_{}_{}.",
                        download_part=None):
    """Download a large file as resumable ranged parts persisted to disk, then concatenate.

    ``download_part(url, part_filename, start, end)`` fetches one inclusive byte range to
    ``part_filename``. It defaults to ranged GETs (``_ranged_get_part``) for S3/HTTP
    presigned URLs; the azure path supplies an SDK-backed downloader. Completed parts are
    recorded by the ``ResumableDownloadTracker`` so an interrupted download resumes across
    process restarts without re-fetching finished parts.
    """
    if download_part is None:
        download_part = _ranged_get_part
    target_id = url_to_id(url)
    tracker = ResumableDownloadTracker(chunk_size, target_id, filename)
    if not tracker.download_started: tracker.start_download(url)
    n_chunks = ceil(total_size_in_bytes / chunk_size)
    for i in range(n_chunks):
        bytes_start, bytes_end = i * chunk_size, min((i + 1) * chunk_size - 1, total_size_in_bytes - 1)
        if tracker.part_has_been_downloaded(i):
            logger.debug(f"Part {i} has already been downloaded.")
        else:
            logger.debug(f"Downloading part {i} of {n_chunks - 1}")
            part_filename = join(dirname(filename), part_prefix.format(i, n_chunks - 1) + basename(filename))
            download_part(url, part_filename, bytes_start, bytes_end)
            part_info = dict(part_number=i, start=bytes_start, end=bytes_end, part_filename=part_filename)
            tracker.add_part(part_info)
        if progress_tracker: progress_tracker.update(bytes_end - bytes_start + 1)

    _concatenate_parts(filename, tracker, n_chunks)
    tracker.cleanup()
    return filename


def _download_generic(url, filename, head=None):
    urllib.request.urlretrieve(url, filename)
    return filename


def _azure_sdk_part_downloader(max_concurrency, n_tries=3):
    """Return a ``download_part(url, part_filename, start, end)`` backed by the azure SDK.

    Each part is fetched as a single ranged read (``offset``/``length``) with the SDK's own
    ``max_concurrency`` splitting that part into concurrent sub-reads, so downloads stay fast
    while remaining resumable at the part level. The part is written to a temp file and
    atomically renamed so a mid-stream failure never leaves a truncated part behind.
    """
    from azure.storage.blob import BlobClient

    def download_part(url, part_filename, start, end):
        length = end - start + 1
        tmp_path = part_filename + ".partial"
        for attempt in range(n_tries):
            try:
                client = BlobClient.from_blob_url(url)
                stream = client.download_blob(max_concurrency=max_concurrency, offset=start, length=length)
                with open(tmp_path, "wb") as f:
                    stream.readinto(f)
                os.replace(tmp_path, part_filename)
                return
            except Exception as e:
                logger.warning(f"azure part [{start}-{end}] failed (attempt {attempt + 1}/{n_tries}): {e}")
                if os.path.isfile(tmp_path):
                    os.remove(tmp_path)
                if attempt + 1 == n_tries:
                    raise

    return download_part


def _download_azure_resumable(url, filename, total_size, progress_tracker=None,
                              max_concurrency=8, chunk_size=5 * FIVE_MB, n_tries=3):
    """Download a large azure blob as resumable ranged parts via ``ResumableDownloadTracker``.

    Mirrors the S3 ``_download_resumable`` path: parts are persisted to disk and re-used across
    process restarts, so a 90 GB download that drops near the end resumes instead of restarting.
    """
    logger.info(
        f"azure-storage-blob SDK: resumable download {filename} "
        f"({total_size} bytes, chunk_size={chunk_size}, max_concurrency={max_concurrency})"
    )
    if progress_tracker:
        progress_tracker.set_num_chunks(total_size)
    download_part = _azure_sdk_part_downloader(max_concurrency, n_tries=n_tries)
    return _download_resumable(
        url, filename, total_size, progress_tracker=progress_tracker,
        chunk_size=chunk_size, download_part=download_part,
    )


def _download_azure_single_shot(client, filename, head=None, progress_tracker=None,
                                max_concurrency=8, n_tries=3):
    """Single-shot SDK download to a temp file, atomically renamed on success.

    Used for small blobs and head-bounded previews. A mid-stream transport failure
    (e.g. `[SYS] unknown error (_ssl.c:2578)`) must not leave a truncated file at `filename`:
    download_url() only checks isfile()+size>0, so a non-empty partial would be treated as a
    valid cached download forever, making the caller's retry loop a no-op. Temp + os.replace()
    keeps `filename` all-or-nothing, and the inner retry self-heals transient blips.
    """
    logger.info(f"azure-storage-blob SDK: downloading {filename} with max_concurrency={max_concurrency}")
    kwargs = {"max_concurrency": max_concurrency}
    if head and head > 0:
        kwargs["offset"] = 0
        kwargs["length"] = head + 1
    tmp_path = filename + ".partial"
    for attempt in range(n_tries):
        try:
            stream = client.download_blob(**kwargs)
            if progress_tracker:
                progress_tracker.set_num_chunks(stream.size)
            with open(tmp_path, "wb") as f:
                stream.readinto(f)
            os.replace(tmp_path, filename)
            if progress_tracker:
                progress_tracker.update(stream.size)
            return filename
        except Exception as e:
            logger.warning(f"azure SDK download failed (attempt {attempt + 1}/{n_tries}) for {filename}: {e}")
            if os.path.isfile(tmp_path):
                os.remove(tmp_path)
            if attempt + 1 == n_tries:
                raise


def _download_azure_sdk(url, filename, head=None, progress_tracker=None,
                        max_concurrency=8, chunk_size=5 * FIVE_MB, n_tries=3):
    """Download an azure blob via the azure-storage-blob SDK.

    Full downloads larger than ``10 * FIVE_MB`` use a resumable ranged strategy so an
    interrupted transfer resumes across process restarts (parity with the S3 path). Smaller
    blobs and head-bounded previews use a single-shot download. ``max_concurrency`` and
    ``chunk_size`` are threaded from the caller so azure download parallelism is runtime
    configurable rather than hardcoded.

    Raises ImportError if azure-storage-blob is not installed so ``download_url`` can fall
    back to ranged GETs.
    """
    # ponytail: presigned blob URL works as-is with from_blob_url, no auth rework
    from azure.storage.blob import BlobClient
    client = BlobClient.from_blob_url(url)
    if not head or head <= 0:
        total_size = client.get_blob_properties().size
        if total_size > 10 * FIVE_MB:
            return _download_azure_resumable(
                url, filename, total_size, progress_tracker=progress_tracker,
                max_concurrency=max_concurrency, chunk_size=chunk_size, n_tries=n_tries,
            )
    return _download_azure_single_shot(
        client, filename, head=head, progress_tracker=progress_tracker,
        max_concurrency=max_concurrency, n_tries=n_tries,
    )


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


def download_url(url, kind='guess', filename=None, head=None, progress_tracker=None,
                 target_uuid=None, max_concurrency=8, chunk_size=5 * FIVE_MB):
    """Return a local filepath to the downloaded file. Download the file.

    ``max_concurrency`` and ``chunk_size`` tune azure downloads: large azure blobs are
    fetched as resumable ``chunk_size`` parts, each read with the SDK's ``max_concurrency``.
    """
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
        try:
            return _download_azure_sdk(
                url, filename, head=head, progress_tracker=progress_tracker,
                max_concurrency=max_concurrency, chunk_size=chunk_size,
            )
        except ImportError:
            logger.warning(
                "azure-storage-blob not installed; falling back to single-threaded ranged GETs. "
                "Install geoseeq[azure] for multipart downloads."
            )
            return _download_head(url, filename, head=head, progress_tracker=progress_tracker)
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

    def download(self, filename=None, flag_suffix='.gs_downloaded', cache=True, head=None,
                 progress_tracker=None, max_concurrency=8, chunk_size=5 * FIVE_MB):
        """Return a local filepath to the file in this result. Download the file if necessary.

        When the file is downloaded, it is cached in the result object. Subsequent calls to download
        on this object will return the cached file unless cache=False is specified or the file is updated
        on the server.

        A flag file is created when the file download is complete. Subsequent calls to download
        will return the cached file if the flag file exists unless cache=False is specified.

        ``max_concurrency`` and ``chunk_size`` tune azure downloads (see ``download_url``).
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
            max_concurrency=max_concurrency, chunk_size=chunk_size,
        )
        if cache and flag_suffix:
            # create flag file
            open(self._download_flag_path(filename, flag_suffix), 'a').close()
        if cache:
            self._cached_filename = filepath
        return filepath
