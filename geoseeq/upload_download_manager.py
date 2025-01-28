import logging
from multiprocessing import Pool, current_process
from os.path import basename, join, dirname
from geoseeq.result import ResultFile
from geoseeq.result.file_download import download_url
from os import makedirs

logger = logging.getLogger('geoseeq_api')
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program


def _make_in_process_logger(log_level):
    logger = logging.getLogger('geoseeq_api')
    logger.setLevel(log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(levelname)s] %(name)s :: ' + current_process().name + ' :: %(message)s'))
    logger.addHandler(handler)
    return logger


def _upload_one_file(args):
    (result_file, filepath, session, progress_tracker,
     link_type, overwrite, log_level, parallel_uploads,
     use_cache, no_new_versions, threads_per_upload,
     num_retries, ignore_errors, chunk_size_mb, use_atomic_upload) = args
    chunk_size = chunk_size_mb * 1024 * 1024 if chunk_size_mb else None
    if parallel_uploads:
        _make_in_process_logger(log_level)
    try:
        if link_type == 'upload':
            # TODO: check checksums to see if the file is the same
            result_file.upload_file(
                filepath,
                session=session, overwrite=overwrite, progress_tracker=progress_tracker,
                threads=threads_per_upload, use_cache=use_cache, chunk_size=chunk_size,
                no_new_versions=no_new_versions, max_retries=num_retries,
                use_atomic_upload=use_atomic_upload
            )
        else:
            result_file.link_file(link_type, filepath)
    except Exception as e:
        if ignore_errors:
            logger.error(f"Error uploading {filepath}: {e}")
        else:
            raise e
    return result_file


class GeoSeeqUploadManager:

    def __init__(self,
                 n_parallel_uploads=1,
                 threads_per_upload=4,
                 session=None,
                 link_type='upload',
                 progress_tracker_factory=None,
                 log_level=logging.WARNING,
                 overwrite=True,
                 no_new_versions=False,
                 num_retries=3,
                 ignore_errors=False,
                 chunk_size_mb=5,
                 use_atomic_upload=True,
                 use_cache=True):
        self.session = session
        self.n_parallel_uploads = n_parallel_uploads
        self.progress_tracker_factory = progress_tracker_factory if progress_tracker_factory else lambda x: None
        self.log_level = log_level
        self.link_type = link_type
        self.overwrite = overwrite
        self._result_files = []
        self.no_new_versions = no_new_versions
        self.use_cache = use_cache
        self.threads_per_upload = threads_per_upload
        self.num_retries = num_retries
        self.ignore_errors = ignore_errors
        self.chunk_size_mb = chunk_size_mb
        self.use_atomic_upload = use_atomic_upload

    def add_result_file(self, result_file, local_path):
        self._result_files.append((result_file, local_path))

    def add_local_file_to_result_folder(self, result_folder, local_path, geoseeq_file_name=None):
        if not geoseeq_file_name:
            if local_path.startswith("/"):  # if local path is an absolute path use the basename
                geoseeq_file_name = basename(local_path)
            else:
                # remove "./" and "../" from local path to get a geoseeq file name
                geoseeq_file_name = local_path.replace("./", "").replace("../", "")
        result_file = result_folder.result_file(geoseeq_file_name)
        self.add_result_file(result_file, local_path)

    def add_local_folder_to_result_folder(self, result_folder, local_path, recursive=False, hidden_files=False, prefix=""):
        for result_file, local_path in result_folder._prepare_folder_upload(local_path, recursive, hidden_files, prefix):
            self.add_result_file(result_file, local_path)

    def get_preview_string(self):
        out = ["Upload Preview:"]
        for result_file, local_path in self._result_files:
            out.append(f"{local_path} -> {result_file}")
        return "\n".join(out)

    def upload_files(self):
        upload_args = [(
                result_file, local_path,
                self.session, self.progress_tracker_factory(local_path),
                self.link_type, self.overwrite, self.log_level,
                self.n_parallel_uploads > 1, self.use_cache, self.no_new_versions,
                self.threads_per_upload, self.num_retries, self.ignore_errors,
                self.chunk_size_mb, self.use_atomic_upload
            ) for result_file, local_path in self._result_files
        ]
        out = []
        if self.n_parallel_uploads == 1:
            logger.info(f"Uploading files in series.")
            for upload_arg in upload_args:
                out.append(_upload_one_file(upload_arg))
        else:
            logger.info(f"Uploading files in parallel with {self.n_parallel_uploads} threads.")
            with Pool(self.n_parallel_uploads) as p:
                for uploaded_result_file in p.imap_unordered(_upload_one_file, upload_args):
                    out.append(uploaded_result_file)
        return out
    
    def __len__(self):
        return len(self._result_files)


def _download_one_file(args):
    url, file_path, key, callback, pbar, ignore_errors, head, log_level, parallel_downloads = args
    if parallel_downloads:
        _make_in_process_logger(log_level)
    if dirname(file_path):
        makedirs(dirname(file_path), exist_ok=True)
    if isinstance(url, ResultFile):
        local_path = url.download(filename=file_path, progress_tracker=pbar, head=head)
    else:
        local_path = download_url(url, filename=file_path, progress_tracker=pbar, head=head)
    try:
        if callback is not None:
            callback_result = callback(key, local_path)
        else:
            callback_result = None
        return local_path, key, callback_result
    except Exception as e:
        if ignore_errors:
            logger.error(f"Error downloading {url}: {e}")
        else:
            raise e
        

class GeoSeeqDownloadManager:

    def __init__(self, n_parallel_downloads=1, ignore_errors=False, head=False, progress_tracker_factory=None, log_level=logging.WARNING):
        self.n_parallel_downloads = n_parallel_downloads
        self.ignore_errors = ignore_errors
        self.head = head
        self.progress_tracker_factory = progress_tracker_factory if progress_tracker_factory else lambda x: None
        self.log_level = log_level
        self._result_files = []


    def add_download(self, url, file_path=None, progress_tracker=None, callback=None, key=None):
        if not file_path:
            if isinstance(url, ResultFile):
                file_path = url.get_local_filename()
            else:
                raise ValueError("file_path must be provided if url is not a ResultFile object.")
        self._result_files.append((url, file_path, key, callback))


    def add_result_folder_download(self, result_folder, local_folder_path, hidden_files=True):
        for result_file in result_folder.get_result_files():
            if not hidden_files and result_file.name.startswith("."):
                continue
            self.add_download(result_file, join(local_folder_path, result_file.get_local_filename()))

    def get_preview_string(self):
        out = ["Download Preview:"]
        for url, file_path, _, _ in self._result_files:
            out.append(f"{url} -> {file_path}")
        return "\n".join(out)
    
    def get_url_string(self):
        self._convert_result_files_to_urls()
        out = []
        for url, _, _, _ in self._result_files:
            out.append(url)
        return "\n".join(out)
    
    def __len__(self):
        return len(self._result_files)
    
    def _convert_result_files_to_urls(self):
        self._result_files = [(
            url.get_download_url() if isinstance(url, ResultFile) else url,
            file_path,
            key,
            callback
        ) for url, file_path, key, callback in self._result_files]

    def download_files(self):
        """Return a list of 3-ples (local_path, key, callback_result).
        
        If no key was provided the key is None
        If no callback was provided with the file callback result is None
        """
        # self._convert_result_files_to_urls()
        download_args = [(
            url, file_path, key, callback,
            self.progress_tracker_factory(file_path),
            self.ignore_errors, self.head, self.log_level,
            self.n_parallel_downloads > 1
        ) for url, file_path, key, callback in self._result_files]
        out = []
        if self.n_parallel_downloads == 1:
            logger.info(f"Downloading files in series.")
            for download_arg in download_args:
                out.append(_download_one_file(download_arg))
        else:
            logger.info(f"Downloading files in parallel with {self.n_parallel_downloads} threads.")
            with Pool(self.n_parallel_downloads) as p:
                for downloaded_file in p.imap_unordered(_download_one_file, download_args):
                    out.append(downloaded_file)
        return out
