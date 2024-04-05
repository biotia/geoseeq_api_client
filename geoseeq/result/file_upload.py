
import time
import json
import os
from os.path import basename, getsize, join, dirname, isfile
from pathlib import Path

import requests

from geoseeq.knex import GeoseeqGeneralError
from geoseeq.constants import FIVE_MB
from geoseeq.utils import md5_checksum
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import *
from geoseeq.file_system_cache import GEOSEEQ_CACHE_DIR

class FileChunker:

    def __init__(self, filepath, chunk_size):
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.file_size = getsize(filepath)
        self.n_parts = int(self.file_size / self.chunk_size) + 1
        self.loaded_parts = []

    def load_all_chunks(self):
        if len(self.loaded_parts) != self.n_parts:
            with open(self.filepath, "rb") as f:
                f.seek(0)
                for i in range(self.n_parts):
                    chunk = f.read(self.chunk_size)
                    self.loaded_parts.append(chunk)
        return self  # convenience for chaining

    def get_chunk(self, num):
        self.load_all_chunks()
        return self.loaded_parts[num]
    
    def get_chunk_size(self, num):
        self.load_all_chunks()
        return len(self.loaded_parts[num])
    

class ResumableUploadTracker:

    def __init__(self, filepath, chunk_size, tracker_file_prefix="gs_resumable_upload_tracker"):
        self.open, self.upload_started = True, False
        self.upload_id, self.urls = None, None
        self.filepath = filepath
        self.tracker_file = join(
            GEOSEEQ_CACHE_DIR, 'upload',
            tracker_file_prefix + f".{chunk_size}." + basename(filepath)
        )
        try:
            os.makedirs(dirname(self.tracker_file), exist_ok=True)
        except Exception as e:
            logger.warning(f'Could not create resumable upload tracker directory. {e}')
            self.open = False
        self._loaded_parts = {}
        self._load_parts_from_file()

    def start_upload(self, upload_id, urls):
        if not self.open:
            return
        if self.upload_started:
            raise GeoseeqGeneralError("Upload has already started.")
        blob = dict(upload_id=upload_id, urls=urls)
        serialized = json.dumps(blob)
        with open(self.tracker_file, "w") as f:
            f.write(serialized + "\n")
        self.upload_id, self.urls = upload_id, urls
        self.upload_started = True
    
    def add_part(self, part_upload_info):
        if not self.open:
            return
        part_id = part_upload_info["PartNumber"]
        serialized = json.dumps(part_upload_info)
        with open(self.tracker_file, "a") as f:
            f.write(serialized + "\n")
        self._loaded_parts[part_id] = part_upload_info
        if len(self._loaded_parts) == len(self.urls):
            self.cleanup()
            self.open = False
    
    def _load_parts_from_file(self):
        if not isfile(self.tracker_file):
            return
        with open(self.tracker_file, "r") as f:
            header_blob = json.loads(f.readline())
            self.upload_id, self.urls = header_blob["upload_id"], header_blob["urls"]
            self.upload_started = True
            for line in f:
                blob = json.loads(line)
                part_id = blob["PartNumber"]
                self._loaded_parts[part_id] = blob
    
    def part_has_been_uploaded(self, part_number):
        if not self.open:
            return False
        return part_number in self._loaded_parts
    
    def get_part_info(self, part_number):
        return self._loaded_parts[part_number]
    
    def cleanup(self):
        if not self.open:
            return
        try:
            os.remove(self.tracker_file)
        except FileNotFoundError:
            pass


class ResultFileUpload:
    """Abstract class that handles upload methods for result files."""

    def _create_multipart_upload(self, filepath, file_size, optional_fields):
        optional_fields = optional_fields if optional_fields else {}
        optional_fields.update(
            {
                "md5_checksum": md5_checksum(filepath),
                "file_size_bytes": file_size,
            }
        )
        data = {
            "filename": basename(filepath),
            "optional_fields": optional_fields,
            "result_type": "sample" if self.is_sample_result else "group",
        }
        response = self.knex.post(f"/ar_fields/{self.uuid}/create_upload", json=data)
        return response
    
    def _prep_multipart_upload(self, filepath, file_size, chunk_size, optional_fields):
        n_parts = int(file_size / chunk_size) + 1
        response = self._create_multipart_upload(filepath, file_size, optional_fields)
        upload_id = response["upload_id"]
        parts = list(range(1, n_parts + 1))
        data = {
            "parts": parts,
            "stance": "upload-multipart",
            "upload_id": upload_id,
            "result_type": "sample" if self.is_sample_result else "group",
        }
        response = self.knex.post(f"/ar_fields/{self.uuid}/create_upload_urls", json=data)
        urls = response
        return upload_id, urls
    
    def _upload_one_part(self, file_chunker, url, num, max_retries, session=None, resumable_upload_tracker=None):
        if resumable_upload_tracker and resumable_upload_tracker.part_has_been_uploaded(num + 1):
            logger.info(f"Part {num + 1} has already been uploaded. Skipping.")
            return resumable_upload_tracker.get_part_info(num + 1)
        file_chunk = file_chunker.get_chunk(num)
        attempts = 0
        while attempts < max_retries:
            try:
                if session:
                    http_response = session.put(url, data=file_chunk)
                else:
                    http_response = requests.put(url, data=file_chunk)
                http_response.raise_for_status()
                logger.debug(f"Upload for part {num + 1} succeeded.")
                break
            except requests.exceptions.HTTPError:
                logger.warn(
                    f"Upload for part {num + 1} failed. Attempt {attempts + 1} of {max_retries}."
                )
                attempts += 1
                if attempts == max_retries:
                    raise
                time.sleep(10**attempts)  # exponential backoff, (10 ** 2)s default max
        etag = http_response.headers["ETag"].replace('"', "")
        blob = {"ETag": etag, "PartNumber": num + 1}
        if resumable_upload_tracker:
            # TODO technically not thread safe, but should be fine for now
            resumable_upload_tracker.add_part(blob)
        return blob
    
    def _finish_multipart_upload(self, upload_id, complete_parts):
        response = self.knex.post(
            f"/ar_fields/{self.uuid}/complete_upload",
            json={
                "parts": complete_parts,
                "upload_id": upload_id,
                "result_type": "sample" if self.is_sample_result else "group",
            },
            json_response=False,
        )
        response.raise_for_status()

    def _upload_parts(self, file_chunker, urls, max_retries, session, progress_tracker, threads, resumable_upload_tracker=None):
        if threads == 1:
            logger.info(f"Uploading parts in series for {file_chunker.filepath}")
            complete_parts = []
            for num, url in enumerate(list(urls.values())):
                response_part = self._upload_one_part(file_chunker, url, num, max_retries, session, resumable_upload_tracker)
                complete_parts.append(response_part)
                if progress_tracker: progress_tracker.update(file_chunker.get_chunk_size(num))
                logger.info(f'Uploaded part {num + 1} of {len(urls)} for "{file_chunker.filepath}"')
            return complete_parts
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            logger.info(f"Uploading parts in parallel for {file_chunker.filepath} with {threads} threads.")
            futures = []
            for num, url in enumerate(list(urls.values())):
                future = executor.submit(
                    self._upload_one_part, file_chunker, url, num, max_retries, session, resumable_upload_tracker
                )
                futures.append(future)
            complete_parts = []
            for future in as_completed(futures):
                response_part = future.result()
                complete_parts.append(response_part)
                if progress_tracker: progress_tracker.update(file_chunker.get_chunk_size(response_part["PartNumber"] - 1))
                logger.info(
                    f'Uploaded part {response_part["PartNumber"]} of {len(urls)} for "{file_chunker.filepath}"'
                )
        complete_parts = sorted(complete_parts, key=lambda x: x["PartNumber"])
        return complete_parts

    def multipart_upload_file(
        self,
        filepath,
        file_size,
        optional_fields=None,
        chunk_size=FIVE_MB,
        max_retries=3,
        session=None,
        progress_tracker=None,
        threads=1,
        use_cache=True,
    ):
        """Upload a file to S3 using the multipart upload process."""
        logger.info(f"Uploading {filepath} to S3 using multipart upload.")
        resumable_upload_tracker = None
        if use_cache and file_size > 10 * FIVE_MB:  # only use resumable upload tracker for larger files
            resumable_upload_tracker = ResumableUploadTracker(filepath, chunk_size)
        if resumable_upload_tracker and resumable_upload_tracker.upload_started:
            upload_id, urls = resumable_upload_tracker.upload_id, resumable_upload_tracker.urls
            logger.info(f'Resuming upload for "{filepath}", upload_id: "{upload_id}"')
        else:
            upload_id, urls = self._prep_multipart_upload(filepath, file_size, chunk_size, optional_fields)
            if resumable_upload_tracker:
                logger.info(f'Creating new resumable upload for "{filepath}", upload_id: "{upload_id}"')
                resumable_upload_tracker.start_upload(upload_id, urls)
        logger.info(f'Starting upload for "{filepath}"')
        complete_parts = []
        file_chunker = FileChunker(filepath, chunk_size).load_all_chunks()
        if progress_tracker: progress_tracker.set_num_chunks(file_chunker.file_size)
        complete_parts = self._upload_parts(
            file_chunker,
            urls,
            max_retries,
            session,
            progress_tracker,
            threads,
            resumable_upload_tracker=resumable_upload_tracker
        )
        self._finish_multipart_upload(upload_id, complete_parts)
        logger.info(f'Finished Upload for "{filepath}"')
        return self

    def upload_file(self, filepath, multipart_thresh=FIVE_MB, overwrite=True, no_new_versions=False, **kwargs):
        if self.exists() and not overwrite:  
            raise GeoseeqGeneralError(f"Overwrite is set to False and file {self.uuid} already exists.")
        self.idem()
        if no_new_versions and self.has_downloadable_file():
            raise GeoseeqGeneralError(f"File {self} already has a downloadable file. Not uploading a new version.")
        resolved_path = Path(filepath).resolve()
        file_size = getsize(resolved_path)
        return self.multipart_upload_file(filepath, file_size, **kwargs)
    
    def upload_json(self, data, **kwargs):
        """Upload a file with the given data as JSON."""
        with NamedTemporaryFile("w", suffix='.json') as f:
            json.dump(data, f)
            f.flush()
            return self.upload_file(f.name, **kwargs)
