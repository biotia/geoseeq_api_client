
import time
import json
import os
from os.path import basename, getsize, join, dirname, isfile, getctime
from pathlib import Path
from random import random
import requests

from geoseeq.knex import GeoseeqGeneralError
from geoseeq.constants import FIVE_MB
from geoseeq.utils import md5_checksum
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import *
from geoseeq.file_system_cache import GEOSEEQ_CACHE_DIR
from .file_chunker import FileChunker
from .resumable_upload_tracker import ResumableUploadTracker


class ResultFileUpload:
    """Abstract class that handles upload methods for result files."""

    def _result_type(self, atomic=False):
        if self.is_sample_result:
            return "sample"
        if atomic:
            return "project"
        return "group"

    def _create_multipart_upload(self, filepath, file_size, optional_fields, atomic=False):
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
            "result_type": self._result_type(atomic),
        }
        url = f"/ar_fields/{self.uuid}/create_upload"
        if atomic:
            data["fieldname"] = self.name
            url = f"/ars/{self.parent.uuid}/create_atomic_upload"
        response = self.knex.post(url, json=data)
        return response
    
    def _prep_multipart_upload(self, filepath, file_size, chunk_size, optional_fields, atomic=False):
        n_parts = int(file_size / chunk_size) + 1
        response = self._create_multipart_upload(filepath, file_size, optional_fields, atomic=atomic)
        upload_id = response["upload_id"]
        data = {
            "parts": list(range(1, n_parts + 1)),
            "stance": "upload-multipart",
            "upload_id": upload_id,
            "result_type": self._result_type(atomic),
        }
        url = f"/ar_fields/{self.uuid}/create_upload_urls"
        if atomic:
            data["uuid"] = response["uuid"]
            data["fieldname"] = self.name
            url = f"ars/{self.parent.uuid}/create_atomic_upload_urls"
        response = self.knex.post(url, json=data)
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
                # url = url.replace("s3.wasabisys.com", "s3.us-east-1.wasabisys.com")
                logger.debug(f"Uploading part {num + 1} to {url}. Size: {len(file_chunk)} bytes.")
                if session:
                    http_response = session.put(url, data=file_chunk)
                else:
                    http_response = requests.put(url, data=file_chunk)
                http_response.raise_for_status()
                logger.debug(f"Upload for part {num + 1} succeeded.")
                break
            except (requests.exceptions.HTTPError, requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                attempts += 1
                logger.debug(
                    f"Upload for part {num + 1} failed. Attempt {attempts} of {max_retries}. Error: {e}"
                )
                if attempts >= max_retries:
                    raise e

                retry_time = min(8 ** attempts, 120)  # exponential backoff, max 120s
                retry_time *= 0.6 + (random() * 0.8)  # randomize to avoid thundering herd
                logger.debug(f"Retrying upload for part {num + 1} in {retry_time} seconds.")
                time.sleep(retry_time)
            
        etag = http_response.headers["ETag"].replace('"', "")
        blob = {"ETag": etag, "PartNumber": num + 1}
        if resumable_upload_tracker:
            # TODO technically not thread safe, but should be fine for now
            resumable_upload_tracker.add_part(blob)
        return blob
    
    def _finish_multipart_upload(self, upload_id, complete_parts, atomic=False):
        data = {
            "parts": complete_parts,
            "upload_id": upload_id,
            "result_type": self._result_type(atomic),
        }
        url = f"/ar_fields/{self.uuid}/complete_upload"
        if atomic:
            data["fieldname"] = self.name
            url = f"/ars/{self.parent.uuid}/complete_atomic_upload"
        response = self.knex.post(url, json=data, json_response=False)
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
        chunk_size=None,
        max_retries=3,
        session=None,
        progress_tracker=None,
        threads=1,
        use_cache=True,
        use_atomic_upload=False,
    ):
        """Upload a file to S3 using the multipart upload process."""
        logger.info(f"Uploading {filepath} to S3 using multipart upload.")
        if not chunk_size:
            chunk_size = FIVE_MB
            if file_size >= 10 * FIVE_MB:
                chunk_size = 5 * FIVE_MB
        logger.debug(f"Using chunk size of {chunk_size} bytes.")
        resumable_upload_tracker = None
        if use_cache and file_size > 10 * FIVE_MB:  # only use resumable upload tracker for larger files
            upload_target_uuid = self.parent.uuid if use_atomic_upload else self.uuid
            resumable_upload_tracker = ResumableUploadTracker(filepath, chunk_size, upload_target_uuid)

        if resumable_upload_tracker and resumable_upload_tracker.upload_started:
            # a resumable upload for this file has already started
            resumable_upload_exists_and_is_valid = True
            upload_id, urls = resumable_upload_tracker.upload_id, resumable_upload_tracker.urls
            use_atomic_upload = resumable_upload_tracker.is_atomic_upload
            logger.info(f'Resuming upload for "{filepath}", upload_id: "{upload_id}"')
        else:
            upload_id, urls = self._prep_multipart_upload(filepath, file_size, chunk_size, optional_fields, atomic=use_atomic_upload)
            if resumable_upload_tracker:
                logger.info(f'Creating new resumable upload for "{filepath}", upload_id: "{upload_id}"')
                resumable_upload_tracker.start_upload(upload_id, urls, is_atomic_upload=use_atomic_upload)

        logger.info(f'Starting upload for "{filepath}"')
        complete_parts = []
        file_chunker = FileChunker(filepath, chunk_size)
        if file_chunker.file_size < 10 * FIVE_MB:
            file_chunker.load_all_chunks()
            logger.debug(f"Preloaded all chunks for {filepath}")
        else:
            logger.debug(f"Did not preload chunks for {filepath}")
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
        self._finish_multipart_upload(upload_id, complete_parts, atomic=use_atomic_upload)
        logger.info(f'Finished Upload for "{filepath}"')
        if use_atomic_upload:
            # if this was an atomic upload then this result may not have existed on the server before
            self.get()
        return self

    def upload_file(self, filepath, multipart_thresh=FIVE_MB, overwrite=True, no_new_versions=False, **kwargs):
        if self.exists() and not overwrite:  
            raise GeoseeqGeneralError(f"Overwrite is set to False and file {self.uuid} already exists.")
        if not kwargs.get("use_atomic_upload", False):
            self.idem()
        else:
            self.parent.idem()
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
