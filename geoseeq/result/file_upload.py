
import time
import json
from os.path import basename, getsize
from pathlib import Path

import requests

from geoseeq.constants import FIVE_MB
from geoseeq.utils import md5_checksum
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import *


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
    
    def _upload_one_part(self, file_chunker, url, num, max_retries, session=None):
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
        return {"ETag": http_response.headers["ETag"], "PartNumber": num + 1}
    
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

    def _upload_parts(self, file_chunker, urls, max_retries, session, progress_tracker, threads):
        if threads == 1:
            logger.info(f"Uploading parts in series for {file_chunker.filepath}")
            complete_parts = []
            for num, url in enumerate(list(urls.values())):
                response_part = self._upload_one_part(file_chunker, url, num, max_retries, session)
                complete_parts.append(response_part)
                if progress_tracker: progress_tracker.update(file_chunker.get_chunk_size(num))
                logger.info(f'Uploaded part {num + 1} of {len(urls)} for "{file_chunker.filepath}"')
            return complete_parts
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            logger.info(f"Uploading parts in parallel for {file_chunker.filepath} with {threads} threads.")
            futures = []
            for num, url in enumerate(list(urls.values())):
                future = executor.submit(
                    self._upload_one_part, file_chunker, url, num, max_retries, session
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
    ):
        """Upload a file to S3 using the multipart upload process."""
        logger.info(f"Uploading {filepath} to S3 using multipart upload.")
        upload_id, urls = self._prep_multipart_upload(filepath, file_size, chunk_size, optional_fields)
        logger.info(f'Starting upload for "{filepath}"')
        complete_parts = []
        file_chunker = FileChunker(filepath, chunk_size).load_all_chunks()
        if progress_tracker: progress_tracker.set_num_chunks(file_chunker.file_size)
        complete_parts = self._upload_parts(file_chunker, urls, max_retries, session, progress_tracker, threads)
        self._finish_multipart_upload(upload_id, complete_parts)
        logger.info(f'Finished Upload for "{filepath}"')
        return self

    def upload_file(self, filepath, multipart_thresh=FIVE_MB, **kwargs):
        resolved_path = Path(filepath).resolve()
        file_size = getsize(resolved_path)
        return self.multipart_upload_file(filepath, file_size, **kwargs)
    
    def upload_json(self, data, **kwargs):
        """Upload a file with the given data as JSON."""
        with NamedTemporaryFile("w", suffix='.json') as f:
            json.dump(data, f)
            f.flush()
            return self.upload_file(f.name, **kwargs)
