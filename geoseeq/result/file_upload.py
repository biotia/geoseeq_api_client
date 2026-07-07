
import time
import json
import os
import base64
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

# Synthetic upload_id the server returns for Azure-backed projects (design D5).
AZURE_UPLOAD_ID = "azure"


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
            resumable_upload_tracker.add_part(blob)  # add_part is internally locked
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

    @staticmethod
    def _azure_block_id(num):
        """Return a base64 block id; Azure requires equal-length base64 ids per blob."""
        return base64.b64encode(f"{num:08d}".encode()).decode()

    @staticmethod
    def _is_transient_upload_error(exc):
        """Return True only for genuinely transient block-staging errors worth retrying.

        Retries connection/SSL/timeout failures and 408/429/5xx HTTP responses. Permanent
        failures (auth, expired/invalid SAS, not-found, other 4xx) return False so the caller
        fails fast instead of burning minutes on retries that will never succeed. azure.core is
        imported lazily so classification still works when only azure.storage.blob is installed.
        """
        if isinstance(exc, (
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
            requests.exceptions.Timeout,  # covers ReadTimeout and ConnectTimeout
        )):
            return True
        if isinstance(exc, requests.exceptions.HTTPError):
            response = exc.response
            if response is None:
                return False
            return response.status_code in (408, 429) or response.status_code >= 500
        try:
            from azure.core.exceptions import (
                HttpResponseError,
                ServiceRequestError,
                ServiceResponseError,
            )
        except ImportError:
            return False
        if isinstance(exc, (ServiceRequestError, ServiceResponseError)):
            return True
        if isinstance(exc, HttpResponseError):
            status = exc.status_code
            return status in (408, 429) or bool(status and status >= 500)
        return False  # bare AzureError (and anything else) is treated as permanent

    def _azure_upload_file(self, filepath, file_size, sas_url, chunk_size,
                           progress_tracker=None, atomic=True, threads=1, max_retries=3,
                           resumable_upload_tracker=None):
        """Stage blocks to Azure via a write SAS URL, commit the block list, then register the field.

        The server mints one write SAS (design D5); the client stages the block list itself and the
        complete endpoint verifies the blob exists (AZU-03) rather than presigning a completion URL.
        Blocks stage in parallel (bounded by ``threads``) with per-block retry (``max_retries``).

        When ``resumable_upload_tracker`` is provided, block indices already staged in a prior run
        are skipped (their blocks remain uncommitted on the blob under the reused SAS) and newly
        staged indices are recorded so a later interruption can resume again. The tracker is cleaned
        up only after the block list commits and the field is registered.
        """
        if threads < 1:
            raise ValueError(f"threads must be >= 1 to stage Azure blocks, got {threads}.")
        if max_retries < 1:
            # max_retries < 1 would skip the staging loop entirely, treating a block as
            # "staged" without ever calling stage_block and committing an empty/partial blob.
            raise ValueError(f"max_retries must be >= 1 to stage Azure blocks, got {max_retries}.")
        try:
            from azure.storage.blob import BlobClient, BlobBlock
        except ImportError:
            raise GeoseeqGeneralError(
                'azure-storage-blob is required to upload to Azure-backed projects. '
                'Install the optional extra: pip install "geoseeq[azure]".'
            )
        logger.info(f"Uploading {filepath} to Azure block blob.")
        blob_client = BlobClient.from_blob_url(sas_url)
        if progress_tracker:
            progress_tracker.set_num_chunks(file_size)
        if file_size <= chunk_size:
            # Small file: one PUT, no block staging needed.
            with open(filepath, "rb") as f:
                blob_client.upload_blob(f, length=file_size, overwrite=True)
            if progress_tracker:
                progress_tracker.update(file_size)
        else:
            self._azure_stage_blocks(
                blob_client, BlobBlock, filepath, chunk_size,
                progress_tracker=progress_tracker, threads=threads, max_retries=max_retries,
                resumable_upload_tracker=resumable_upload_tracker,
            )
        self._finish_multipart_upload(AZURE_UPLOAD_ID, [], atomic=atomic)
        # Commit + field registration both succeeded, so the resume state is no longer needed.
        if resumable_upload_tracker:
            resumable_upload_tracker.cleanup()
        logger.info(f'Finished Azure upload for "{filepath}"')
        if atomic:
            # The field may not have existed on the server before this atomic upload.
            self.get()
        return self

    def _azure_stage_one_block(self, blob_client, file_chunker, num, block_id, max_retries):
        """Stage a single Azure block, retrying transient errors with exponential backoff.

        Mirrors ``_upload_one_part``: transient failures back off and retry; permanent failures
        (auth, expired/invalid SAS, not-found, other 4xx) re-raise immediately so the caller aborts
        before committing a partial block list.
        """
        chunk = file_chunker.get_chunk(num)
        attempts = 0
        while attempts < max_retries:
            try:
                blob_client.stage_block(block_id, chunk)
                return
            except Exception as e:
                if not self._is_transient_upload_error(e):
                    raise  # permanent failure -> fail fast rather than retry
                attempts += 1
                logger.debug(
                    f"Staging block {num + 1} failed. Attempt {attempts} of {max_retries}. Error: {e}"
                )
                if attempts >= max_retries:
                    raise
                retry_time = min(8 ** attempts, 120)  # exponential backoff, max 120s
                retry_time *= 0.6 + (random() * 0.8)  # randomize to avoid thundering herd
                logger.debug(f"Retrying staging of block {num + 1} in {retry_time} seconds.")
                time.sleep(retry_time)

    def _azure_stage_blocks(self, blob_client, blob_block_cls, filepath, chunk_size,
                            progress_tracker=None, threads=1, max_retries=3,
                            resumable_upload_tracker=None):
        """Stage each file chunk as an Azure block (in parallel) and commit the ordered block list.

        Block ids are index-derived and built up front so the committed block list is in ascending
        index order regardless of the order blocks finish staging. FileChunker.n_parts includes a
        trailing empty part when file_size is an exact multiple of chunk_size; that part is excluded
        via the ceil-division block count.

        Azure block ids are deterministic (``_azure_block_id(num)``), so the committed list is fully
        determined by the block count and is independent of which blocks were staged in this run.
        The tracker therefore only records which block indices were already staged; a resumed run
        skips re-staging those (their uncommitted blocks persist on the blob for ~7 days under the
        reused SAS) and the full ordered id list is committed regardless. Note: if a resume happens
        after Azure has garbage-collected the uncommitted blocks (>7 days), commit_block_list will
        fail — acceptable for now (the tracker's own freshness check already expires it well before).
        """
        file_chunker = FileChunker(filepath, chunk_size)
        n_blocks = -(-file_chunker.file_size // chunk_size)  # ceil division; trailing empty part excluded
        blocks = [(num, self._azure_block_id(num)) for num in range(n_blocks)]

        def _stage(num, block_id):
            """Stage one block unless the tracker shows this index was already staged; then record it.

            Part numbers are 1-based to match the S3 tracker convention (block index ``num`` -> part
            ``num + 1``). Runs on worker threads when ``threads > 1``; the tracker's add_part is locked.
            """
            if resumable_upload_tracker and resumable_upload_tracker.part_has_been_uploaded(num + 1):
                logger.info(f'Block {num + 1} already staged in a previous run for "{filepath}". Skipping.')
                return
            self._azure_stage_one_block(blob_client, file_chunker, num, block_id, max_retries)
            if resumable_upload_tracker:
                resumable_upload_tracker.add_part({"PartNumber": num + 1})

        def _record_staged(num):
            """Advance progress and log a completed block (called from the main thread only)."""
            if progress_tracker:
                progress_tracker.update(file_chunker.get_chunk_size(num))
            logger.info(f'Staged block {num + 1} of {n_blocks} for "{filepath}"')

        if threads == 1:
            for num, block_id in blocks:
                _stage(num, block_id)
                _record_staged(num)
        else:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = {
                    executor.submit(_stage, num, block_id): num
                    for num, block_id in blocks
                }
                for future in as_completed(futures):
                    future.result()  # re-raise staging failures before committing anything
                    _record_staged(futures[future])

        # Commit in ascending index order even though blocks may have staged out of order.
        block_list = [blob_block_cls(block_id=block_id) for _, block_id in blocks]
        blob_client.commit_block_list(block_list)

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
        """Upload a file using the multipart upload process (S3), or block staging for Azure-backed projects."""
        logger.info(f"Starting multipart/atomic upload for {filepath}.")
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
            # Persist the upload for both S3 (part urls) and Azure (single write SAS). For
            # Azure this records the SAS so a resumed run reuses the same blob (with its
            # already-staged, still-uncommitted blocks) instead of minting a fresh SAS.
            if resumable_upload_tracker:
                logger.info(f'Creating new resumable upload for "{filepath}", upload_id: "{upload_id}"')
                resumable_upload_tracker.start_upload(upload_id, urls, is_atomic_upload=use_atomic_upload)

        if upload_id == AZURE_UPLOAD_ID:
            # Azure-backed project: the server returned one write SAS URL (design D5).
            return self._azure_upload_file(
                filepath, file_size, urls, chunk_size,
                progress_tracker=progress_tracker, atomic=use_atomic_upload,
                threads=threads, max_retries=max_retries,
                resumable_upload_tracker=resumable_upload_tracker,
            )

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
