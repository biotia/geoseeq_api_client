
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


class ResumableUploadTracker:

    def __init__(self, filepath, chunk_size, upload_target_uuid, tracker_file_prefix="gs_resumable_upload_tracker"):
        self.open, self.upload_started = True, False
        self.upload_id, self.urls, self.is_atomic_upload = None, None, None
        self.upload_target_uuid = upload_target_uuid
        self.filepath = filepath
        self.tracker_file_dir = join(GEOSEEQ_CACHE_DIR, 'upload')
        self.tracker_file = join(
            self.tracker_file_dir,
            tracker_file_prefix + f".{upload_target_uuid}.{chunk_size}.{getsize(filepath)}." + basename(filepath)
        )
        try:
            os.makedirs(self.tracker_file_dir, exist_ok=True)
        except Exception as e:
            logger.warning(f'Could not create resumable upload tracker directory. {e}')
            self.open = False
        self._loaded_parts = {}
        self._load_parts_from_file()

    def start_upload(self, upload_id, urls, is_atomic_upload=False):
        if not self.open:
            return
        if self.upload_started:
            raise GeoseeqGeneralError("Upload has already started.")
        self.upload_started = True
        blob = dict(upload_id=upload_id,
                    urls=urls,
                    is_atomic_upload=is_atomic_upload,
                    upload_target_uuid=self.upload_target_uuid,
                    start_time=time.time())
        serialized = json.dumps(blob)
        with open(self.tracker_file, "w") as f:
            f.write(serialized + "\n")
        self.upload_id, self.urls, self.is_atomic_upload = upload_id, urls, is_atomic_upload

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
            self.upload_id, self.urls, self.is_atomic_upload = (
                header_blob["upload_id"], header_blob["urls"], header_blob["is_atomic_upload"]
            )
            start_time = header_blob["start_time"]
            if (time.time() - start_time) > (60 * 60 * 23):
                logger.warning(f"Tracker file {self.tracker_file} is too old. Deleting.")
                os.remove(self.tracker_file)
                return
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
