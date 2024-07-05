
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



class ResumableDownloadTracker:

    def __init__(self, chunk_size, download_target_id, target_local_path, tracker_file_prefix="gs_resumable_download_tracker"):
        self.open, self.download_started = True, False
        self.download_target_id = download_target_id
        self.target_local_path = target_local_path
        self.tracker_file_dir = join(GEOSEEQ_CACHE_DIR, 'download')
        self.tracker_file = join(
            self.tracker_file_dir,
            tracker_file_prefix + f".{download_target_id}.{chunk_size}." + basename(target_local_path)
        )
        try:
            os.makedirs(self.tracker_file_dir, exist_ok=True)
        except Exception as e:
            logger.warning(f'Could not create resumable download tracker directory. {e}')
            self.open = False
        self._loaded_parts = {}
        self._load_parts_from_file()

    def start_download(self, download_url):
        if not self.open:
            return
        if self.download_started:
            raise GeoseeqGeneralError("Download has already started.")
        self.download_started = True
        blob = dict(download_url=download_url,
                    download_target_id=self.download_target_id,
                    start_time=time.time())
        serialized = json.dumps(blob)
        with open(self.tracker_file, "w") as f:
            f.write(serialized + "\n")
        self.download_url = download_url
        return self
    
    def add_part(self, part_download_info):
        if not self.open:
            assert False, "Cannot add part to closed ResumableDownloadTracker"
        part_id = part_download_info["part_number"]
        serialized = json.dumps(part_download_info)
        with open(self.tracker_file, "a") as f:
            f.write(serialized + "\n")
        self._loaded_parts[part_id] = part_download_info

    def _load_parts_from_file(self):
        if not isfile(self.tracker_file):
            return
        with open(self.tracker_file, "r") as f:
            header_blob = json.loads(f.readline())
            self.download_url = header_blob["download_url"]
            start_time = header_blob["start_time"]  # for now we don't expire resumable downloads
            self.download_started = True
            for line in f:
                part_info = json.loads(line)
                part_id = part_info["part_number"]
                self._loaded_parts[part_id] = part_info

    def part_has_been_downloaded(self, part_number):
        if not self.open:
            return False
        if part_number not in self._loaded_parts:
            return False
        part_info = self._loaded_parts[part_number]
        part_path = part_info["part_filename"]
        return isfile(part_path)
    
    def get_part_info(self, part_number):
        if not self.open:
            return None
        return self._loaded_parts.get(part_number, None)
    
    def cleanup(self):
        if not self.open:
            return
        for part in self._loaded_parts.values():
            part_path = part["part_filename"]
            if isfile(part_path):
                os.remove(part_path)
        os.remove(self.tracker_file)
        self.open = False
    
