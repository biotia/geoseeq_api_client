
from os.path import getsize
import logging

logger = logging.getLogger("geoseeq_api")  # Same name as calling module
logger.addHandler(logging.NullHandler())


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
    
    def chunk_is_preloaded(self, num):
        return len(self.loaded_parts) > num and self.loaded_parts[num]
    
    def read_one_chunk(self, num):
        if not self.chunk_is_preloaded(num):
            logger.debug(f"Reading chunk {num} from {self.filepath}")
            with open(self.filepath, "rb") as f:
                f.seek(num * self.chunk_size)
                chunk = f.read(self.chunk_size)
                return chunk
        return self.loaded_parts[num]

    def get_chunk(self, num):
        if self.chunk_is_preloaded(num):
            return self.loaded_parts[num]
        return self.read_one_chunk(num)
    
    def get_chunk_size(self, num):
        if num < (self.n_parts - 1):  # all but the last chunk
            return self.chunk_size
        if self.chunk_is_preloaded(num):  # last chunk, pre-loaded
            return len(self.loaded_parts[num])
        return len(self.read_one_chunk(num))  # last chunk, not pre-loaded
    
