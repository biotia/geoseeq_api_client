import base64
import threading
import time
import sys
import types
from unittest.mock import MagicMock, patch

import pytest
import requests

# Stub azure.storage.blob so tests run without the optional [azure] extra installed.
# Another test module (test_file_download_azure) may have already inserted a partial
# stub exposing only BlobClient, so force-attach BOTH BlobClient and BlobBlock rather
# than skipping when the module is merely present. When the real SDK is installed (CI
# installs the [test] extra, which pulls azure-storage-blob) this leaves it untouched.
_BlobBlockStub = type("BlobBlock", (), {"__init__": lambda self, block_id=None: setattr(self, "id", block_id)})
try:
    import azure.storage.blob  # noqa: F401

    _blob_mod = sys.modules["azure.storage.blob"]
    if not hasattr(_blob_mod, "BlobClient"):
        _blob_mod.BlobClient = types.SimpleNamespace(from_blob_url=lambda url: None)
    if not hasattr(_blob_mod, "BlobBlock"):
        _blob_mod.BlobBlock = _BlobBlockStub
except ImportError:
    _azure = sys.modules.get("azure") or types.ModuleType("azure")
    _azure_storage = types.ModuleType("azure.storage")
    _azure_blob = types.ModuleType("azure.storage.blob")
    _azure_blob.BlobClient = types.SimpleNamespace(from_blob_url=lambda url: None)
    _azure_blob.BlobBlock = _BlobBlockStub
    _azure_storage.blob = _azure_blob
    _azure.storage = _azure_storage
    sys.modules["azure"] = _azure
    sys.modules["azure.storage"] = _azure_storage
    sys.modules["azure.storage.blob"] = _azure_blob

from geoseeq.knex import GeoseeqGeneralError
from geoseeq.result.file_upload import AZURE_UPLOAD_ID, ResultFileUpload

SAS_URL = "https://acct.blob.core.windows.net/container/prefix/reads.fastq.gz?sig=abc"


class _FakeResultFile(ResultFileUpload):
    """Minimal ResultFileUpload host that records the finish call and skips the network get()."""

    def __init__(self):
        self.finish_calls = []
        self.got = False

    def _finish_multipart_upload(self, upload_id, complete_parts, atomic=False):
        self.finish_calls.append((upload_id, complete_parts, atomic))

    def get(self):
        self.got = True
        return self


def test_azure_upload_stages_blocks_and_commits(tmp_path):
    """A file larger than one chunk stages each block, commits the block list, then registers the field."""
    payload = b"0123456789"  # 10 bytes, chunk_size 4 -> 3 blocks (4, 4, 2)
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    progress = MagicMock()
    rf = _FakeResultFile()

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client) as from_url:
        result = rf._azure_upload_file(str(filepath), len(payload), SAS_URL, 4, progress_tracker=progress)

    from_url.assert_called_once_with(SAS_URL)
    assert blob_client.stage_block.call_count == 3
    blob_client.upload_blob.assert_not_called()
    blob_client.commit_block_list.assert_called_once()
    committed_blocks = blob_client.commit_block_list.call_args.args[0]
    assert len(committed_blocks) == 3
    # Block ids passed to stage_block must be equal-length so the committed list is valid.
    staged_ids = [call.args[0] for call in blob_client.stage_block.call_args_list]
    assert len({len(i) for i in staged_ids}) == 1
    assert staged_ids == sorted(staged_ids)  # ordered by chunk index
    assert rf.finish_calls == [(AZURE_UPLOAD_ID, [], True)]
    assert rf.got is True
    progress.set_num_chunks.assert_called_once_with(len(payload))
    assert result is rf


def test_azure_upload_exact_multiple_block_count(tmp_path, caplog):
    """When file_size is an exact multiple of chunk_size, the trailing empty part is not staged
    and the logged block count is accurate (not off by one from FileChunker.n_parts)."""
    import logging

    payload = b"01234567"  # 8 bytes, chunk_size 4 -> exactly 2 blocks (n_parts would be 3)
    filepath = tmp_path / "exact.bin"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    rf = _FakeResultFile()

    with caplog.at_level(logging.INFO, logger="geoseeq_api"):
        with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
            rf._azure_upload_file(str(filepath), len(payload), SAS_URL, 4)

    assert blob_client.stage_block.call_count == 2
    assert len(blob_client.commit_block_list.call_args.args[0]) == 2
    assert "Staged block 2 of 2" in caplog.text
    assert "of 3" not in caplog.text


def test_azure_upload_small_file_single_put(tmp_path):
    """A file that fits in one chunk uploads with a single PUT and no block staging."""
    payload = b"tiny"  # 4 bytes, chunk_size 8 -> single PUT
    filepath = tmp_path / "small.txt"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    rf = _FakeResultFile()

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        rf._azure_upload_file(str(filepath), len(payload), SAS_URL, 8)

    blob_client.upload_blob.assert_called_once()
    blob_client.stage_block.assert_not_called()
    blob_client.commit_block_list.assert_not_called()
    assert rf.finish_calls == [(AZURE_UPLOAD_ID, [], True)]


def test_azure_upload_missing_extra_raises(tmp_path):
    """When azure-storage-blob is not importable, a clear GeoseeqGeneralError is raised."""
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(b"data")
    rf = _FakeResultFile()

    # Setting the module to None makes `from azure.storage.blob import ...` raise ImportError.
    with patch.dict(sys.modules, {"azure.storage.blob": None}):
        with pytest.raises(GeoseeqGeneralError, match="azure-storage-blob is required"):
            rf._azure_upload_file(str(filepath), 4, SAS_URL, 8)


def test_azure_block_id_is_base64_and_fixed_length():
    """Block ids are base64 and equal-length across sequential indices."""
    ids = [ResultFileUpload._azure_block_id(n) for n in (0, 1, 42, 9999)]
    assert len({len(i) for i in ids}) == 1
    assert base64.b64decode(ids[0]) == b"00000000"


def _block_index(block_id):
    """Recover the integer chunk index encoded in an Azure block id."""
    return int(base64.b64decode(block_id).decode())


class _ConcurrencyTracker:
    """Records max concurrent stage_block calls and their completion order.

    Earlier blocks are made to sleep longer so completion order is reversed from
    submission order — this proves the committed block list is ordered by index,
    not by staging completion.
    """

    def __init__(self, n_blocks, unit_delay=0.02):
        self._lock = threading.Lock()
        self._n_blocks = n_blocks
        self._unit_delay = unit_delay
        self.current = 0
        self.max_in_flight = 0
        self.completion_order = []

    def stage(self, block_id, chunk):
        with self._lock:
            self.current += 1
            self.max_in_flight = max(self.max_in_flight, self.current)
        # Earlier indices sleep longer -> finish later than later indices.
        idx = _block_index(block_id)
        time.sleep((self._n_blocks - idx) * self._unit_delay)
        with self._lock:
            self.current -= 1
            self.completion_order.append(idx)


def test_azure_upload_parallel_staging_bounded_and_committed_in_order(tmp_path):
    """threads>1 stages blocks concurrently (bounded by threads) yet commits in ascending index order."""
    n_blocks = 5
    chunk_size = 4
    payload = b"x" * (n_blocks * chunk_size)  # 20 bytes -> 5 blocks
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    threads = 3
    tracker = _ConcurrencyTracker(n_blocks)
    blob_client = MagicMock()
    blob_client.stage_block.side_effect = tracker.stage
    rf = _FakeResultFile()

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        rf._azure_upload_file(str(filepath), len(payload), SAS_URL, chunk_size, threads=threads)

    assert blob_client.stage_block.call_count == n_blocks
    # Concurrency was real (more than one in flight) but never exceeded the thread bound.
    assert tracker.max_in_flight > 1
    assert tracker.max_in_flight <= threads
    # Blocks finished out of submission order (earlier indices slept longer)...
    assert tracker.completion_order != sorted(tracker.completion_order)
    # ...yet the committed block list is strictly ascending by index.
    committed = blob_client.commit_block_list.call_args.args[0]
    committed_indices = [_block_index(b.id) for b in committed]
    assert committed_indices == list(range(n_blocks))


def test_azure_upload_serial_threads_one(tmp_path):
    """threads==1 keeps the simple serial path and still commits every block in order."""
    payload = b"0123456789"  # 10 bytes, chunk_size 4 -> 3 blocks
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    rf = _FakeResultFile()

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        rf._azure_upload_file(str(filepath), len(payload), SAS_URL, 4, threads=1)

    assert blob_client.stage_block.call_count == 3
    committed = blob_client.commit_block_list.call_args.args[0]
    assert [_block_index(b.id) for b in committed] == [0, 1, 2]


def test_azure_upload_retries_transient_stage_error(tmp_path):
    """A block that raises a transient error once then succeeds still completes the upload."""
    payload = b"0123456789"  # 3 blocks
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    calls = {"n": 0}

    def flaky_stage(block_id, chunk):
        # Fail the very first stage attempt only, then succeed for everything.
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.exceptions.ConnectionError("transient")

    blob_client = MagicMock()
    blob_client.stage_block.side_effect = flaky_stage
    rf = _FakeResultFile()

    with patch("geoseeq.result.file_upload.time.sleep"):  # skip backoff sleep
        with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
            rf._azure_upload_file(str(filepath), len(payload), SAS_URL, 4, threads=1, max_retries=3)

    # 3 blocks + 1 retry for the first failure = 4 stage_block calls.
    assert blob_client.stage_block.call_count == 4
    blob_client.commit_block_list.assert_called_once()
    assert rf.finish_calls == [(AZURE_UPLOAD_ID, [], True)]


def test_azure_upload_retries_read_timeout_stage_error(tmp_path):
    """A block that raises a requests Timeout once then succeeds still completes the upload.

    Timeouts (ReadTimeout/ConnectTimeout) are common transient failures on large block
    uploads and must be retried rather than propagating on the first occurrence.
    """
    payload = b"0123456789"  # 3 blocks
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    calls = {"n": 0}

    def flaky_stage(block_id, chunk):
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.exceptions.ReadTimeout("read timed out")

    blob_client = MagicMock()
    blob_client.stage_block.side_effect = flaky_stage
    rf = _FakeResultFile()

    with patch("geoseeq.result.file_upload.time.sleep"):  # skip backoff sleep
        with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
            rf._azure_upload_file(str(filepath), len(payload), SAS_URL, 4, threads=1, max_retries=3)

    # 3 blocks + 1 retry for the first timeout = 4 stage_block calls.
    assert blob_client.stage_block.call_count == 4
    blob_client.commit_block_list.assert_called_once()
    assert rf.finish_calls == [(AZURE_UPLOAD_ID, [], True)]


def test_azure_upload_raises_and_skips_commit_on_persistent_failure(tmp_path):
    """A block that always fails exhausts retries, raises, and never commits a partial block list."""
    payload = b"0123456789"  # 3 blocks
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    blob_client.stage_block.side_effect = requests.exceptions.ConnectionError("always fails")
    rf = _FakeResultFile()

    with patch("geoseeq.result.file_upload.time.sleep"):
        with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
            with pytest.raises(requests.exceptions.ConnectionError):
                rf._azure_upload_file(str(filepath), len(payload), SAS_URL, 4, threads=1, max_retries=2)

    blob_client.commit_block_list.assert_not_called()
    assert rf.finish_calls == []
