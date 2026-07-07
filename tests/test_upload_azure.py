import base64
import glob
import json
import os
import threading
import time
import sys
import types
from os.path import join
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

from geoseeq.constants import FIVE_MB
from geoseeq.knex import GeoseeqGeneralError
from geoseeq.result.file_upload import AZURE_UPLOAD_ID, ResultFileUpload
from geoseeq.result.resumable_upload_tracker import ResumableUploadTracker

SAS_URL = "https://acct.blob.core.windows.net/container/prefix/reads.fastq.gz?sig=abc"

# multipart_upload_file only builds a ResumableUploadTracker for files > 10*FIVE_MB. We pass a
# large file_size argument (the real on-disk file stays tiny so the tests are fast) to cross that
# threshold, and an explicit small chunk_size so the tiny file still splits into several blocks.
BIG_FILE_SIZE = 10 * FIVE_MB + 1


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


# --- Resumable Azure upload tests -------------------------------------------------------------
#
# These drive multipart_upload_file (not just _azure_upload_file) so the ResumableUploadTracker
# creation/consultation path is exercised end to end. GEOSEEQ_CACHE_DIR is redirected to a temp
# dir so the tracker file is isolated per test.


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    """Point the resumable upload tracker's cache dir at a temp location for the test."""
    cache_dir = tmp_path / "cache"
    monkeypatch.setattr(
        "geoseeq.result.resumable_upload_tracker.GEOSEEQ_CACHE_DIR", str(cache_dir)
    )
    return cache_dir


def _tracker_dir(cache_dir):
    """Directory the tracker writes its per-upload state files into."""
    return join(str(cache_dir), "upload")


def _tracker_files(cache_dir):
    """All tracker state files currently on disk (empty list once cleaned up)."""
    return glob.glob(join(_tracker_dir(cache_dir), "gs_resumable_upload_tracker*"))


class _ResumableFake(ResultFileUpload):
    """ResultFileUpload host that stubs the network seams so multipart_upload_file runs offline.

    _prep_multipart_upload stands in for the server minting a write SAS; counting its calls lets a
    test prove a resumed run reuses the persisted SAS instead of minting a new one.
    """

    def __init__(self, uuid="field-uuid", parent_uuid="parent-uuid"):
        self.uuid = uuid
        self.parent = types.SimpleNamespace(uuid=parent_uuid)
        self.prep_calls = 0
        self.finish_calls = []
        self.got = False

    def _prep_multipart_upload(self, filepath, file_size, chunk_size, optional_fields, atomic=False):
        self.prep_calls += 1
        return AZURE_UPLOAD_ID, SAS_URL

    def _finish_multipart_upload(self, upload_id, complete_parts, atomic=False):
        self.finish_calls.append((upload_id, complete_parts, atomic))

    def get(self):
        self.got = True
        return self


def _fail_indices_stage(fail_indices):
    """stage_block side effect that raises a transient error for the given block indices."""
    def stage(block_id, chunk):
        if _block_index(block_id) in fail_indices:
            raise requests.exceptions.ConnectionError(f"block {_block_index(block_id)} failed")
    return stage


def test_azure_upload_resumes_and_reuses_sas(tmp_path, isolated_cache):
    """An interrupted large Azure upload resumes: already-staged blocks are not re-staged, the SAS
    is reused (no second create_atomic_upload_urls), and the full ordered block list is committed."""
    payload = b"x" * 20  # 20 bytes, chunk_size 4 -> 5 blocks (parts 1..5)
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    # Run 1: blocks 0 and 1 stage, block 2 fails -> upload aborts before committing.
    run1_client = MagicMock()
    run1_client.stage_block.side_effect = _fail_indices_stage({2})
    rf1 = _ResumableFake()
    with patch("geoseeq.result.file_upload.time.sleep"):
        with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=run1_client):
            with pytest.raises(requests.exceptions.ConnectionError):
                rf1.multipart_upload_file(
                    str(filepath), BIG_FILE_SIZE, chunk_size=4, threads=1, max_retries=1,
                )
    run1_client.commit_block_list.assert_not_called()
    assert rf1.prep_calls == 1  # SAS minted once on the first run
    assert len(_tracker_files(isolated_cache)) == 1  # resume state persisted

    # Run 2: everything stages fine. Blocks 0 and 1 were recorded in run 1, so they are skipped.
    run2_client = MagicMock()  # no side effect -> all stage_block calls succeed
    rf2 = _ResumableFake()  # same uuids -> resolves to the same tracker file on disk
    with patch("geoseeq.result.file_upload.time.sleep"):
        with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=run2_client):
            rf2.multipart_upload_file(
                str(filepath), BIG_FILE_SIZE, chunk_size=4, threads=1, max_retries=1,
            )

    # SAS was reused: the resumed run never re-minted (prep) a new upload/SAS.
    assert rf2.prep_calls == 0
    # Only the not-yet-staged blocks (indices 2, 3, 4) were re-staged; 0 and 1 were skipped.
    staged_indices = {_block_index(c.args[0]) for c in run2_client.stage_block.call_args_list}
    assert staged_indices == {2, 3, 4}
    # The committed block list is still the full ordered set of all 5 blocks.
    committed = run2_client.commit_block_list.call_args.args[0]
    assert [_block_index(b.id) for b in committed] == [0, 1, 2, 3, 4]
    # is_atomic_upload (False, the multipart_upload_file default) round-tripped through the tracker.
    assert rf2.finish_calls == [(AZURE_UPLOAD_ID, [], False)]


def test_azure_upload_tracker_cleaned_up_after_success(tmp_path, isolated_cache):
    """After a successful large Azure upload the tracker state file is removed."""
    payload = b"x" * 20  # 5 blocks
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    rf = _ResumableFake()
    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        rf.multipart_upload_file(str(filepath), BIG_FILE_SIZE, chunk_size=4, threads=1)

    blob_client.commit_block_list.assert_called_once()
    assert _tracker_files(isolated_cache) == []  # cleaned up on success


def test_azure_upload_no_tracker_for_small_file(tmp_path, isolated_cache):
    """A file at/below the resumable threshold uploads without ever creating a tracker file."""
    payload = b"x" * 20
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    rf = _ResumableFake()
    # file_size below the 10*FIVE_MB threshold -> no tracker created.
    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        rf.multipart_upload_file(str(filepath), len(payload), chunk_size=4, threads=1)

    blob_client.commit_block_list.assert_called_once()
    assert not os.path.isdir(_tracker_dir(isolated_cache))


def test_azure_upload_no_tracker_when_cache_disabled(tmp_path, isolated_cache):
    """use_cache=False disables the resumable tracker even for a large file."""
    payload = b"x" * 20
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    rf = _ResumableFake()
    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        rf.multipart_upload_file(
            str(filepath), BIG_FILE_SIZE, chunk_size=4, threads=1, use_cache=False,
        )

    blob_client.commit_block_list.assert_called_once()
    assert not os.path.isdir(_tracker_dir(isolated_cache))


def test_azure_upload_parallel_records_all_blocks_under_lock(tmp_path, isolated_cache):
    """Parallel staging records every staged block in the tracker without corrupting the file.

    One block is made to fail so the commit is skipped and the tracker file survives for inspection;
    the remaining blocks all stage concurrently and every line in the tracker must be valid JSON
    (proving the add_part lock serialized the concurrent appends)."""
    payload = b"x" * 40  # 40 bytes, chunk_size 4 -> 10 blocks (parts 1..10)
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    blob_client.stage_block.side_effect = _fail_indices_stage({4})  # part 5 fails
    rf = _ResumableFake()
    with patch("geoseeq.result.file_upload.time.sleep"):
        with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
            with pytest.raises(requests.exceptions.ConnectionError):
                rf.multipart_upload_file(
                    str(filepath), BIG_FILE_SIZE, chunk_size=4, threads=5, max_retries=1,
                )

    blob_client.commit_block_list.assert_not_called()
    tracker_files = _tracker_files(isolated_cache)
    assert len(tracker_files) == 1
    with open(tracker_files[0]) as f:
        lines = [line for line in f.read().splitlines() if line]
    # Header line + one line per successfully staged block; every line parses cleanly (no
    # interleaved/corrupted writes from the concurrent stagers).
    parsed = [json.loads(line) for line in lines]
    recorded_parts = {blob["PartNumber"] for blob in parsed if "PartNumber" in blob}
    assert recorded_parts == {1, 2, 3, 4, 6, 7, 8, 9, 10}  # every block except the failed part 5


def test_azure_upload_parallel_raises_and_skips_commit_on_persistent_failure(tmp_path):
    """In parallel mode, a block that always fails propagates the exception and never commits."""
    payload = b"0123456789"  # 3 blocks
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(payload)

    blob_client = MagicMock()
    blob_client.stage_block.side_effect = requests.exceptions.ConnectionError("always fails")
    rf = _FakeResultFile()

    with patch("geoseeq.result.file_upload.time.sleep"):
        with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
            with pytest.raises(requests.exceptions.ConnectionError):
                rf._azure_upload_file(str(filepath), len(payload), SAS_URL, 4, threads=3, max_retries=2)

    blob_client.commit_block_list.assert_not_called()
    assert rf.finish_calls == []


def test_add_part_after_cleanup_is_safe_noop(tmp_path, isolated_cache):
    """After cleanup() closes the tracker, a late add_part (e.g. a thread that was blocked on the
    lock while another thread cleaned up) must be a no-op: it must NOT re-create the deleted file
    with a headerless part line, which would crash the next run's _load_parts_from_file."""
    filepath = tmp_path / "reads.fastq.gz"
    filepath.write_bytes(b"x" * 20)

    tracker = ResumableUploadTracker(str(filepath), 4, "target-uuid")
    tracker.start_upload(AZURE_UPLOAD_ID, SAS_URL, is_atomic_upload=False)
    tracker.add_part({"PartNumber": 1})
    tracker.cleanup()
    assert tracker.open is False  # cleanup() closes the tracker
    assert _tracker_files(isolated_cache) == []  # file deleted

    # A late add_part after cleanup must not resurrect the tracker file.
    tracker.add_part({"PartNumber": 2})
    assert _tracker_files(isolated_cache) == []

    # A fresh tracker over the same file loads cleanly (no headerless-line KeyError crash).
    reloaded = ResumableUploadTracker(str(filepath), 4, "target-uuid")
    assert reloaded.upload_started is False
