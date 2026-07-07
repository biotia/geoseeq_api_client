"""Unit tests for resumable azure downloads in geoseeq.result.file_download.

These mock the azure-storage-blob SDK (BlobClient.download_blob / get_blob_properties)
so they run without the optional [azure] extra installed and without any network.
"""
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# Stub azure.storage.blob so tests run without the optional [azure] extra installed.
# `azure` is a namespace package shared by many azure-* libs, so it may already be in
# sys.modules without `.storage.blob`; always force-attach the stub submodules.
try:
    import azure.storage.blob  # noqa: F401
except ImportError:
    _azure = sys.modules.get("azure") or types.ModuleType("azure")
    _azure_storage = types.ModuleType("azure.storage")
    _azure_blob = types.ModuleType("azure.storage.blob")
    _azure_blob.BlobClient = types.SimpleNamespace(from_blob_url=lambda url: None)
    _azure_storage.blob = _azure_blob
    _azure.storage = _azure_storage
    sys.modules["azure"] = _azure
    sys.modules["azure.storage"] = _azure_storage
    sys.modules["azure.storage.blob"] = _azure_blob

from geoseeq.result import file_download, resumable_download_tracker
from geoseeq.result.file_download import (
    _download_azure_resumable,
    _download_azure_sdk,
    download_url,
)

URL = "https://example.blob.core.windows.net/c/bigblob?sig=x"


class FakeAzureBackend:
    """A fake azure blob backed by an in-memory payload.

    Serves ranged ``download_blob(offset, length)`` reads against ``payload`` and records
    every call so tests can assert which byte ranges (parts) were fetched. Parts whose index
    is in ``fail_parts`` raise, simulating a mid-download transport failure.
    """

    def __init__(self, payload, chunk_size, fail_parts=()):
        self.payload = payload
        self.chunk_size = chunk_size
        self.fail_parts = set(fail_parts)
        self.download_calls = []  # list of dicts: {offset, length, max_concurrency}

    def make_client(self, url):
        backend = self
        client = MagicMock()
        client.get_blob_properties.return_value.size = len(backend.payload)

        def download_blob(max_concurrency=1, offset=None, length=None):
            backend.download_calls.append(
                {"offset": offset, "length": length, "max_concurrency": max_concurrency}
            )
            if offset is None:  # full download (single-shot path)
                data = backend.payload
            else:
                part_idx = offset // backend.chunk_size
                if part_idx in backend.fail_parts:
                    raise IOError(f"[SYS] transport died on part {part_idx}")
                data = backend.payload[offset:offset + length]
            stream = MagicMock(size=len(data))
            stream.readinto.side_effect = lambda f: (f.write(data), len(data))[1]
            return stream

        client.download_blob.side_effect = download_blob
        return client

    def fetched_offsets(self):
        return [c["offset"] for c in self.download_calls]


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    """Point the resumable-download tracker at a temp cache dir so tests don't collide."""
    cache_dir = tmp_path / "gs_cache"
    monkeypatch.setattr(resumable_download_tracker, "GEOSEEQ_CACHE_DIR", str(cache_dir))
    return cache_dir


def _patch_from_blob_url(backend):
    return patch(
        "azure.storage.blob.BlobClient.from_blob_url",
        side_effect=lambda url: backend.make_client(url),
    )


def test_resumable_download_produces_correct_full_file(tmp_path, isolated_cache):
    """A large azure download splits into chunk_size parts and concatenates to the exact
    original bytes."""
    payload = bytes(range(256)) * 4  # 1024 bytes
    chunk_size = 100  # -> 11 parts
    out = tmp_path / "bigblob"
    backend = FakeAzureBackend(payload, chunk_size)

    with _patch_from_blob_url(backend):
        result = _download_azure_resumable(URL, str(out), len(payload), chunk_size=chunk_size)

    assert result == str(out)
    assert out.read_bytes() == payload
    # Every part was fetched exactly once as an aligned ranged read.
    assert backend.fetched_offsets() == list(range(0, len(payload), chunk_size))


def test_resumable_download_resumes_without_refetching_completed_parts(tmp_path, isolated_cache):
    """When a part fails mid-download, a re-invocation resumes from the tracker: completed
    parts are NOT re-fetched, and the final file is correct."""
    payload = bytes(range(256)) * 4  # 1024 bytes
    chunk_size = 100  # 11 parts (0..10); part 3 fails on the first attempt
    out = tmp_path / "bigblob"

    # First attempt: part 3 raises on every retry -> the whole download aborts.
    backend1 = FakeAzureBackend(payload, chunk_size, fail_parts={3})
    with _patch_from_blob_url(backend1):
        with pytest.raises(IOError):
            _download_azure_resumable(URL, str(out), len(payload), chunk_size=chunk_size, n_tries=2)

    # Interruption must not leave a valid-looking file at the target path.
    assert not out.exists()
    # Parts 0,1,2 completed before the failure; parts 4+ were never reached.
    assert 0 in backend1.fetched_offsets() and 200 in backend1.fetched_offsets()
    assert 400 not in backend1.fetched_offsets()

    # Second attempt (fresh process): everything succeeds and resumes from the tracker.
    backend2 = FakeAzureBackend(payload, chunk_size)
    with _patch_from_blob_url(backend2):
        result = _download_azure_resumable(URL, str(out), len(payload), chunk_size=chunk_size)

    assert result == str(out)
    assert out.read_bytes() == payload  # full byte-content correctness
    # Completed parts 0,1,2 (offsets 0,100,200) are NOT re-fetched on resume.
    assert 0 not in backend2.fetched_offsets()
    assert 100 not in backend2.fetched_offsets()
    assert 200 not in backend2.fetched_offsets()
    # Part 3 (offset 300) onward IS fetched to finish the download.
    assert 300 in backend2.fetched_offsets()


def test_interrupted_download_leaves_no_valid_file(tmp_path, isolated_cache):
    """An interrupted download never leaves a non-empty file at `filename` that the
    download_url isfile()+size>0 cache check would treat as complete."""
    payload = b"x" * 1000
    chunk_size = 100
    out = tmp_path / "bigblob"
    backend = FakeAzureBackend(payload, chunk_size, fail_parts={5})

    with _patch_from_blob_url(backend):
        with pytest.raises(IOError):
            _download_azure_resumable(URL, str(out), len(payload), chunk_size=chunk_size, n_tries=1)

    assert not out.exists()
    assert not (tmp_path / (out.name + ".partial")).exists()


def test_max_concurrency_and_chunk_size_honored_from_caller(tmp_path, isolated_cache):
    """max_concurrency and chunk_size flow from download_url() down to the resumable path."""
    chunk_size = 150
    out = tmp_path / "bigblob"

    # Route through download_url -> _download_azure_sdk -> resumable, with a spy that captures
    # the arguments actually passed to the resumable helper. Report the blob as larger than the
    # 10*FIVE_MB threshold so the size-based router picks the resumable path.
    captured = {}

    def spy_resumable(url, filename, total_size, progress_tracker=None,
                      max_concurrency=8, chunk_size=5 * file_download.FIVE_MB, n_tries=3):
        captured.update(
            url=url, filename=filename, total_size=total_size,
            max_concurrency=max_concurrency, chunk_size=chunk_size,
        )
        return filename

    client = MagicMock()
    client.get_blob_properties.return_value.size = 10 * file_download.FIVE_MB + 1
    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=client), \
            patch.object(file_download, "_download_azure_resumable", spy_resumable):
        download_url(URL, kind="azure", filename=str(out), max_concurrency=4, chunk_size=chunk_size)

    assert captured["max_concurrency"] == 4
    assert captured["chunk_size"] == chunk_size
    assert captured["total_size"] == 10 * file_download.FIVE_MB + 1


def test_resumable_parts_use_caller_max_concurrency(tmp_path, isolated_cache):
    """Each ranged SDK read is issued with the caller's max_concurrency."""
    payload = b"y" * 500
    chunk_size = 100  # 5 parts
    out = tmp_path / "bigblob"
    backend = FakeAzureBackend(payload, chunk_size)

    with _patch_from_blob_url(backend):
        _download_azure_resumable(URL, str(out), len(payload), chunk_size=chunk_size, max_concurrency=3)

    assert backend.download_calls  # sanity
    assert all(call["max_concurrency"] == 3 for call in backend.download_calls)


def test_small_azure_file_uses_single_shot_path(tmp_path, isolated_cache):
    """A blob at or below the resumable threshold uses the single-shot download, not the
    resumable/ranged machinery."""
    payload = b"small-blob-content"
    out = tmp_path / "small.bin"

    client = MagicMock()
    client.get_blob_properties.return_value.size = len(payload)  # below 10*FIVE_MB
    stream = MagicMock(size=len(payload))
    stream.readinto.side_effect = lambda f: (f.write(payload), len(payload))[1]
    client.download_blob.return_value = stream

    resumable_called = []

    def spy_resumable(*a, **kw):
        resumable_called.append(True)
        return str(out)

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=client), \
            patch.object(file_download, "_download_azure_resumable", spy_resumable):
        result = _download_azure_sdk(URL, str(out))

    assert result == str(out)
    assert out.read_bytes() == payload
    assert not resumable_called  # single-shot path only
    # single-shot => one full download_blob call with no ranged offset/length
    client.download_blob.assert_called_once_with(max_concurrency=8)
