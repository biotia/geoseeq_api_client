import sys
import types
from unittest.mock import MagicMock, patch

import pytest

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
    import base64
    assert base64.b64decode(ids[0]) == b"00000000"
