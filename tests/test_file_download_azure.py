import sys
import types
from unittest.mock import MagicMock, patch

# Stub azure.storage.blob so tests run without the optional [azure] extra installed.
try:
    import azure.storage.blob  # noqa: F401
except ImportError:
    _azure = types.ModuleType("azure")
    _azure_storage = types.ModuleType("azure.storage")
    _azure_blob = types.ModuleType("azure.storage.blob")
    _azure_blob.BlobClient = types.SimpleNamespace(from_blob_url=lambda url: None)
    sys.modules.setdefault("azure", _azure)
    sys.modules.setdefault("azure.storage", _azure_storage)
    sys.modules.setdefault("azure.storage.blob", _azure_blob)

from geoseeq.result import file_download
from geoseeq.result.file_download import _download_azure_sdk, download_url


def test_download_azure_sdk_writes_blob_to_file(tmp_path):
    """_download_azure_sdk should hand the file handle to download_blob().readinto()."""
    payload = b"hello-blob-payload"
    out = tmp_path / "blob.bin"

    stream = MagicMock()
    stream.size = len(payload)

    def fake_readinto(f):
        f.write(payload)
        return len(payload)

    stream.readinto.side_effect = fake_readinto

    blob_client = MagicMock()
    blob_client.download_blob.return_value = stream

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client) as from_url:
        result = _download_azure_sdk("https://example.blob.core.windows.net/c/b?sig=x", str(out))

    from_url.assert_called_once_with("https://example.blob.core.windows.net/c/b?sig=x")
    blob_client.download_blob.assert_called_once_with(max_concurrency=8)
    assert result == str(out)
    assert out.read_bytes() == payload


def test_download_azure_sdk_respects_head_range(tmp_path):
    """When head is set, download_blob should be called with offset/length."""
    out = tmp_path / "blob.bin"
    stream = MagicMock(size=10)
    stream.readinto.side_effect = lambda f: f.write(b"0123456789") or 10

    blob_client = MagicMock()
    blob_client.download_blob.return_value = stream

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        _download_azure_sdk("https://x.blob.core.windows.net/c/b?sig=x", str(out), head=9)

    blob_client.download_blob.assert_called_once_with(max_concurrency=8, offset=0, length=10)


def test_download_url_azure_falls_back_when_sdk_missing(tmp_path, monkeypatch):
    """If azure-storage-blob is not importable, download_url should fall back to _download_head."""
    out = tmp_path / "blob.bin"

    def boom(*a, **kw):
        raise ImportError("no azure")

    monkeypatch.setattr(file_download, "_download_azure_sdk", boom)

    called = {}

    def fake_head(url, filename, head=None, progress_tracker=None):
        called["url"] = url
        called["filename"] = filename
        called["progress_tracker"] = progress_tracker
        return filename

    monkeypatch.setattr(file_download, "_download_head", fake_head)

    sentinel_tracker = object()
    result = download_url(
        "https://example.blob.core.windows.net/c/b?sig=x",
        kind="azure",
        filename=str(out),
        progress_tracker=sentinel_tracker,
    )

    assert result == str(out)
    assert called["url"].startswith("https://example.blob.core.windows.net")
    assert called["progress_tracker"] is sentinel_tracker
