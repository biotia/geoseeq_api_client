import sys
import types
from unittest.mock import MagicMock, patch

# Stub azure.storage.blob so tests run without the optional [azure] extra installed.
# Note: `azure` is a namespace package shared by many azure-* libs, so it may already
# be in sys.modules without `.storage.blob`. Always force-attach the stub submodules.
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


def test_download_azure_sdk_retries_and_leaves_no_partial(tmp_path):
    """A mid-stream failure must self-heal on retry and never leave a file at the
    target path (a truncated partial there would defeat download_url's cache check)."""
    payload = b"complete-payload"
    out = tmp_path / "blob.bin"

    good = MagicMock(size=len(payload))
    good.readinto.side_effect = lambda f: f.write(payload) or len(payload)

    def flaky_readinto(f):
        f.write(b"trunc")  # partial write, then the transport dies mid-stream
        raise IOError("[SYS] unknown error (_ssl.c:2578)")

    bad = MagicMock(size=len(payload))
    bad.readinto.side_effect = flaky_readinto

    blob_client = MagicMock()
    blob_client.download_blob.side_effect = [bad, good]  # fail once, then succeed

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        result = _download_azure_sdk("https://x.blob.core.windows.net/c/b?sig=x", str(out))

    assert result == str(out)
    assert out.read_bytes() == payload            # clean file, no truncated prefix
    assert not (tmp_path / "blob.bin.partial").exists()


def test_download_azure_sdk_all_retries_fail_no_poison(tmp_path):
    """When every attempt fails, raise and leave no file at the target path so the
    caller's outer retry (and download_url's exists-check) can re-fetch cleanly."""
    out = tmp_path / "blob.bin"

    def flaky_readinto(f):
        f.write(b"trunc")
        raise IOError("[SYS] unknown error (_ssl.c:2578)")

    bad = MagicMock(size=16)
    bad.readinto.side_effect = flaky_readinto

    blob_client = MagicMock()
    blob_client.download_blob.return_value = bad

    with patch("azure.storage.blob.BlobClient.from_blob_url", return_value=blob_client):
        try:
            _download_azure_sdk("https://x.blob.core.windows.net/c/b?sig=x", str(out), n_tries=2)
            assert False, "expected the exhausted retry loop to raise"
        except IOError:
            pass

    assert not out.exists()
    assert not (tmp_path / "blob.bin.partial").exists()


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
