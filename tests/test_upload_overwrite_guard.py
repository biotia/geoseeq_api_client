"""Unit tests for the `overwrite=False` guard in `ResultFileUpload.upload_file`.

Regression coverage for biotia/geoseeq_api_client#96 (TKT-218): every
brand-new upload failed with

    GeoseeqGeneralError: Overwrite is set to False and file <uuid> already exists.

The old guard used `RemoteObject.exists()`, which calls `get()`, which
short-circuits to a no-op once `_already_fetched` is True. The CLI creates or
bulk-creates the `ResultFile` record *before* uploading (which sets
`_already_fetched`), so `exists()` reported True for a record holding no data
at all. The guard now asks `has_downloadable_file()`, which never consults
`_already_fetched` at all: it resolves a download URL out of `stored_data` and
tries to fetch it, so a record holding no data answers False regardless of what
the fetch flag says.
"""

from unittest.mock import MagicMock, patch

import pytest

from geoseeq import file_system_cache
from geoseeq.knex import GeoseeqGeneralError
from geoseeq.result.result_file import SampleResultFile


@pytest.fixture(autouse=True)
def disable_blob_cache(monkeypatch):
    """Pin the blob cache off so these tests do not depend on test ordering.

    `USE_GEOSEEQ_CACHE` is a process-global that the CLI's `--use-cache`
    callback sets to True and never restores, so a CLI test running earlier in
    the session would otherwise send `create()` down the caching path, where
    hashing a MagicMock parent blows up.
    """
    monkeypatch.setattr(file_system_cache, "USE_GEOSEEQ_CACHE", False)


BLOB = {
    "uuid": "field-uuid-1",
    "name": "R1",
    "stored_data": {},
    "created_at": "2026-06-17T00:00:00Z",
    "updated_at": "2026-06-17T00:00:00Z",
    "pipeline_run": None,
}


def _make_file():
    """Build a SampleResultFile with mocked knex/parent, not yet fetched."""
    knex = MagicMock()
    knex.post.return_value = dict(BLOB)
    parent = MagicMock()
    parent.nested_url.return_value = "sample_ars/parent-uuid"
    return SampleResultFile(knex, parent, "R1")


def _created_in_process():
    """Return a file whose record was created in this process, as the CLI does.

    `create()` sets `_already_fetched = True`, which is what made the old
    `exists()` based guard report True for a record with no data.
    """
    result_file = _make_file()
    result_file.create()
    assert result_file._already_fetched is True
    return result_file


def _bulk_created():
    """Return a file flagged as fetched the way the bulk-create path does.

    `geoseeq.bulk_creators` sets `_already_fetched = True` directly on the
    in-memory objects it resolves, bypassing `create()`.
    """
    result_file = _make_file()
    result_file._already_fetched = True
    return result_file


@pytest.mark.parametrize("make_file", [_created_in_process, _bulk_created])
def test_fresh_record_reports_no_downloadable_file(make_file):
    """The real `has_downloadable_file()` returns False for a data-less record.

    This is the half of the regression the mocked tests below cannot prove: that
    the method the new guard delegates to actually disagrees with `exists()` for
    a freshly created record. The contrast asserted here is the whole bug.

    No network or HTTP mocking is needed. With `stored_data == {}` the blob type
    is `""`, so `get_download_url()` rejects it outright and
    `has_downloadable_file()`'s `except Exception` turns that into False. The
    `pytest.raises` below pins that mechanism: if `get_download_url()` ever
    starts returning a URL for an empty record, this test fails loudly here
    instead of quietly starting to make real network calls.
    """
    result_file = make_file()

    assert result_file._already_fetched is True
    assert result_file.stored_data == {}
    assert result_file.exists() is True  # what the old, broken guard consulted
    with pytest.raises(ValueError):
        result_file.get_download_url()

    assert result_file.has_downloadable_file() is False


@pytest.mark.parametrize("make_file", [_created_in_process, _bulk_created])
def test_no_overwrite_uploads_fresh_record_end_to_end(tmp_path, make_file):
    """Regression, unmocked: overwrite=False lets a brand-new upload through.

    Only `multipart_upload_file` is patched, so the guard runs against the real
    `has_downloadable_file()`. This is the test that would have caught #96.
    """
    result_file = make_file()
    filepath = tmp_path / "reads.fastq"
    filepath.write_text("@r\nATCG\n+\nIIII\n")

    with patch.object(SampleResultFile, "multipart_upload_file",
                      return_value="uploaded") as upload:
        assert result_file.upload_file(filepath, overwrite=False) == "uploaded"

    upload.assert_called_once()


@pytest.mark.parametrize("make_file", [_created_in_process, _bulk_created])
def test_no_overwrite_allows_upload_when_no_downloadable_file(tmp_path, make_file):
    """Regression: a freshly created record has no data, so the upload proceeds."""
    result_file = make_file()
    filepath = tmp_path / "reads.fastq"
    filepath.write_text("@r\nATCG\n+\nIIII\n")

    with patch.object(SampleResultFile, "has_downloadable_file", return_value=False), \
            patch.object(SampleResultFile, "multipart_upload_file",
                         return_value="uploaded") as upload:
        assert result_file.upload_file(filepath, overwrite=False) == "uploaded"

    upload.assert_called_once()


def test_no_overwrite_raises_when_downloadable_file_exists(tmp_path):
    """A record that really has data is still protected by overwrite=False."""
    result_file = _created_in_process()
    filepath = tmp_path / "reads.fastq"
    filepath.write_text("@r\nATCG\n+\nIIII\n")

    with patch.object(SampleResultFile, "has_downloadable_file", return_value=True), \
            patch.object(SampleResultFile, "multipart_upload_file") as upload:
        with pytest.raises(GeoseeqGeneralError):
            result_file.upload_file(filepath, overwrite=False)

    upload.assert_not_called()


def test_overwrite_default_skips_the_check(tmp_path):
    """The default overwrite=True short-circuits: no check, no extra round-trip."""
    result_file = _created_in_process()
    filepath = tmp_path / "reads.fastq"
    filepath.write_text("@r\nATCG\n+\nIIII\n")

    with patch.object(SampleResultFile, "has_downloadable_file",
                      return_value=True) as has_file, \
            patch.object(SampleResultFile, "multipart_upload_file",
                         return_value="uploaded") as upload:
        assert result_file.upload_file(filepath) == "uploaded"

    has_file.assert_not_called()
    upload.assert_called_once()
