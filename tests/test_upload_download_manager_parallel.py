"""Unit tests for the parallel paths of the upload/download managers.

These cover the regression from issue #82: with a process ``Pool`` a single
per-file exception could not be pickled back across the pool boundary, raising
``multiprocessing.pool.MaybeEncodingError`` and losing all results. After
switching to a ``ThreadPool`` the real exception propagates (or is swallowed by
``ignore_errors``) and successful results are preserved.

Everything here is mocked so the tests are deterministic and never hit the
network.
"""
import logging
import threading
import unittest
from unittest.mock import MagicMock, patch

import pytest

from geoseeq import upload_download_manager as udm
from geoseeq.upload_download_manager import (
    GeoSeeqDownloadManager,
    GeoSeeqUploadManager,
    _make_in_process_logger,
)


class TestUploadManagerParallel(unittest.TestCase):
    """Parallel and series behaviour of GeoSeeqUploadManager.upload_files()."""

    def _manager(self, cores, ignore_errors):
        return GeoSeeqUploadManager(
            n_parallel_uploads=cores, ignore_errors=ignore_errors
        )

    def test_parallel_reraises_real_exception_not_maybe_encoding_error(self):
        """cores>1, ignore_errors=False: the real exception crosses the pool."""
        bad = MagicMock()
        bad.upload_file.side_effect = ValueError("boom")
        mgr = self._manager(cores=2, ignore_errors=False)
        mgr.add_result_file(bad, "bad.fastq")
        mgr.add_result_file(MagicMock(), "good.fastq")

        with pytest.raises(ValueError):
            mgr.upload_files()

    def test_parallel_ignore_errors_preserves_results(self):
        """cores>1, ignore_errors=True: failing file skipped, results kept."""
        bad = MagicMock()
        bad.upload_file.side_effect = ValueError("boom")
        good_a, good_b = MagicMock(), MagicMock()
        mgr = self._manager(cores=3, ignore_errors=True)
        mgr.add_result_file(bad, "bad.fastq")
        mgr.add_result_file(good_a, "good_a.fastq")
        mgr.add_result_file(good_b, "good_b.fastq")

        out = mgr.upload_files()

        # imap_unordered may reorder, so compare as sets.
        self.assertEqual(len(out), 3)
        self.assertEqual({id(x) for x in out}, {id(bad), id(good_a), id(good_b)})
        good_a.upload_file.assert_called_once()
        good_b.upload_file.assert_called_once()

    def test_series_path_unchanged(self):
        """cores==1: files upload in series and results are returned."""
        good_a, good_b = MagicMock(), MagicMock()
        mgr = self._manager(cores=1, ignore_errors=False)
        mgr.add_result_file(good_a, "a.fastq")
        mgr.add_result_file(good_b, "b.fastq")

        out = mgr.upload_files()

        self.assertEqual(out, [good_a, good_b])
        good_a.upload_file.assert_called_once()
        good_b.upload_file.assert_called_once()


class TestDownloadManagerParallel(unittest.TestCase):
    """Parallel and series behaviour of GeoSeeqDownloadManager.download_files()."""

    @staticmethod
    def _fake_download_url(url, filename=None, progress_tracker=None, head=False):
        """Stand-in for download_url: raises for 'bad' paths, else echoes path."""
        if filename and "bad" in filename:
            raise ValueError("boom")
        return filename

    def test_parallel_reraises_real_exception_not_maybe_encoding_error(self):
        """cores>1, ignore_errors=False: a download error propagates cleanly."""
        mgr = GeoSeeqDownloadManager(n_parallel_downloads=2, ignore_errors=False)
        mgr.add_download("http://x/bad", "bad.fastq", key="bad")
        mgr.add_download("http://x/good", "good.fastq", key="good")

        with patch.object(udm, "download_url", self._fake_download_url):
            with pytest.raises(ValueError):
                mgr.download_files()

    def test_parallel_ignore_errors_preserves_results(self):
        """cores>1, ignore_errors=True: a failing callback is skipped, rest kept."""
        def callback(key, local_path):
            if key == "bad":
                raise ValueError("boom")
            return f"cb-{key}"

        mgr = GeoSeeqDownloadManager(n_parallel_downloads=3, ignore_errors=True)
        # File paths avoid "bad" so download succeeds; only the callback raises.
        mgr.add_download("http://x/f0", "f0.fastq", callback=callback, key="bad")
        mgr.add_download("http://x/g1", "g1.fastq", callback=callback, key="g1")
        mgr.add_download("http://x/g2", "g2.fastq", callback=callback, key="g2")

        with patch.object(udm, "download_url", self._fake_download_url):
            out = mgr.download_files()

        self.assertEqual(len(out), 3)
        successes = [r for r in out if r is not None]
        keys = {r[1] for r in successes}
        self.assertEqual(keys, {"g1", "g2"})
        self.assertEqual({r[2] for r in successes}, {"cb-g1", "cb-g2"})

    def test_series_path_unchanged(self):
        """cores==1: files download in series and results are returned."""
        def callback(key, local_path):
            return f"cb-{key}"

        mgr = GeoSeeqDownloadManager(n_parallel_downloads=1, ignore_errors=False)
        mgr.add_download("http://x/g1", "g1.fastq", callback=callback, key="g1")
        mgr.add_download("http://x/g2", "g2.fastq", callback=callback, key="g2")

        with patch.object(udm, "download_url", self._fake_download_url):
            out = mgr.download_files()

        self.assertEqual(
            out,
            [("g1.fastq", "g1", "cb-g1"), ("g2.fastq", "g2", "cb-g2")],
        )


class TestInProcessLoggerIdempotent(unittest.TestCase):
    """_make_in_process_logger must not multiply handlers under threads."""

    def setUp(self):
        self.logger = logging.getLogger("geoseeq_api")
        # Remove any handler a prior parallel test attached so the count is stable.
        self.logger.handlers = [
            h for h in self.logger.handlers
            if not getattr(h, "_geoseeq_in_process", False)
        ]

    def tearDown(self):
        self.logger.handlers = [
            h for h in self.logger.handlers
            if not getattr(h, "_geoseeq_in_process", False)
        ]

    def _tagged_handlers(self):
        return [
            h for h in self.logger.handlers
            if getattr(h, "_geoseeq_in_process", False)
        ]

    def test_repeated_calls_add_at_most_one_handler(self):
        before = len(self.logger.handlers)
        _make_in_process_logger(logging.INFO)
        after_one = len(self.logger.handlers)
        for _ in range(5):
            _make_in_process_logger(logging.INFO)
        after_many = len(self.logger.handlers)

        self.assertEqual(after_one, before + 1)
        self.assertEqual(after_many, after_one)

    def test_concurrent_calls_add_exactly_one_handler(self):
        """Many threads racing to configure the logger add exactly one handler.

        A barrier maximises the chance all threads reach the check together, so
        without the lock this would add roughly one handler per thread.
        """
        n_threads = 16
        barrier = threading.Barrier(n_threads)

        def worker():
            barrier.wait()
            _make_in_process_logger(logging.INFO)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(self._tagged_handlers()), 1)


if __name__ == "__main__":
    unittest.main()
