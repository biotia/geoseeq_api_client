import gzip
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from geoseeq.cli.upload.upload_reads import _index_one_reads_file
from geoseeq.result import read_index

# The indexer needs the optional `indexing` extra; skip real-build tests without it.
_HAS_DEPS = read_index.deps_available()
needs_deps = pytest.mark.skipif(not _HAS_DEPS, reason="requires geoseeq[indexing] (isal + indexed_gzip)")


def _write_fastq_gz(path, n_reads, readlen=50):
    with gzip.open(path, "wt") as f:
        for i in range(n_reads):
            f.write(f"@read{i}\n{'A' * readlen}\n+\n{'I' * readlen}\n")


@needs_deps
def test_build_read_index_counts_reads_and_writes_gzi(tmp_path: Path):
    fq = tmp_path / "reads.fastq.gz"
    _write_fastq_gz(fq, n_reads=1000)
    gzi = tmp_path / "reads.gzi"

    index = read_index.build_read_index(str(fq), str(gzi), spacing=1 << 20)

    assert index["read_count"] == 1000
    assert index["format"] == read_index.INDEX_FORMAT
    assert gzi.exists() and gzi.stat().st_size > 0
    # sections always start at offset 0 with 0 reads before it.
    assert index["sections"][0] == {"offset": 0, "reads_before": 0}
    assert index["gzi_file"] == "reads.gzi"


@needs_deps
def test_build_read_index_records_sections_by_spacing(tmp_path: Path):
    fq = tmp_path / "reads.fastq.gz"
    _write_fastq_gz(fq, n_reads=20000)  # ~2.4 MB uncompressed
    gzi = tmp_path / "reads.gzi"

    index = read_index.build_read_index(str(fq), str(gzi), spacing=1 << 20)  # 1 MB

    assert len(index["sections"]) > 1  # multiple sections at 1 MB spacing
    offsets = [s["offset"] for s in index["sections"]]
    reads_before = [s["reads_before"] for s in index["sections"]]
    assert offsets == sorted(offsets)          # monotonic
    assert reads_before == sorted(reads_before)
    assert reads_before[-1] <= index["read_count"]


def test_require_deps_raises_helpful_error(monkeypatch):
    monkeypatch.setattr(read_index, "deps_available", lambda: False)
    with pytest.raises(ImportError, match="geoseeq\\[indexing\\]"):
        read_index._require_deps()


def _mock_reads_file(name="short_read::R1"):
    reads_file = MagicMock()
    reads_file.name = name
    folder = reads_file.parent
    folder.result_file.side_effect = lambda n: MagicMock(name=n)
    return reads_file, folder


@needs_deps
def test_index_one_reads_file_uploads_two_sidecars(tmp_path: Path):
    fq = tmp_path / "reads.fastq.gz"
    _write_fastq_gz(fq, n_reads=500)
    reads_file, folder = _mock_reads_file()

    _index_one_reads_file(reads_file, str(fq))

    uploaded = [c.args[0] for c in folder.result_file.call_args_list]
    assert reads_file.name + ".gzi" in uploaded
    assert reads_file.name + ".index.json" in uploaded
    assert folder.result_file.call_count == 2  # exactly the two sidecars


def test_index_one_reads_file_skips_non_gzip(tmp_path: Path):
    fq = tmp_path / "reads.fastq"  # not gzipped
    fq.write_text("@r\nACGT\n+\nIIII\n")
    reads_file, folder = _mock_reads_file()

    _index_one_reads_file(reads_file, str(fq))

    folder.result_file.assert_not_called()  # nothing uploaded for uncompressed input


def test_index_one_reads_file_swallows_errors(tmp_path: Path, monkeypatch):
    fq = tmp_path / "reads.fastq.gz"
    _write_fastq_gz(fq, n_reads=10) if _HAS_DEPS else fq.write_bytes(b"\x1f\x8b")
    reads_file, folder = _mock_reads_file()
    # Force the indexer to blow up; the helper must log and return, not raise.
    monkeypatch.setattr(
        "geoseeq.result.read_index.build_read_index",
        MagicMock(side_effect=RuntimeError("boom")),
    )

    _index_one_reads_file(reads_file, str(fq))  # must not raise

    folder.result_file.assert_not_called()


def test_index_json_roundtrips(tmp_path: Path):
    index = {"format": read_index.INDEX_FORMAT, "read_count": 7, "sections": []}
    path = read_index.write_index_json(index, str(tmp_path / "x.json"))
    assert json.loads(Path(path).read_text())["read_count"] == 7
