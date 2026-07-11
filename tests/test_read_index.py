import gzip
import io
import json
import tarfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from geoseeq.cli.upload.upload_reads import _index_one_reads_file
from geoseeq.result import read_index

# The indexer needs the optional `indexing` extra; skip real-build tests without it.
_HAS_DEPS = read_index.deps_available()
needs_deps = pytest.mark.skipif(
    not _HAS_DEPS, reason="requires geoseeq[indexing] (isal + indexed_gzip)"
)


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


@needs_deps
def test_build_read_index_counts_all_members_of_concatenated_gzip(tmp_path: Path):
    # Concatenated per-lane .gz files are one valid multi-member gzip stream; the
    # counter must read past the first member, not stop at it.
    a, b = tmp_path / "a.fastq.gz", tmp_path / "b.fastq.gz"
    _write_fastq_gz(a, n_reads=300)
    _write_fastq_gz(b, n_reads=200)
    combined = tmp_path / "combined.fastq.gz"
    combined.write_bytes(a.read_bytes() + b.read_bytes())

    index = read_index.build_read_index(str(combined), str(tmp_path / "c.gzi"), spacing=1 << 20)

    assert index["read_count"] == 500  # 300 + 200, not just the first member


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

    # Capture the JSON sidecar content at upload time (temp file still exists then).
    captured = {}

    def _result_file(sidecar_name):
        rf = MagicMock()
        def _upload(path):
            if path.endswith(".index.json"):
                captured["json"] = json.loads(Path(path).read_text())
        rf.upload_file.side_effect = _upload
        return rf
    folder.result_file.side_effect = _result_file

    _index_one_reads_file(reads_file, str(fq))

    uploaded = [c.args[0] for c in folder.result_file.call_args_list]
    assert reads_file.name + ".gzi" in uploaded
    assert reads_file.name + ".index.json" in uploaded
    assert folder.result_file.call_count == 2  # exactly the two sidecars
    # gzi_file in the metadata must match the uploaded sidecar, not the temp name.
    assert captured["json"]["gzi_file"] == reads_file.name + ".gzi"


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


# --- tar index -----------------------------------------------------------------


def _make_tar(path, files, gzipped):
    mode = "w:gz" if gzipped else "w"
    with tarfile.open(path, mode) as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


def test_build_tar_index_plain(tmp_path: Path):
    files = {"a.txt": b"hello", "b/c.txt": b"world!!"}
    tar = tmp_path / "plain.tar"
    _make_tar(tar, files, gzipped=False)

    index = read_index.build_tar_index(str(tar))

    assert index["format"] == read_index.TAR_INDEX_FORMAT
    assert index["compressed"] is False
    assert index["gzi_file"] is None
    assert index["member_count"] == 2
    by_name = {m["name"]: m for m in index["members"]}
    assert by_name["a.txt"]["size"] == 5 and by_name["b/c.txt"]["size"] == 7


def test_read_tar_member_plain_random_access(tmp_path: Path):
    files = {"a.txt": b"hello", "big.bin": b"X" * 1000, "z.txt": b"tail"}
    tar = tmp_path / "plain.tar"
    _make_tar(tar, files, gzipped=False)
    index = read_index.build_tar_index(str(tar))
    by_name = {m["name"]: m for m in index["members"]}

    # Read the last member without scanning the whole archive.
    assert read_index.read_tar_member(str(tar), by_name["z.txt"]) == b"tail"
    assert read_index.read_tar_member(str(tar), by_name["a.txt"]) == b"hello"


@needs_deps
def test_build_and_read_tar_index_gzipped(tmp_path: Path):
    files = {"a.txt": b"hello", "b.txt": b"second file", "c.txt": b"third"}
    tar = tmp_path / "arc.tar.gz"
    _make_tar(tar, files, gzipped=True)
    gzi = tmp_path / "arc.gzi"

    index = read_index.build_tar_index(str(tar), str(gzi))

    assert index["compressed"] is True
    assert gzi.exists() and index["gzi_file"] == "arc.gzi"
    by_name = {m["name"]: m for m in index["members"]}
    # Random access into the gzipped tar via the seek index.
    got = read_index.read_tar_member(str(tar), by_name["b.txt"], gzi_path=str(gzi))
    assert got == b"second file"


@needs_deps
def test_gzipped_tar_random_access_with_real_checkpoints(tmp_path: Path):
    # Members spread over enough uncompressed bytes that a small spacing yields
    # internal gzi checkpoints, so seeking to a late member starts from a
    # checkpoint rather than scanning from byte 0 — the feature's actual fast path.
    files = {f"f{i}.bin": bytes([65 + i]) * 100_000 for i in range(6)}  # 600 KB uncompressed
    tar = tmp_path / "big.tar.gz"
    _make_tar(tar, files, gzipped=True)
    gzi = tmp_path / "big.gzi"

    index = read_index.build_tar_index(str(tar), str(gzi), spacing=1 << 16)  # 64 KB spacing
    by_name = {m["name"]: m for m in index["members"]}

    last = read_index.read_tar_member(str(tar), by_name["f5.bin"], gzi_path=str(gzi))
    assert last == bytes([70]) * 100_000  # 'F' * 100000, reached via a non-zero checkpoint


def test_read_tar_member_gzipped_without_gzi_raises(tmp_path: Path):
    # Guard against silently seeking uncompressed offsets into compressed bytes.
    tar = tmp_path / "arc.tar.gz"
    _make_tar(tar, {"a.txt": b"hello"}, gzipped=True)
    with pytest.raises(ValueError, match="gzi_path"):
        read_index.read_tar_member(str(tar), {"offset": 512, "size": 5})


def test_index_one_tar_file_gzipped_without_deps_uploads_manifest_only(tmp_path: Path, monkeypatch):
    # The member manifest needs no optional deps; only the .gzi does. Without the
    # indexing extra we should still upload the manifest, just skip the .gzi.
    from geoseeq.cli.upload import upload as upload_mod
    tar = tmp_path / "arc.tar.gz"
    _make_tar(tar, {"a.txt": b"hi"}, gzipped=True)
    folder = MagicMock()
    monkeypatch.setattr("geoseeq.result.read_index.deps_available", lambda: False)

    upload_mod._index_one_tar_file(folder, "arc.tar.gz", str(tar))

    uploaded = [c.args[0] for c in folder.result_file.call_args_list]
    assert "arc.tar.gz.tar-index.json" in uploaded  # manifest still uploaded
    assert "arc.tar.gz.gzi" not in uploaded          # .gzi skipped without deps


def test_index_one_tar_file_skips_non_tar(tmp_path: Path):
    from geoseeq.cli.upload.upload import _index_one_tar_file
    not_tar = tmp_path / "notes.txt"
    not_tar.write_text("just text")
    folder = MagicMock()

    _index_one_tar_file(folder, "notes.txt", str(not_tar))

    folder.result_file.assert_not_called()


def test_index_one_tar_file_uploads_sidecar(tmp_path: Path):
    from geoseeq.cli.upload.upload import _index_one_tar_file
    tar = tmp_path / "plain.tar"
    _make_tar(tar, {"a.txt": b"hi"}, gzipped=False)
    folder = MagicMock()

    _index_one_tar_file(folder, "plain.tar", str(tar))

    uploaded = [c.args[0] for c in folder.result_file.call_args_list]
    assert "plain.tar.tar-index.json" in uploaded
    assert "plain.tar.gzi" not in uploaded  # plain tar -> no gzi sidecar
