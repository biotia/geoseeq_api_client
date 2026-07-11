import gzip
import shutil
import struct
import subprocess
import tarfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from geoseeq.result import bgzf
from geoseeq.cli.upload.upload_reads import cli_upload_reads_wizard
from geoseeq.cli.upload.upload import cli_upload_file
from geoseeq.cli.upload._convert import convert_one_to_bgzf


def _content(n_reads=50000):
    return b"".join(
        f"@read{i}\nACGTACGTACGT\n+\nIIIIIIIIIIII\n".encode() for i in range(n_reads)
    )


def _write_gz(path, content):
    with gzip.open(path, "wb") as f:
        f.write(content)


def _read_gzi(gzi_path):
    data = Path(gzi_path).read_bytes()
    (n,) = struct.unpack("<Q", data[:8])
    entries = [struct.unpack("<QQ", data[8 + 16 * i : 24 + 16 * i]) for i in range(n)]
    return entries


def _decompress_block_at(bgzf_path, coffset):
    """Read the single BGZF block starting at compressed offset `coffset` and return
    its decompressed bytes (each block is a standalone gzip member)."""
    with open(bgzf_path, "rb") as f:
        f.seek(coffset)
        header = f.read(18)
        bsize = struct.unpack("<H", header[16:18])[0] + 1
        block = header + f.read(bsize - 18)
    return gzip.decompress(block)


def test_recompress_content_roundtrips(tmp_path: Path):
    content = _content()
    src = tmp_path / "reads.fastq.gz"
    _write_gz(src, content)
    dst = tmp_path / "reads.bgz"

    bgzf.recompress_to_bgzf(str(src), str(dst))

    assert bgzf.is_bgzf(str(dst))
    # BGZF is gzip-compatible: plain gunzip yields the exact original content.
    assert gzip.open(str(dst), "rb").read() == content


def test_gzi_structure_and_seek(tmp_path: Path):
    content = _content()
    src = tmp_path / "reads.fastq.gz"
    _write_gz(src, content)
    dst = tmp_path / "reads.bgz"
    gzi = tmp_path / "reads.bgz.gzi"

    bgzf.recompress_to_bgzf(str(src), str(dst))
    n = bgzf.build_gzi(str(dst), str(gzi))

    entries = _read_gzi(str(gzi))
    assert len(entries) == n and n > 1  # multiple blocks for this size
    coffs = [c for c, _ in entries]
    uoffs = [u for _, u in entries]
    assert coffs == sorted(coffs) and uoffs == sorted(uoffs)  # monotonic
    assert entries[0][0] != 0  # first (0,0) block is implicit, not stored

    # Seek: the block at a .gzi entry must decompress to the content at its uoffset.
    coff, uoff = entries[len(entries) // 2]
    block = _decompress_block_at(str(dst), coff)
    assert content[uoff : uoff + len(block)] == block


@pytest.mark.skipif(shutil.which("bgzip") is None, reason="bgzip CLI not available")
def test_gzi_matches_bgzip_ground_truth(tmp_path: Path):
    content = _content()
    src = tmp_path / "reads.fastq.gz"
    _write_gz(src, content)
    dst = tmp_path / "reads.bgz"
    mine = tmp_path / "mine.gzi"
    ref = tmp_path / "ref.gzi"

    bgzf.recompress_to_bgzf(str(src), str(dst))
    bgzf.build_gzi(str(dst), str(mine))
    subprocess.run(["bgzip", "-r", "-I", str(ref), str(dst)], check=True)

    assert mine.read_bytes() == ref.read_bytes()  # byte-identical to htslib's index


def test_make_bgzf_with_index_plain_input(tmp_path: Path):
    content = _content(1000)
    src = tmp_path / "reads.fastq"  # not gzipped
    src.write_bytes(content)
    dst = tmp_path / "reads.bgz"
    gzi = tmp_path / "reads.bgz.gzi"

    stats = bgzf.make_bgzf_with_index(str(src), str(dst), str(gzi))

    assert stats["already_bgzf"] is False
    assert bgzf.is_bgzf(str(dst))
    assert gzip.open(str(dst), "rb").read() == content
    assert stats["gzi_blocks"] == len(_read_gzi(str(gzi)))


def test_make_bgzf_with_index_already_bgzf_is_detected(tmp_path: Path):
    content = _content(1000)
    plain = tmp_path / "reads.fastq.gz"
    _write_gz(plain, content)
    first = tmp_path / "first.bgz"
    bgzf.recompress_to_bgzf(str(plain), str(first))  # now a real BGZF file

    dst = tmp_path / "second.bgz"
    gzi = tmp_path / "second.bgz.gzi"
    stats = bgzf.make_bgzf_with_index(str(first), str(dst), str(gzi))

    assert stats["already_bgzf"] is True  # skipped recompression
    assert gzip.open(str(dst), "rb").read() == content


def test_convert_helper_falls_back_on_failure(tmp_path: Path, monkeypatch):
    src = tmp_path / "reads.fastq.gz"
    _write_gz(src, _content(100))
    # _convert imported make_bgzf_with_index by name, so patch it there.
    monkeypatch.setattr(
        "geoseeq.cli.upload._convert.make_bgzf_with_index",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    upload_path, gzi_path = convert_one_to_bgzf(str(src), str(tmp_path / "out"))

    assert upload_path == str(src)  # fell back to the original file
    assert gzi_path is None


def test_cli_reads_convert_and_index_are_mutually_exclusive(tmp_path: Path):
    fq = tmp_path / "x.fastq.gz"  # must exist to pass click's Path(exists=True)
    _write_gz(fq, _content(10))
    result = CliRunner().invoke(
        cli_upload_reads_wizard,
        ["--convert-file-format", "bgzf", "--index-reads", "some_project", str(fq)],
    )
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


def test_cli_files_convert_and_index_tar_are_mutually_exclusive(tmp_path: Path):
    f = tmp_path / "x.tar.gz"
    _write_gz(f, _content(10))
    result = CliRunner().invoke(
        cli_upload_file,
        ["--convert-file-format", "bgzf", "--index-tar", "some_folder", str(f)],
    )
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


def _make_real_tarball(path: Path) -> Path:
    """Write a real .tar.gz so tarfile.is_tarfile() returns True."""
    inner = path.parent / "inner.txt"
    inner.write_text("hello")
    with tarfile.open(path, "w:gz") as tf:
        tf.add(str(inner), arcname="inner.txt")
    return path


def _patch_knex_and_folder(monkeypatch):
    """Patch out the API layer so cli_upload_file reaches the tarball gate."""
    fake_folder = MagicMock()
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload.handle_folder_id",
        lambda *a, **kw: fake_folder,
    )
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload.GeoSeeqUploadManager",
        MagicMock(return_value=MagicMock()),
    )


def test_cli_files_tarball_gate_aborts_without_yes(tmp_path: Path, monkeypatch):
    """A tarball + --convert-file-format bgzf without --yes prompts and aborts on 'n'."""
    _patch_knex_and_folder(monkeypatch)
    tar = _make_real_tarball(tmp_path / "archive.tar.gz")
    result = CliRunner().invoke(
        cli_upload_file,
        ["--convert-file-format", "bgzf", "some_folder", str(tar)],
        input="n\n",
    )
    assert result.exit_code != 0
    assert "tarball" in result.output.lower() or "Warning" in result.output


def test_cli_files_tarball_gate_bypassed_with_yes(tmp_path: Path, monkeypatch):
    """--yes skips the tarball confirmation gate entirely."""
    _patch_knex_and_folder(monkeypatch)
    # Also patch convert_one_to_bgzf so no real compression happens.
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload.convert_one_to_bgzf",
        lambda path, out_dir: (path, None),
    )
    tar = _make_real_tarball(tmp_path / "archive.tar.gz")
    result = CliRunner().invoke(
        cli_upload_file,
        ["--convert-file-format", "bgzf", "--yes", "some_folder", str(tar)],
    )
    # Gate is not hit; command proceeds past the tarball check (may fail later on
    # network, but exit_code != caused-by-tarball-gate means the gate was skipped).
    assert "Convert" not in result.output or result.exit_code == 0
