"""Optional BGZF recompression for uploads (default off).

BGZF (the block-gzip format used by htslib/samtools/tabix) is a valid gzip file
that is internally a sequence of independent <=64 KB blocks, so it supports random
access with only a tiny block-offset index (`.gzi`) — no 32 KB window snapshots.
Recompressing an upload to BGZF makes any byte range (a read, a tar member)
seekable with standard bioinformatics tooling, while remaining `zcat`-able.

This is the seekable-format alternative to the zran-style `.gzi` in read_index.py
(which indexes a plain-gzip stream in place). The two are mutually exclusive: BGZF
rewrites the bytes; the zran index annotates them.

Only `biopython` (already a base dependency) is required — `Bio.bgzf` writes the
BGZF; the `.gzi` is built by scanning block framing. isal is used opportunistically
to speed up the decompress pass when installed.
"""
import gzip
import struct
from os.path import basename

from Bio import bgzf


def is_gzipped(filepath):
    with open(filepath, "rb") as f:
        return f.read(2) == b"\x1f\x8b"


def is_bgzf(filepath):
    """True if the file already starts with a BGZF block header (gzip + 'BC' extra)."""
    with open(filepath, "rb") as f:
        head = f.read(18)
    return len(head) >= 14 and head[:4] == b"\x1f\x8b\x08\x04" and head[12:14] == b"BC"


def _open_decompressed(filepath):
    """Binary file-like over the file's uncompressed content (gunzipped if gzipped).

    Handles multi-member gzip (concatenated per-lane .gz). Uses isal when available.
    """
    if not is_gzipped(filepath):
        return open(filepath, "rb")
    try:
        from isal import igzip

        return igzip.open(filepath, "rb")
    except ImportError:
        return gzip.open(filepath, "rb")


def recompress_to_bgzf(src_path, dst_path, read_size=1 << 20):
    """Rewrite ``src_path`` (gzipped or plain) as a BGZF file at ``dst_path``.

    Decompressed content is preserved byte-for-byte; only the compression framing
    changes. Returns the number of uncompressed bytes written.
    """
    total = 0
    with _open_decompressed(src_path) as fin, bgzf.BgzfWriter(dst_path, "wb") as fout:
        for chunk in iter(lambda: fin.read(read_size), b""):
            fout.write(chunk)
            total += len(chunk)
    return total


def build_gzi(bgzf_path, gzi_path):
    """Scan a BGZF file's block framing and write an htslib-compatible `.gzi`.

    Format (little-endian): uint64 n_entries, then n_entries (uint64 compressed_offset,
    uint64 uncompressed_offset) pairs marking each block start after the first (the
    first block at 0,0 is implicit; the empty EOF marker block is not indexed).
    Byte-identical to `bgzip -r` output. Returns the entry count.
    """
    entries = []
    coff = uoff = 0
    with open(bgzf_path, "rb") as f:
        while True:
            header = f.read(18)
            if len(header) < 18:
                break
            bsize = struct.unpack("<H", header[16:18])[0] + 1  # BC extra field: block size - 1
            block_tail = f.read(bsize - 18)
            isize = struct.unpack("<I", block_tail[-4:])[0]  # uncompressed size of this block
            if isize == 0:  # EOF marker block
                break
            if coff != 0:  # the first block's (0, 0) is implicit
                entries.append((coff, uoff))
            coff += bsize
            uoff += isize
    with open(gzi_path, "wb") as f:
        f.write(struct.pack("<Q", len(entries)))
        for c, u in entries:
            f.write(struct.pack("<QQ", c, u))
    return len(entries)


def make_bgzf_with_index(src_path, dst_bgzf, dst_gzi):
    """Produce a seekable BGZF plus its `.gzi` for a local file.

    If the source is already BGZF the block framing is reused as-is (the file is
    still rewritten so the caller gets a stable path); otherwise it is recompressed.
    Returns a dict of stats.
    """
    if is_bgzf(src_path):
        # Already block-gzipped — decompress+recompress would waste CPU. Copy through
        # BgzfWriter is avoided; just index the existing framing after copying bytes.
        with open(src_path, "rb") as fi, open(dst_bgzf, "wb") as fo:
            for chunk in iter(lambda: fi.read(1 << 20), b""):
                fo.write(chunk)
        already_bgzf = True
    else:
        recompress_to_bgzf(src_path, dst_bgzf)
        already_bgzf = False
    n_blocks = build_gzi(dst_bgzf, dst_gzi)
    return {
        "source_file": basename(src_path),
        "already_bgzf": already_bgzf,
        "gzi_blocks": n_blocks,
    }
