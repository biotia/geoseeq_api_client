"""Optional read-indexing at upload time (default off).

Since an upload already streams every byte of a gzipped fastq, we can pay one
extra decompress pass to record artifacts that fastq itself doesn't store:

  - total read count
  - a gzip seek index (.gzi, zran-style) for random access without full decompress
  - per-section read counts, for two-phase subsampling (pick sections -> sample reads)

Enabled via `geoseeq upload reads --index-reads`. Needs the `indexing` extra
(isal + indexed_gzip); both are imported lazily so the base install is unaffected.
"""
import json
import logging
from os.path import basename

logger = logging.getLogger("geoseeq_api")

INDEX_FORMAT = "geoseeq-read-index-1"
DEFAULT_SPACING = 100 << 20  # 100 MB uncompressed between seek points -> sub-MB index


def deps_available():
    try:
        import indexed_gzip  # noqa: F401
        from isal import igzip  # noqa: F401
        return True
    except ImportError:
        return False


def _require_deps():
    if not deps_available():
        raise ImportError(
            "--index-reads needs the 'indexing' extra: "
            "pip install 'geoseeq[indexing]' (installs isal + indexed_gzip)."
        )


def build_read_index(filepath, gzi_path, spacing=DEFAULT_SPACING, bufsize=1 << 20):
    """Build a seek index + read stats for a gzipped fastq.

    Writes the seekable index to ``gzi_path`` and returns a JSON-able dict. Section
    offsets are approximate (recorded at the decompress-buffer boundary that crosses
    each ``spacing`` mark) — a consumer seeks there and re-syncs to the next full
    record, so exact read-boundary alignment isn't needed.
    """
    _require_deps()
    import indexed_gzip as igz
    from isal import igzip

    # Seekable .gzi (indexed_gzip runs its own C decompress pass).
    # TODO: one-pass fusion — feed a single decompressed stream to both the
    # checkpoint recorder and the counter instead of decompressing twice.
    f = igz.IndexedGzipFile(filepath, spacing=spacing)
    try:
        f.build_full_index()
        f.export_index(gzi_path)
    finally:
        f.close()

    # Read counts + per-section counts (isal decompress pass). igzip.open handles
    # multi-member gzip (concatenated per-lane .gz), which a bare decompressobj would
    # truncate at the first member.
    newlines = uncomp = 0
    next_bound = spacing
    sections = [{"offset": 0, "reads_before": 0}]
    with igzip.open(filepath, "rb") as fh:
        for out in iter(lambda: fh.read(bufsize), b""):
            newlines += out.count(b"\n")
            uncomp += len(out)
            if uncomp >= next_bound:
                sections.append({"offset": uncomp, "reads_before": newlines // 4})
                next_bound += spacing

    return {
        "format": INDEX_FORMAT,
        "source_file": basename(filepath),
        "read_count": newlines // 4,
        "uncompressed_size": uncomp,
        "spacing_bytes": spacing,
        "gzi_file": basename(gzi_path),
        "sections": sections,  # each: {offset (uncompressed), reads_before (cumulative)}
    }


def write_index_json(index, json_path):
    with open(json_path, "w") as f:
        json.dump(index, f, indent=4)
    return json_path


TAR_INDEX_FORMAT = "geoseeq-tar-index-1"


def is_gzipped(filepath):
    with open(filepath, "rb") as f:
        return f.read(2) == b"\x1f\x8b"


def build_tar_index(filepath, gzi_path=None, spacing=DEFAULT_SPACING):
    """Index the members of a tar (optionally gzipped) for random access.

    Returns a JSON-able dict with one entry per regular file — {name, offset, size}
    where ``offset`` is the uncompressed byte offset of the member's content. When the
    tar is gzipped and ``gzi_path`` is given, also writes a .gzi seek index so a
    consumer can jump straight to a member with :func:`read_tar_member` instead of
    decompressing the whole archive. Member offsets come from stdlib ``tarfile``, so
    the manifest needs no optional deps; only the .gzi does.
    """
    import tarfile

    gz = is_gzipped(filepath)
    gzi_written = None
    if gz and gzi_path:
        _require_deps()
        import indexed_gzip as igz
        f = igz.IndexedGzipFile(filepath, spacing=spacing)
        try:
            f.build_full_index()
            f.export_index(gzi_path)
        finally:
            f.close()
        gzi_written = basename(gzi_path)

    # Stream sequentially; offset_data is the member's uncompressed content offset.
    members = []
    with tarfile.open(filepath, "r|gz" if gz else "r|") as tf:
        for m in tf:
            if m.isfile():
                members.append({"name": m.name, "offset": m.offset_data, "size": m.size})

    return {
        "format": TAR_INDEX_FORMAT,
        "source_file": basename(filepath),
        "compressed": gz,
        "gzi_file": gzi_written,
        "member_count": len(members),
        "members": members,
    }


def read_tar_member(tar_path, member, gzi_path=None):
    """Read one member's bytes via random access, given its index entry
    ({offset, size} from :func:`build_tar_index`). Uses the .gzi seek index for
    gzipped tars; a plain seek otherwise."""
    offset, size = member["offset"], member["size"]
    if gzi_path:
        _require_deps()
        import indexed_gzip as igz
        with igz.IndexedGzipFile(tar_path) as f:
            f.import_index(gzi_path)
            f.seek(offset)
            return f.read(size)
    with open(tar_path, "rb") as f:
        f.seek(offset)
        return f.read(size)
