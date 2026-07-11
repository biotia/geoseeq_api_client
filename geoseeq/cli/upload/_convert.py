"""Shared upload-time file-format conversion (`--convert-file-format`).

Currently only BGZF (block-gzip): recompress a file to a seekable BGZF and attach a
`.gzi` block index. Kept as a small primitive so `upload reads` and `upload files`
share one implementation, and so future formats (e.g. CRAM) can slot in here.
"""
import logging
import os
from os.path import basename, join

import click

from geoseeq.result.bgzf import make_bgzf_with_index

logger = logging.getLogger('geoseeq_api')

# Reusable option; the Choice leaves room for more target formats later.
convert_file_format_option = click.option(
    '--convert-file-format',
    type=click.Choice(['bgzf']),
    default=None,
    help='Convert each file to a seekable format before upload. Currently only "bgzf" '
         '(block-gzip): recompresses the file and attaches a .gzi block index so its bytes '
         'are randomly seekable with samtools/tabix, while staying a normal gzip file. '
         'Mutually exclusive with the in-place --index-* flags.',
)


def convert_one_to_bgzf(local_path, out_dir):
    """Recompress one file to BGZF (+ .gzi) for a seekable upload.

    Returns (upload_path, gzi_path). Best-effort: on any failure it returns
    (local_path, None) so the original file is uploaded instead — conversion never
    fails the upload. The BGZF temp keeps the original basename so the stored name
    is unchanged.
    """
    try:
        os.makedirs(out_dir, exist_ok=True)
        bgzf_path = join(out_dir, basename(local_path))
        gzi_path = bgzf_path + '.gzi'
        stats = make_bgzf_with_index(local_path, bgzf_path, gzi_path)
        click.echo(f"BGZF {basename(local_path)}: {stats['gzi_blocks']} blocks"
                   + (" (already bgzf)" if stats['already_bgzf'] else " (recompressed)")
                   + ".", err=True)
        return bgzf_path, gzi_path
    except Exception as exc:
        logger.warning(f"BGZF conversion failed for {local_path}: {exc}; uploading original.")
        return local_path, None
