"""Helpers for interpreting fastq result-file field names.

The geoseeq_server encodes a file's read-pair number and sequencing lane in the
*field name* it emits for each file in a reads folder (the dict key), e.g.
``"read_1"`` or ``"paired_end::read_2::lane_3"``. The patterns below mirror the
server's ``DATA_NAMES`` field-name conventions so client-side consumers
(pipeline config generation, the Sample class) classify reads the same way the
server names them.
"""
from __future__ import annotations

import re
from typing import Tuple

# Read pair number and lane number are encoded in a file's field name, e.g.
# "read_1", "paired_end::read_2::lane_3". These mirror the server's DATA_NAMES.
READ_PAIR_RE = re.compile(r"read_(?P<pair_num>1|2)")
LANE_RE = re.compile(r"lane_(?P<lane_num>\d+)")


def classify_fastq_field(field_name: str) -> Tuple[int, int]:
    """Classify a fastq file's *field name* into ``(read_num, lane_num)``.

    ``read_num`` is ``2`` only when the field name explicitly matches ``read_2``;
    single-end reads (and any name without a pair token) are treated as read 1.
    ``lane_num`` is the 1-based lane encoded in the field name, defaulting to 1
    when no ``lane_<n>`` token is present.
    """
    pair_match = READ_PAIR_RE.search(field_name)
    read_num = 2 if pair_match is not None and pair_match.group("pair_num") == "2" else 1

    lane_match = LANE_RE.search(field_name)
    lane_num = int(lane_match.group("lane_num")) if lane_match else 1

    return read_num, lane_num
