import json
import logging
import os
import time
import urllib.request
from os.path import basename, getsize, join
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests

from geoseeq.constants import FIVE_MB
from geoseeq.remote_object import RemoteObject, RemoteObjectError
from geoseeq.utils import download_ftp, md5_checksum

logger = logging.getLogger("geoseeq_api")  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program
    

def diff_dicts(blob1, blob2):
    for problem in _diff_dicts("original", "$", blob1, blob2):
        yield problem
    for problem in _diff_dicts("serialized", "$", blob2, blob1):
        yield problem


def _diff_lists(suffix, depth, l1, l2):
    if len(l1) != len(l2):
        yield (f"MISMATCHED_LENGTHS:{suffix}:{depth}", len(l1), len(l2))
    for i in range(len(l1)):
        v1, v2 = l1[i], l2[i]
        if v1 != v2:
            mydepth = f"{depth}.index_{i}"
            if not isinstance(v1, type(v2)):
                yield (f"MISMATCHED_TYPES:{suffix}:{mydepth}", v1, v2)
            elif isinstance(v1, dict):
                for problem in _diff_dicts(suffix, mydepth, v1, v2):
                    yield problem
            elif isinstance(v1, list):
                for problem in _diff_lists(suffix, mydepth, v1, v2):
                    yield problem
            else:
                yield (f"MISMATCHED_LIST_VALUES:{suffix}:{mydepth}", v1, v2)


def _diff_dicts(suffix, depth, blob1, blob2):
    for k, v in blob1.items():
        if k not in blob2:
            yield (f"MISSING_KEY:{suffix}:{depth}", k, None)
        elif v != blob2[k]:
            if not isinstance(v, type(blob2[k])):
                yield (f"MISMATCHED_TYPES:{suffix}:{depth}", v, blob2[k])
            elif isinstance(v, dict):
                for problem in _diff_dicts(suffix, depth + "." + k, v, blob2[k]):
                    yield problem
            elif isinstance(v, list):
                for problem in _diff_lists(suffix, depth + "." + k, v, blob2[k]):
                    yield problem
            else:
                yield (f"MISMATCHED_DICT_VALUES:{suffix}:{depth}", v, blob2[k])


def check_json_serialization(blob):
    """Raise an error if serialization+deserialization fails to return an exact copy."""
    json_serialized = json.loads(json.dumps(blob))
    if json_serialized == blob:
        return
    if isinstance(blob, dict):
        issues = list(diff_dicts(blob, json_serialized))
    else:
        issues = [("MISMATCHED_VALUES:values_only:0", blob, json_serialized)]
    issues = [str(el) for el in issues]
    issues = "\n".join(issues)
    raise RemoteObjectError(f"JSON Serialization modifies object\nIssues:\n{issues}")

