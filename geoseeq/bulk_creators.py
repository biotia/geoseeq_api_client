
from .blob_constructors import (
    sample_from_blob,
    sample_result_folder_from_blob,
)
import json


def bulk_create_samples(knex, samples):
    """Create multiple samples at once. Returns a list of created samples.
    
    Only returns samples which were newly created.
    If a sample already exists on the server, it will not be returned.
    """
    data = {"samples": [sample.get_post_data() for sample in samples]}
    # print(json.dumps(data, indent=4))
    result = knex.post("bulk_samples", json=data)
    created_samples = [
        sample_from_blob(knex, result_blob) for result_blob in result['samples'] if result_blob
    ]
    return created_samples


def bulk_create_sample_result_folders(knex, sample_results):
    """Create multiple sample results at once. Returns a list of created sample results.
    
    Only returns sample results which were newly created.
    If a sample result already exists on the server, it will not be returned.
    """
    data = {"sample_results": [sample_result.get_post_data() for sample_result in sample_results]}
    result = knex.post(
        "bulk_sample_results",
        json=data,
    )
    created_sample_result_folders = [
        sample_result_folder_from_blob(knex, result_blob) for result_blob in result['sample_results'] if result_blob
    ]
    return created_sample_result_folders


def bulk_create_sample_result_files(knex, sample_result_fields):
    """Create multiple sample result fields at once. Returns a list of created sample result fields.

    Only returns sample result fields which were newly created.
    If a sample result field already exists on the server, it will not be returned.

    The server uses ``SampleAnalysisResultFieldListSerializer`` for this
    endpoint's response, which intentionally omits ``analysis_result_obj``
    to keep the payload small. So instead of building fresh objects via
    ``sample_result_file_from_blob`` (which requires that nested key), we
    reuse the in-memory objects we already passed in, matching them back
    to the created blobs by ``(parent.uuid, name)`` and writing the
    server-assigned ``uuid`` + cached blob onto them.
    """
    result = knex.post(
        "bulk_sample_result_fields",
        json={"sample_result_fields": [f.get_post_data() for f in sample_result_fields]},
    )
    in_memory_by_key = {(f.parent.uuid, f.name): f for f in sample_result_fields}
    created = []
    for blob in result['sample_result_fields']:
        if not blob:
            continue
        in_memory = in_memory_by_key.get((blob['analysis_result'], blob['name']))
        if in_memory is None:
            continue
        in_memory.uuid = blob['uuid']
        in_memory.load_blob(blob, allow_overwrite=True)
        in_memory._already_fetched = True
        in_memory.cache_blob(blob)
        created.append(in_memory)
    return created