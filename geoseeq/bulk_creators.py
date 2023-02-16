
from .blob_constructors import (
    sample_from_blob,
    sample_result_from_blob,
    sample_ar_field_from_blob,
)


def bulk_create_samples(knex, samples):
    """Create multiple samples at once. Returns a list of created samples.
    
    Only returns samples which were newly created.
    If a sample already exists on the server, it will not be returned.
    """
    result = knex.post(
        "bulk_samples",
        json={"samples": [sample.get_post_data() for sample in samples]},
    )
    created_samples = [
        sample_from_blob(knex, result_blob) for result_blob in result if result_blob
    ]
    return created_samples


def bulk_create_sample_results(knex, sample_results):
    """Create multiple sample results at once. Returns a list of created sample results.
    
    Only returns sample results which were newly created.
    If a sample result already exists on the server, it will not be returned.
    """
    result = knex.post(
        "bulk_sample_results",
        json={"sample_results": [sample_result.get_post_data() for sample_result in sample_results]},
    )
    created_sample_results = [
        sample_result_from_blob(knex, result_blob) for result_blob in result if result_blob
    ]
    return created_sample_results


def bulk_create_sample_result_fields(knex, sample_result_fields):
    """Create multiple sample result fields at once. Returns a list of created sample result fields.
    
    Only returns sample result fields which were newly created.
    If a sample result field already exists on the server, it will not be returned.    
    """
    result = knex.post(
        "bulk_sample_result_fields",
        json={"sample_result_fields": [sample_result_field.get_post_data() for sample_result_field in sample_result_fields]},
    )
    created_sample_result_fields = [
        sample_ar_field_from_blob(knex, result_blob) for result_blob in result if result_blob
    ]
    return created_sample_result_fields