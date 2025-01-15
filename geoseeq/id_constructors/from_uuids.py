from geoseeq import GeoseeqNotFoundError
from geoseeq.knex import with_knex

from .from_blobs import *


@with_knex
def org_from_uuid(knex, uuid):
    """Return the organization object which the uuid points to."""
    blob = knex.get(f"organizations/{uuid}")
    org = org_from_blob(knex, blob)
    return org


@with_knex
def project_from_uuid(knex, uuid):
    """Return the project object which the uuid points to."""
    blob = knex.get(f"sample_groups/{uuid}")
    project = project_from_blob(knex, blob)
    return project


sample_group_from_uuid = project_from_uuid  # Alias


@with_knex
def sample_from_uuid(knex, uuid):
    """Return the sample object which the uuid points to."""
    blob = knex.get(f"samples/{uuid}")
    sample = sample_from_blob(knex, blob)
    return sample


@with_knex
def sample_result_folder_from_uuid(knex, uuid):
    """Return the sample result folder object which the uuid points to."""
    blob = knex.get(f"sample_ars/{uuid}")
    ar = sample_result_folder_from_blob(knex, blob)
    return ar


sample_ar_from_uuid = sample_result_folder_from_uuid  # Alias


@with_knex
def project_result_folder_from_uuid(knex, uuid):
    """Return the project result folder object which the uuid points to."""
    blob = knex.get(f"sample_group_ars/{uuid}")
    ar = project_result_folder_from_blob(knex, blob)
    return ar


sample_group_ar_from_uuid = project_result_folder_from_uuid  # Alias


@with_knex
def result_folder_from_uuid(knex, uuid):
    """Return a result folder object which the uuid points to.

    Guess the result folder is a sample result folder. If not, try a project result folder.
    """
    try:
        return sample_result_folder_from_uuid(knex, uuid)
    except GeoseeqNotFoundError:
        return project_result_folder_from_uuid(knex, uuid)


@with_knex
def sample_result_file_from_uuid(knex, uuid):
    """Return the sample result file object which the uuid points to."""
    blob = knex.get(f"sample_ar_fields/{uuid}")
    ar = sample_result_file_from_blob(knex, blob)
    return ar


sample_ar_field_from_uuid = sample_result_file_from_uuid  # Alias


@with_knex
def project_result_file_from_uuid(knex, uuid):
    """Return the project result file object which the uuid points to."""
    blob = knex.get(f"sample_group_ar_fields/{uuid}")
    ar = project_result_file_from_blob(knex, blob)
    return ar


sample_group_ar_field_from_uuid = project_result_file_from_uuid  # Alias


@with_knex
def result_file_from_uuid(knex, uuid):
    """Return a result file object which the uuid points to.

    Guess the result file is a sample result file. If not, try a project result file.
    """
    try:
        return sample_result_file_from_uuid(knex, uuid)
    except GeoseeqNotFoundError:
        return project_result_file_from_uuid(knex, uuid)
    

@with_knex
def pipeline_from_uuid(knex, uuid):
    """Return the pipeline object which the uuid points to."""
    blob = knex.get(f"pipelines/{uuid}")
    pipeline = pipeline_from_blob(knex, blob)
    return pipeline


app_from_uuid = pipeline_from_uuid  # Alias


@with_knex
def pipeline_run_from_uuid(knex, uuid):
    """Return a pipeline run object which the uuid points to."""
    blob = knex.get(f"app_runs/{uuid}")
    pipeline_run = pipeline_run_from_blob(knex, blob)
    return pipeline_run


@with_knex
def smart_table_from_uuid(knex, uuid):
    """Return a smart table object which the uuid points to."""
    blob = knex.get(f"table/{uuid}")
    table = smart_table_from_blob(knex, blob)
    return table
