from geoseeq import GeoseeqNotFoundError
from geoseeq.organization import Organization
from geoseeq.pipeline import PipelineRun
from geoseeq.project import Project
from geoseeq.result import (
    ProjectResultFile,
    ProjectResultFolder,
    SampleResultFile,
    SampleResultFolder,
)
from geoseeq.sample import Sample


def org_from_blob(knex, blob, already_fetched=True, modified=False):
    """Return an Organization object from a blob."""
    org = Organization(knex, blob["name"])
    org.load_blob(blob)
    org._already_fetched = already_fetched
    org._modified = modified
    return org


def project_from_blob(knex, blob, already_fetched=True, modified=False):
    """Return a Project object from a blob."""
    org = org_from_blob(
        knex, blob["organization_obj"], already_fetched=already_fetched, modified=modified
    )
    grp = Project(knex, org, blob["name"], is_library=blob["is_library"])
    grp.load_blob(blob)
    grp._already_fetched = already_fetched
    grp._modified = modified
    return grp


sample_group_from_blob = project_from_blob  # Alias


def sample_from_blob(knex, blob, already_fetched=True, modified=False):
    """Return a Sample object from a blob."""
    lib = sample_group_from_blob(
        knex, blob["library_obj"], already_fetched=already_fetched, modified=modified
    )
    sample = Sample(knex, lib, blob["name"], metadata=blob["metadata"])
    sample.load_blob(blob)
    sample._already_fetched = already_fetched
    sample._modified = modified
    return sample


def project_result_folder_from_blob(knex, blob, already_fetched=True, modified=False):
    """Return a ProjectResultFolder object from a blob."""
    group = project_from_blob(
        knex, blob["sample_group_obj"], already_fetched=already_fetched, modified=modified
    )
    ar = ProjectResultFolder(
        knex, group, blob["module_name"], replicate=blob["replicate"], metadata=blob["metadata"]
    )
    ar.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return ar


sample_group_ar_from_blob = project_result_folder_from_blob  # Alias


def sample_result_folder_from_blob(knex, blob, already_fetched=True, modified=False):
    """Return a SampleResultFolder object from a blob."""
    sample = sample_from_blob(
        knex, blob["sample_obj"], already_fetched=already_fetched, modified=modified
    )
    ar = SampleResultFolder(
        knex, sample, blob["module_name"], replicate=blob["replicate"], metadata=blob["metadata"]
    )
    ar.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return ar


sample_ar_from_blob = sample_result_folder_from_blob  # Alias


def sample_result_file_from_blob(knex, blob, already_fetched=True, modified=False):
    """Return a SampleResultFile object from a blob."""
    ar = sample_result_folder_from_blob(
        knex, blob["analysis_result_obj"], already_fetched=already_fetched, modified=modified
    )
    arf = SampleResultFile(knex, ar, blob["name"], data=blob["stored_data"])
    arf.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return arf


sample_ar_field_from_blob = sample_result_file_from_blob  # Alias


def project_result_file_from_blob(knex, blob, already_fetched=True, modified=False):
    """Return a ProjectResultFile object from a blob."""
    ar = project_result_folder_from_blob(
        knex, blob["analysis_result_obj"], already_fetched=already_fetched, modified=modified
    )
    arf = ProjectResultFile(knex, ar, blob["name"], data=blob["stored_data"])
    arf.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return arf


sample_group_ar_field_from_blob = project_result_file_from_blob  # Alias


def pipeline_run_from_blob(knex, blob, already_fetched=True, modified=False):
    """Return a Pipeline run object from a blob."""
    pipeline_run = PipelineRun(
        knex,
        blob["sample_group"],
        blob["pipeline"],
        pipeline_version=blob["pipeline_version"],
    )
    pipeline_run.load_blob(blob)

    pipeline_run._already_fetched = already_fetched
    pipeline_run._modified = modified
    return pipeline_run
