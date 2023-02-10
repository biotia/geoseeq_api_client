from .analysis_result import (
    SampleAnalysisResult,
    SampleGroupAnalysisResult,
    SampleAnalysisResultField,
    SampleGroupAnalysisResultField,
)
from .organization import Organization
from .sample import Sample
from .sample_group import SampleGroup
from .knex import GeoseeqNotFoundError


def org_from_blob(knex, blob, already_fetched=True, modified=False):
    org = Organization(knex, blob["name"])
    org.load_blob(blob)
    org._already_fetched = already_fetched
    org._modified = modified
    return org


def sample_group_from_blob(knex, blob, already_fetched=True, modified=False):
    org = org_from_blob(
        knex, blob["organization_obj"], already_fetched=already_fetched, modified=modified
    )
    grp = SampleGroup(knex, org, blob["name"], is_library=blob["is_library"])
    grp.load_blob(blob)
    grp._already_fetched = already_fetched
    grp._modified = modified
    return grp


def sample_from_blob(knex, blob, already_fetched=True, modified=False):
    lib = sample_group_from_blob(
        knex, blob["library_obj"], already_fetched=already_fetched, modified=modified
    )
    sample = Sample(knex, lib, blob["name"], metadata=blob["metadata"])
    sample.load_blob(blob)
    sample._already_fetched = already_fetched
    sample._modified = modified
    return sample


def sample_group_ar_from_blob(knex, blob, already_fetched=True, modified=False):
    group = sample_group_from_blob(
        knex, blob["sample_group_obj"], already_fetched=already_fetched, modified=modified
    )
    ar = SampleGroupAnalysisResult(
        knex, sample, blob["module_name"], replicate=blob["replicate"], metadata=blob["metadata"]
    )
    ar.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return ar


def sample_ar_from_blob(knex, blob, already_fetched=True, modified=False):
    sample = sample_from_blob(
        knex, blob["sample_obj"], already_fetched=already_fetched, modified=modified
    )
    ar = SampleAnalysisResult(
        knex, sample, blob["module_name"], replicate=blob["replicate"], metadata=blob["metadata"]
    )
    ar.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return ar


def sample_group_ar_from_blob(knex, blob, already_fetched=True, modified=False):
    group = sample_group_from_blob(
        knex, blob["sample_group_obj"], already_fetched=already_fetched, modified=modified
    )
    ar = SampleGroupAnalysisResult(
        knex, sample, blob["module_name"], replicate=blob["replicate"], metadata=blob["metadata"]
    )
    ar.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return ar


def sample_ar_field_from_blob(knex, blob, already_fetched=True, modified=False):
    ar = sample_ar_from_blob(
        knex, blob["analysis_result_obj"], already_fetched=already_fetched, modified=modified
    )
    arf = SampleAnalysisResultField(
        knex, ar, blob["name"], data=blob["stored_data"]
    )
    arf.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return arf


def sample_group_ar_field_from_blob(knex, blob, already_fetched=True, modified=False):
    ar = sample_group_ar_from_blob(
        knex, blob["analysis_result_obj"], already_fetched=already_fetched, modified=modified
    )
    arf = SampleGroupAnalysisResultField(
        knex, ar, blob["name"], data=blob["stored_data"]
    )
    arf.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return arf


def sample_group_from_uuid(knex, uuid):
    blob = knex.get(f"sample_groups/{uuid}")
    sample = sample_group_from_blob(knex, blob)
    return sample


def sample_from_uuid(knex, uuid):
    blob = knex.get(f"samples/{uuid}")
    sample = sample_from_blob(knex, blob)
    return sample


def sample_ar_from_uuid(knex, uuid):
    blob = knex.get(f"sample_ars/{uuid}")
    ar = sample_ar_from_blob(knex, blob)
    return ar


def sample_group_ar_from_uuid(knex, uuid):
    blob = knex.get(f"sample_group_ars/{uuid}")
    ar = sample_group_ar_from_blob(knex, blob)
    return ar


def sample_ar_field_from_uuid(knex, uuid):
    blob = knex.get(f"sample_ar_fields/{uuid}")
    ar = sample_ar_field_from_blob(knex, blob)
    return ar


def sample_group_ar_field_from_uuid(knex, uuid):
    blob = knex.get(f"sample_group_ar_fields/{uuid}")
    ar = sample_group_ar_field_from_blob(knex, blob)
    return ar


def resolve_brn(knex, brn):
    """Return the object which the brn points to."""
    assert brn.startswith(f'brn:{knex.instance_code()}:')
    _, _, object_type, uuid = brn.split(':')
    if object_type == 'project':
        return object_type, sample_group_from_uuid(knex, uuid)
    if object_type == 'sample':
        return object_type, sample_from_uuid(knex, uuid)
    if object_type == 'sample_result':
        return object_type, sample_ar_from_uuid(knex, uuid)
    if object_type == 'sample_result_field':
        return object_type, sample_ar_field_from_uuid(knex, uuid)
    if object_type == 'project_result':
        return object_type, sample_group_ar_from_uuid(knex, uuid)
    if object_type == 'project_result_field':
        return object_type, sample_group_ar_field_from_uuid(knex, uuid)
    raise GeoseeqNotFoundError(f'Type "{object_type}" not found')
