
from .organization import Organization
from .sample_group import SampleGroup
from .sample import Sample
from .analysis_result import SampleAnalysisResult

from functools import lru_cache


def org_from_blob(knex, blob, already_fetched=True, modified=False):
    org = Organization(knex, blob['name'])
    org.load_blob(blob)
    org._already_fetched = already_fetched
    org._modified = modified
    return org


def sample_group_from_blob(knex, blob, already_fetched=True, modified=False):
    org = org_from_blob(
        knex, blob['organization_obj'],
        already_fetched=already_fetched, modified=modified
    )
    grp = SampleGroup(knex, org, blob['name'], is_library=blob['is_library'])
    grp.load_blob(blob)
    grp._already_fetched = already_fetched
    grp._modified = modified
    return grp


def sample_from_blob(knex, blob, already_fetched=True, modified=False):
    lib = sample_group_from_blob(
        knex, blob['library_obj'],
        already_fetched=already_fetched, modified=modified
    )
    sample = Sample(knex, lib, blob['name'], metadata=blob['metadata'])
    sample.load_blob(blob)
    sample._already_fetched = already_fetched
    sample._modified = modified
    return sample


def sample_ar_from_blob(knex, blob, already_fetched=True, modified=False):
    sample = sample_from_blob(
        knex, blob['sample_obj'],
        already_fetched=already_fetched, modified=modified
    )
    ar = SampleAnalysisResult(knex, sample, blob['module_name'], replicate=blob['replicate'], metadata=blob['metadata'])
    ar.load_blob(blob)
    ar._already_fetched = already_fetched
    ar._modified = modified
    return ar


def sample_group_from_uuid(knex, uuid):
    blob = knex.get(f'sample_groups/{uuid}')
    sample = sample_group_from_blob(knex, blob)
    return sample


def sample_from_uuid(knex, uuid):
    blob = knex.get(f'samples/{uuid}')
    sample = sample_from_blob(knex, blob)
    return sample

def sample_ar_from_uuid(knex, uuid):
    blob = knex.get(f'sample_ars/{uuid}')
    ar = sample_ar_from_blob(knex, blob)
    return ar
