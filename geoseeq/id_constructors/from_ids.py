from geoseeq import GeoseeqNotFoundError
from .from_uuids import (
    org_from_uuid,
    project_from_uuid,
    sample_from_uuid,
    sample_result_folder_from_uuid,
    project_result_folder_from_uuid,
    result_folder_from_uuid,
    sample_result_file_from_uuid,
    project_result_file_from_uuid,
    result_file_from_uuid,
)
from .from_names import (
    org_from_name,
    project_from_name,
    sample_from_name,
    sample_result_folder_from_name,
    project_result_folder_from_name,
    result_folder_from_name,
    sample_result_file_from_name,
    project_result_file_from_name,
    result_file_from_name,
)
from .utils import is_grn_or_uuid, is_name


def _generic_from_id(knex, id, from_uuid_func, from_name_func):
    """Return the object which the id points to."""
    if is_grn_or_uuid(id):
        return from_uuid_func(knex, id)
    if is_name(id):
        return from_name_func(knex, id)
    raise ValueError(f'"{id}" is not a GRN, UUID, or name')


def org_from_id(knex, id):
    """Return the organization object which the id points to."""
    return _generic_from_id(knex, id, org_from_uuid, org_from_name)


def project_from_id(knex, id):
    """Return the project object which the id points to."""
    return _generic_from_id(knex, id, project_from_uuid, project_from_name)


def sample_from_id(knex, id):
    """Return the sample object which the id points to."""
    return _generic_from_id(knex, id, sample_from_uuid, sample_from_name)


def sample_result_folder_from_id(knex, id):
    """Return the sample result folder object which the id points to."""
    return _generic_from_id(knex, id, sample_result_folder_from_uuid, sample_result_folder_from_name)


def project_result_folder_from_id(knex, id):
    """Return the project result folder object which the id points to."""
    return _generic_from_id(knex, id, project_result_folder_from_uuid, project_result_folder_from_name)


def result_folder_from_id(knex, id):
    """Return a result folder object which the id points to.
    
    Guess the result folder is a sample result folder. If not, try a project result folder.
    """
    return _generic_from_id(knex, id, result_folder_from_uuid, result_folder_from_name)


def sample_result_file_from_id(knex, id):
    """Return the sample result file object which the id points to."""
    return _generic_from_id(knex, id, sample_result_file_from_uuid, sample_result_file_from_name)


def project_result_file_from_id(knex, id):
    """Return the project result file object which the id points to."""
    return _generic_from_id(knex, id, project_result_file_from_uuid, project_result_file_from_name)


def result_file_from_id(knex, id):
    """Return a result file object which the id points to.
    
    Guess the result file is a sample result file. If not, try a project result file.
    """
    return _generic_from_id(knex, id, result_file_from_uuid, result_file_from_name)



