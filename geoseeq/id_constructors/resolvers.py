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
from .utils import is_name, is_grn
from geoseeq.knex import with_knex


@with_knex
def resolve_id(knex, id):
    """Return the object which the id points to."""
    if is_grn(id):
        return resolve_grn(knex, id)
    if is_name(id):
        return resolve_name(knex, id)
    raise ValueError(f'"{id}" is not a GRN, or name. UUIDs cannot be resolved without a type')


@with_knex
def resolve_name(knex, name):
    """Return the object which the name points to."""
    assert is_name(name), f'"{name}" is not a name'
    tkns = name.split("/")
    if len(tkns) == 1:  # org
        return "org", org_from_name(knex, name)
    if len(tkns) == 2:  # project
        return "project", project_from_name(knex, name)
    if len(tkns) == 3:  # sample or project result folder
        try:
            return "sample", sample_from_name(knex, name)
        except GeoseeqNotFoundError:
            return "folder", result_folder_from_name(knex, name)
    if len(tkns) == 4:  # sample result folder or project result file
        try:
            return "folder", result_folder_from_name(knex, name)
        except GeoseeqNotFoundError:
            return "file", result_file_from_name(knex, name)
    if len(tkns) == 5:  # sample result file
        return "file", result_file_from_name(knex, name)
    raise GeoseeqNotFoundError(f'Name "{name}" not found')


@with_knex
def resolve_grn(knex, grn):
    """Return the object which the grn points to."""
    assert is_grn(grn), f'"{grn}" is not a GRN'
    _, _, object_type, uuid = grn.split(':')
    if object_type == 'org':
        return object_type, org_from_uuid(knex, uuid)
    if object_type == 'project':
        return object_type, project_from_uuid(knex, uuid)
    if object_type == 'sample':
        return object_type, sample_from_uuid(knex, uuid)
    if object_type == 'folder':
        return object_type, result_folder_from_uuid(knex, uuid)
    if object_type == 'file':
        return object_type, result_file_from_uuid(knex, uuid)
    raise GeoseeqNotFoundError(f'Type "{object_type}" not found')