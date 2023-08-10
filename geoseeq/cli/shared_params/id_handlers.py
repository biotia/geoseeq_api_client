
import uuid
from geoseeq.blob_constructors import (
    project_from_uuid,
    org_from_uuid,
    sample_from_uuid,
    sample_result_folder_from_uuid,
    project_result_folder_from_uuid,
)
from geoseeq.knex import GeoseeqNotFoundError
from os.path import isfile
from .id_utils import *
from .obj_getters import (
    _get_org_and_proj,
    _get_org_proj_and_sample,
    _get_org_proj_sample_and_folder,
    _get_org_proj_and_folder,
)

    

def read_els_from_file(file):
    """Return a list of els from a file"""
    with open(file) as f:
        return [line for line in (l.strip() for l in f) if line]
    

def flatten_list_of_els_and_files(els):
    """Return a list of els with any files replaced by the contents of the file"""
    flattened = []
    for el in els:
        if isfile(el):
            flattened.extend(read_els_from_file(el))
        else:
            flattened.append(el)
    return flattened


def el_is_org_id(knex, el):
    """Return an organization object if `el` is an organization ID, otherwise return None"""
    if is_uuid(el):
        try:
            org = org_from_uuid(knex, el)
            return org
        except GeoseeqNotFoundError:
            return None
    elif is_grn(el):
        if ':organization:' in el:
            return org_from_uuid(knex, el.split(':')[-1])
        return None
    return None


def el_is_project_id(knex, el):
    """Return a project object if `el` is a project ID, otherwise return None"""
    try:
       return handle_project_id(knex, el)
    except (ValueError, GeoseeqNotFoundError):
        return None
    

def handle_project_id(knex, project_id, yes=False, private=True, create=True):
    """Return a project object
    
    Project ID must be one of the following types:
    - a UUID
    - an organization name and project name separated by a slash
    - a GeoSeeq Resource Number (GRN)

    If both an organization name and project name are provided, the project and organization will
    be created if they do not exist and the user confirms.
    """

    if '/' in project_id:
        org_name, project_name = project_id.split('/')
        _, project = _get_org_and_proj(knex, org_name, project_name, yes=yes, private=private, create=create)
        return project
    elif len(project_id) == 1:
        proj_uuid = project_id[0].split(':')[-1]  # this gives a UUID either way
        project = project_from_uuid(knex, proj_uuid)
        return project
    raise ValueError('project_id must be a UUID, an organization name and project name, or a GRN')


def handle_folder_id(knex, folder_id, yes=False, private=True, create=True):
    """Return a folder object, creating it if it does not exist and the user confirms

    Can be either a project or sample folder depending on context
    """
    if '/' in folder_id:
        tkns = folder_id.split('/')
        if len(tkns) == 4:  # org, proj, sample, folder
            org_name, project_name, sample_name, folder_name = tkns
            _, _, _, folder = _get_org_proj_sample_and_folder(knex, org_name, project_name, sample_name, folder_name, yes=yes, private=private, create=create)
        elif len(tkns) == 3:  # proj, sample, folder
            project_name, sample_name, folder_name = tkns
            _, _, folder = _get_org_proj_and_folder(knex, project_name, sample_name, folder_name, yes=yes, private=private, create=create)
        else:
            raise ValueError(f'Invalid folder ID: {folder_id}')
        return folder
    elif len(folder_id) == 1:
        folder_uuid = folder_id[0].split(':')[-1]  # this gives a UUID either way
        # we guess that this is a sample folder, TODO: use GRN if available
        try:
            folder = sample_result_folder_from_uuid(knex, folder_uuid)
        except GeoseeqNotFoundError:
            folder = project_result_folder_from_uuid(knex, folder_uuid)
        return folder
    raise ValueError('sample_folder_id must be a UUID, an organization name and project name, or a GRN')


def handle_multiple_sample_ids(knex, sample_ids, proj=None):
    """Return a list of fetched sample objects
    
    `sample_ids` may have three different structures:
     - the sole element may be a project ID of any type, samples will be fetched from that project
     - the first element may be a project ID of any type, followed by a list of sample ids of any type or sample names
     - No project ID is provided, and each element is a sample id of any type

    Any sample may in fact be a file containing sample IDs, in which case the file will be read line by line and

    If `one_project` is True, all samples must be from the same project
    """
    project_as_arg = bool(proj)
    if proj or (proj := el_is_project_id(knex, sample_ids[0])):
        # The first element is a project ID. Remaining els may be sample IDs or names
        if not project_as_arg and sample_ids:
            sample_ids = list(sample_ids)[1:]
        if len(sample_ids) == 0:
            return list(proj.get_samples(cache=False))
        else:
            samples = []
            for el in flatten_list_of_els_and_files(sample_ids):
                if is_grn_or_uuid(el):
                    el = el.split(':')[-1]
                    samples.append(sample_from_uuid(knex, el))
                else:  # assume it's a sample name
                    samples.append(proj.sample(el).get())
    else:
        # No project ID provided. Each element is a sample ID
        samples = []
        for el in flatten_list_of_els_and_files(sample_ids):
            if is_grn_or_uuid(el):
                el = el.split(':')[-1]
                samples.append(sample_from_uuid(knex, el))
            else:
                raise ValueError(f'"{el}" is not a valid sample ID. To use samples by name, provide a project ID first.')
    return samples


