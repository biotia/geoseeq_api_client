
import uuid
from geoseeq.blob_constructors import (
    project_from_uuid,
    org_from_uuid,
    sample_from_uuid,
    sample_result_folder_from_uuid,
    project_result_folder_from_uuid,
    pipeline_from_uuid,
    pipeline_from_blob,
)
from geoseeq.knex import GeoseeqNotFoundError
from os.path import isfile
from geoseeq.id_constructors.utils import is_grn, is_uuid, is_grn_or_uuid
from .obj_getters import (
    _get_org,
    _get_org_and_proj,
    _get_org_proj_and_sample,
    _get_org_proj_sample_and_folder,
    _get_org_proj_and_folder,
)
from geoseeq.id_constructors import (
    result_file_from_uuid,
    result_file_from_name,
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
    

def handle_org_id(knex, org_id, yes=False, create=True):
    """Return an organization object.
    
    Organization ID must be one of the following types:
    - a UUID
    - an organization name
    - a GeoSeeq Resource Number (GRN)

    If the organization name is provided, the organization will be created if it does not exist
    """
    if is_grn_or_uuid(org_id):
        org_id = org_id.split(':')[-1]  # this gives a UUID either way
        org = org_from_uuid(knex, org_id)
        return org
    org = _get_org(knex, org_id, yes=yes, create=create)
    return org
    

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
    elif is_grn_or_uuid(project_id):
        proj_uuid = project_id.split(':')[-1]  # this gives a UUID either way
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
    elif is_grn_or_uuid(folder_id):
        folder_uuid = folder_id.split(':')[-1]  # this gives a UUID either way
        # we guess that this is a sample folder, TODO: use GRN if available
        try:
            folder = sample_result_folder_from_uuid(knex, folder_uuid)
        except GeoseeqNotFoundError:
            folder = project_result_folder_from_uuid(knex, folder_uuid)
        return folder
    raise ValueError('sample_folder_id must be a UUID, an organization name and project name, or a GRN')


def map_alternate_ids_to_uuids(proj, alternate_id_col, sample_ids):
    """Return a list of sample UUIDs
    
    `proj` is a project object
    `alternate_id_col` is the name of the column containing alternate IDs
    `sample_ids` is a list of alternate IDs
    """
    metadata = proj.get_sample_metadata()
    if alternate_id_col not in metadata:
        raise ValueError(f'Column "{alternate_id_col}" not found in project metadata')
    alt_col_df = metadata[["uuid", alternate_id_col]]
    # filter to the alt ids in our list- it is possible alt_id_col as a whole is not
    # unique but that our list of alt ids is
    alt_col_df = alt_col_df[alt_col_df[alternate_id_col].isin(sample_ids)]
    if alt_col_df.shape[0] == 0:
        raise ValueError(f'No samples found with the given alternate IDs in list')
    if alt_col_df.shape[0] < len(sample_ids):
        raise ValueError(f'Not all alternate IDs in list are found')
    if alt_col_df.shape[0] > len(sample_ids):
        raise ValueError(f'More than one sample found with the same alternate ID')
    return list(alt_col_df['uuid'])


def handle_multiple_sample_ids(knex, sample_ids, proj=None, alternate_id_col=None):
    """Return a list of fetched sample objects
    
    `sample_ids` may have three different structures:
     - the sole element may be a project ID of any type, samples will be fetched from that project
     - the first element may be a project ID of any type, followed by a list of sample ids of any type or project
     - No project ID is provided, and each element is a sample id of any type

    Any sample may in fact be a file containing sample IDs, in which case the file will be read line by line
    and each element will be a sample ID

    If `proj` is provided then `alternate_id_col` may also be provided.
    If so then alternate IDs will be used to fetch samples. If alternate ids are 
    not present or not unique then fail.
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
            sample_ids = flatten_list_of_els_and_files(sample_ids)
            if alternate_id_col:
                sample_ids = map_alternate_ids_to_uuids(proj, alternate_id_col, sample_ids)
            for el in sample_ids:
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


def handle_pipeline_id(knex, pipeline_id):
    """Return a pipeline object.
    
    `pipeline_id` may be a UUI, GRN,  or a pipeline name.
    """
    if is_grn_or_uuid(pipeline_id):
        pipeline_uuid = pipeline_id.split(':')[-1]
        pipeline = pipeline_from_uuid(knex, pipeline_uuid)
        return pipeline
    # the pipeline ID is a name, the only way to get it is to fetch all pipelines and search
    pipelines = list(knex.get('pipelines')['results'])
    for pipeline in pipelines:
        if pipeline['name'] == pipeline_id:
            return pipeline_from_blob(knex, pipeline)
    raise ValueError(f'Pipeline "{pipeline_id}" not found')


def handle_project_or_sample_id(knex, proj_or_sample_id, yes=False, private=True, create=True):
    """Return a project or sample object
    
    `proj_or_sample_id` may be a project ID or a sample ID
    """
    if '/' in proj_or_sample_id:
        tkns = proj_or_sample_id.split('/')
        if len(tkns) == 3:
            org_name, project_name, sample_name = tkns
            _, _, sample = _get_org_proj_and_sample(knex, org_name, project_name, sample_name, yes=yes, private=private, create=create)
            return sample
        elif len(tkns) == 2:
            org_name, project_name = tkns
            _, proj = _get_org_and_proj(knex, org_name, project_name, yes=yes, create=create, private=private)
            return proj
    elif is_grn_or_uuid(proj_or_sample_id):
        proj_or_sample_id = proj_or_sample_id.split(':')[-1]
        try:
            return sample_from_uuid(knex, proj_or_sample_id)
        except GeoseeqNotFoundError:
            return project_from_uuid(knex, proj_or_sample_id)
    raise ValueError(f'ID must be a UUID, path, or a GRN')


def handle_multiple_result_file_ids(knex, result_file_ids):
    """Return a list of fetched result file objects
    
    `result_file_ids` is a list of SampleResultFile and ProjectResultFile ids, names, or a mix of both

    Any result file id may in fact be a file containing result file IDs, in which case the file will be read line by line
    and each element will be a result file ID
    """
    result_file_ids = flatten_list_of_els_and_files(result_file_ids)
    result_files = []
    for result_id in result_file_ids:
        # we guess that this is a sample file to start, TODO: use GRN if available
        if "/" in result_id:  # result name/path
            result_file = result_file_from_name(knex, result_id)
        else:  # uuid or grn
            result_uuid = result_id.split(':')[-1]
            result_file = result_file_from_uuid(knex, result_uuid)
        result_files.append(result_file)
    return result_files