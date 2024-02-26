"""Construct objects from names.

This module is used to construct objects from names. It is used by the

Names are human-readable ids that are used to identify objects in GeoSeeq.
They similar to file paths, e.g.:
 - "My Org"
 - "My Org/My Project"
 - "My Org/My Project/My Sample/My Result Folder/My Result File"
 - "My Org/My Project/My Result Folder/My Result File"

Unlike UUIDs or GRNs names can be changed by the user.
This makes them more human-readable, but not reliable for permanent references.
"""
from geoseeq.organization import Organization
from geoseeq import GeoseeqNotFoundError
from geoseeq.knex import with_knex


@with_knex
def org_from_name(knex, name):
    """Return the organization object which the name points to.
    
    e.g. "My Org"
    """
    tkns = name.split("/")
    org_name = tkns[0]
    org = Organization(knex, org_name)
    org.get()
    return org


@with_knex
def project_from_name(knex, name):
    """Return the project object which the name points to.
    
    e.g. "My Org/My Project"
    """
    tkns = name.split("/")
    proj_name = tkns[1]
    org = org_from_name(knex, name)
    proj = org.project(proj_name)
    proj.get()
    return proj


@with_knex
def sample_from_name(knex, name):
    """Return the sample object which the name points to.
    
    e.g. "My Org/My Project/My Sample"
    """
    tkns = name.split("/")
    sample_name = tkns[2]
    proj = project_from_name(knex, name)
    sample = proj.sample(sample_name)
    sample.get()
    return sample


@with_knex
def sample_result_folder_from_name(knex, name):
    """Return the sample result folder object which the name points to.
    
    e.g. "My Org/My Project/My Sample/My Result Folder"
    """
    tkns = name.split("/")
    result_folder_name = tkns[3]
    sample = sample_from_name(knex, name)
    ar = sample.result_folder(result_folder_name)
    ar.get()
    return ar


@with_knex
def project_result_folder_from_name(knex, name):
    """Return the project result folder object which the name points to.
    
    e.g. "My Org/My Project/My Result Folder"
    """
    tkns = name.split("/")
    result_folder_name = tkns[2]
    proj = project_from_name(knex, name)
    rf = proj.result_folder(result_folder_name)
    rf.get()
    return rf


@with_knex
def result_folder_from_name(knex, name):
    """Return a result folder object which the name points to.
    
    Guess the result folder is a sample result folder. If not, try a project result folder.
    """
    tkns = name.split("/")
    if len(tkns) >= 4:
        try:
            return sample_result_folder_from_name(knex, name)
        except GeoseeqNotFoundError:
            return project_result_folder_from_name(knex, name)
    else:  # can't be a sample result folder
        return project_result_folder_from_name(knex, name)


@with_knex
def sample_result_file_from_name(knex, name):
    """Return the sample result file object which the name points to.
    
    e.g. "My Org/My Project/My Sample/My Result Folder/My Result File"
    """
    tkns = name.split("/")
    result_file_name = tkns[4]
    r_folder = sample_result_folder_from_name(knex, name)
    r_file = r_folder.result_file(result_file_name)
    r_file.get()
    return r_file


@with_knex
def project_result_file_from_name(knex, name):
    """Return the project result file object which the name points to.
    
    e.g. "My Org/My Project/My Result Folder/My Result File"
    """
    tkns = name.split("/")
    result_file_name = tkns[3]
    r_folder = project_result_folder_from_name(knex, name)
    r_file = r_folder.result_file(result_file_name)
    r_file.get()
    return r_file


@with_knex
def result_file_from_name(knex, name):
    """Return a result file object which the name points to.
    
    Guess the result file is a sample result file. If not, try a project result file.
    """
    tkns = name.split("/")
    if len(tkns) >= 5:
        try:
            return sample_result_file_from_name(knex, name)
        except GeoseeqNotFoundError:
            return project_result_file_from_name(knex, name)
    else:  # can't be a sample result file
        return project_result_file_from_name(knex, name)
