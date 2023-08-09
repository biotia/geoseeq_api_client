import click

from geoseeq import Organization
from geoseeq.blob_constructors import project_from_uuid
from geoseeq.knex import GeoseeqNotFoundError


def _get_org_and_proj(knex, org_name, project_name, yes, private):
    """Return an organization and project, creating them if necessary and desired."""
    org = Organization(knex, org_name).get()
    try:
        org = Organization(knex, org_name).get()
    except GeoseeqNotFoundError:
        if not yes:
            click.confirm(f'Organization "{org_name}" does not exist. Create it?', abort=True)
        org = Organization(knex, org_name).create()
    try:
        proj = org.sample_group(project_name).get()
    except GeoseeqNotFoundError:
        if not yes:
            click.confirm(f'Project "{project_name}" does not exist. Create it?', abort=True)
        proj = org.sample_group(project_name, is_public=not private).create()
    return org, proj


def handle_project_id(knex, project_id, yes, private):
    """Return a project object
    
    Project ID must be one of the following types:
    - a UUID
    - an organization name and project name (as seperate arguments)
    - a GeoSeeq Resource Number (GRN)

    If both an organization name and project name are provided, the project and organization will
    be created if they do not exist and the user confirms.
    """

    if len(project_id) == 2:
        org_name, project_name = project_id
        _, project = _get_org_and_proj(knex, org_name, project_name, yes, private)
        return project
    elif len(project_id) == 1:
        proj_uuid = project_id[0].split(':')[-1]  # this gives a UUID either way
        project = project_from_uuid(knex, proj_uuid)
        return project
    raise ValueError('project_id must be a UUID, an organization name and project name, or a GRN')