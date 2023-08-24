import click
from geoseeq import Organization
from geoseeq.knex import GeoseeqNotFoundError



def _get_org(knex, org_name, yes, create=True):
    """Return an organization, creating it if necessary and desired."""
    try:
        org = Organization(knex, org_name).get()
    except GeoseeqNotFoundError:
        if not create:
            raise
        if not yes:
            click.confirm(f'Organization "{org_name}" does not exist. Create it?', abort=True)
        org = Organization(knex, org_name).create()
    return org


def _get_org_and_proj(knex, org_name, project_name, yes, private, create=True):
    """Return an organization and project, creating them if necessary and desired."""
    org = _get_org(knex, org_name, yes, create=create)
    try:
        proj = org.sample_group(project_name).get()
    except GeoseeqNotFoundError:
        if not create:
            raise
        if not yes:
            click.confirm(f'Project "{project_name}" does not exist. Create it?', abort=True)
        proj = org.sample_group(project_name, is_public=not private).create()
    return org, proj


def _get_org_proj_and_sample(knex, org_name, project_name, sample_name, yes, private, create=True):
    org, proj = _get_org_and_proj(knex, org_name, project_name, yes, private, create=create)
    try:
        sample = proj.sample(sample_name).get()
    except GeoseeqNotFoundError:
        if not create:
            raise
        if not yes:
            click.confirm(f'Sample "{sample_name}" does not exist. Create it?', abort=True)
        sample = proj.sample(sample_name).create()
    return org, proj, sample


def _get_org_proj_sample_and_folder(knex, org_name, project_name, sample_name, folder_name, yes, private, create=True):
    org, proj, sample = _get_org_proj_and_sample(knex, org_name, project_name, sample_name, yes, private, create=create)
    try:
        folder = sample.result_folder(folder_name).get()
    except GeoseeqNotFoundError:
        if not create:
            raise
        if not yes:
            click.confirm(f'Folder "{folder_name}" does not exist. Create it?', abort=True)
        folder = sample.result_folder(folder_name).create()
    return org, proj, sample, folder


def _get_org_proj_and_folder(knex, org_name, project_name, folder_name, yes, private, create=True):
    org, proj = _get_org_and_proj(knex, org_name, project_name, yes, private, create=create)
    try:
        folder = proj.result_folder(folder_name).get()
    except GeoseeqNotFoundError:
        if not create:
            raise
        if not yes:
            click.confirm(f'Folder "{folder_name}" does not exist. Create it?', abort=True)
        folder = proj.result_folder(folder_name).create()
    return org, proj, folder