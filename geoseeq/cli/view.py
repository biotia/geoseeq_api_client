import click

from geoseeq import Organization, App
from geoseeq.id_constructors import resolve_id
from .shared_params import (
    use_common_state,
    project_id_arg,
    sample_ids_arg,
    yes_option,
    private_option,
    org_id_arg,
    handle_project_id,
    handle_multiple_sample_ids,
    handle_org_id,
)
from geoseeq.blob_constructors import org_from_uuid


@click.group('view')
def cli_view():
    """View objects on GeoSeeq."""
    pass


output_type_opt = click.option('--output-type', type=click.Choice(['uuid', 'name', 'all']), default='all', help='Type of output to print. Defaults to "all".')


def get_obj_output(obj, output_type):
    if output_type == 'uuid':
        return obj.uuid
    elif output_type == 'name':
        return obj.name
    else:
        return f'"{obj.name}"\t{obj.uuid}'


@cli_view.command('samples')
@use_common_state
@output_type_opt
@project_id_arg
def cli_list_samples(state, output_type, project_id):
    """Print a list of samples in the specified project.

    ---

    Example Usage:

    \b
    # List samples in "My Org/My Project"
    $ geoseeq view samples "My Org/My Project"

    ---

    Command Arguments:

    [PROJECT_ID] is the name or ID of the project to list samples from.

    ---
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, create=False)
    for sample in proj.get_samples():
        print(get_obj_output(sample, output_type), file=state.outfile)


@cli_view.command('organizations')
@use_common_state
@output_type_opt
def cli_list_organizations(state, output_type):
    """Print a list of organizations.

    ---

    Example Usage:

    \b
    # List all organizations
    $ geoseeq view organizations

    ---
    """
    knex = state.get_knex()
    for uuid in Organization.all_uuids(knex):
        if output_type == 'uuid':
            print(uuid, file=state.outfile)
        else:
            org = org_from_uuid(knex, uuid)
            print(get_obj_output(org, output_type), file=state.outfile)


@cli_view.command('projects')
@use_common_state
@output_type_opt
@org_id_arg
def cli_list_projects(state, output_type, org_id):
    """Print a list of projects in the specified organization.

    ---

    Example Usage:

    \b
    # List projects in "My Org"
    $ geoseeq view projects "My Org"

    ---

    Command Arguments:

    [ORG_ID] is the name or ID of the organization to list projects from.

    ---
    """
    knex = state.get_knex()
    org = handle_org_id(knex, org_id, create=False)
    for proj in org.get_projects():
        print(get_obj_output(proj, output_type), file=state.outfile)


@cli_view.command('app')
@use_common_state
@click.argument('uuid')
def cli_view_app(state, uuid):
    """Print the specified app.

    ---

    Example Usage:

    \b
    # Print the app with UUID "d051ce05-f799-4aa7-8d8f-5cbf99136543"
    $ geoseeq view app d051ce05-f799-4aa7-8d8f-5cbf99136543

    ---

    Command Arguments:

    [UUID] is the UUID of the app to print.

    ---
    """
    knex = state.get_knex()
    app = App(knex, uuid)
    app.get()
    print(app)
    for field_name, field_value, optional in app.get_remote_fields():
        optional = "Optional" if optional else "Required"
        print(f'\t{field_name} :: "{field_value}" ({optional})')


@cli_view.command('project')
@use_common_state
@project_id_arg
def cli_view_project(state, project_id):
    """Print the specified project.

    ---

    Example Usage:

    \b
    # Print the project with ID "My Org/My Project"
    $ geoseeq view project "My Org/My Project"

    ---

    Command Arguments:

    [PROJECT_ID] is the name or ID of the project to print.

    ---
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, create=False)
    print(proj)
    for field_name, field_value, optional in proj.get_remote_fields():
        optional = "Optional" if optional else "Required"
        print(f'\t{field_name} :: "{field_value}" ({optional})')


@cli_view.command('sample')
@use_common_state
@project_id_arg
@sample_ids_arg
def cli_view_sample(state, project_id, sample_ids):
    """Print the specified sample.

    ---

    Example Usage:

    \b
    # Print the sample with ID "My Sample" from "My Org/My Project/
    $ geoseeq view sample "My Org/My Project" "My Sample"

    ---

    Command Arguments:

    [PROJECT_ID] is the name or ID of the project to print the sample from.

    [SAMPLE_IDS]... is the name or ID of the sample to print.

    ---
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, create=False)
    sample_ids = handle_multiple_sample_ids(knex, sample_ids, proj)
    for sample in sample_ids:
        print(sample)
        for field_name, field_value, optional in sample.get_remote_fields():
            optional = "Optional" if optional else "Required"
            print(f'\t{field_name} :: "{field_value}" ({optional})')


@cli_view.command('object')
@use_common_state
@click.argument('ids', nargs=-1)
def cli_view_object(state, ids):
    """Print the specified object as well as its type.

    ---

    Example Usage:

    \b
    # Print the object with name "My Org/My Project/My Sample/My Result Folder"
    $ geoseeq view object "My Org/My Project/My Sample/My Result Folder"

    \b
    # Print the object with GRN "grn:geoseeq:sample::d051ce05-f799-4aa7-8d8f-5cbf99136543"
    $ geoseeq view object "grn:geoseeq:sample::d051ce05-f799-4aa7-8d8f-5cbf99136543"

    ---

    Command Arguments:

    [ID] is the name or GRN of the object to print. UUIDs cannot be resolved without a type.

    ---
    """
    knex = state.get_knex()
    for id in ids:
        obj_type, obj = resolve_id(knex, id)
        print(f'{obj_type}:\t{obj}', file=state.outfile)