import click
from .shared_params import (
    use_common_state,
    project_id_arg,
    sample_ids_arg,
    yes_option,
    private_option,
    handle_project_id,
    handle_multiple_sample_ids,
)
from geoseeq import Organization


@click.group('manage')
def cli_manage():
    """Manage GeoSeeq objects, add, delete, etc."""
    pass


@cli_manage.command('add-samples-to-project')
@use_common_state
@yes_option
@project_id_arg
@sample_ids_arg
def cli_add_samples(state, yes, project_id, sample_ids):
    """Add samples to a project.

    ---

    Example Usage:

    \b
    # Add two samples to "My Org/My Project"
    $ geoseeq manage add-samples-to-project "My Org/My Project" c8141b43-398a-4ab5-a8cf-0aa1fd902a95 553ba5fc-5f8c-45a4-a057-3cf4b310c53d

    \b
    # Add all samples in "My Org/My Source Project" to "My Org/My Target Project"
    $ geoseeq manage add-samples-to-project "My Org/My Target Project" "My Org/My Source Project"

    ---

    Command Arguments:

    \b
    [PROJECT_ID] is the name or ID of the project to add samples to.

    \b
    [SAMPLE_IDS]... can be a list of sample names or IDs, files containing a list of sample names or IDs, or a mix of both.
    The first element in the list can optionally be a project ID or name.
    If a project ID is not provided, then sample ids must be UUIDs or GRNs, not names.
    If only a project ID is provided, then all samples in the project will be added.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes=yes)
    samples = handle_multiple_sample_ids(knex, sample_ids)
    click.echo(f'Adding {len(samples)} samples to {proj.name}', err=True)
    if not yes:
        click.confirm('Continue?', abort=True)
    for sample in samples:
        proj.add_sample(sample)
    proj.save()
    click.echo(f'Added {len(samples)} to {proj.name}', err=True)


@cli_manage.command('create-org')
@use_common_state
@click.option('--exists-ok/--no-exists-ok', default=True)
@yes_option
@click.argument('org_name')
def cli_create_org(state, exists_ok, yes, org_name):
    """Create an organization.
    
    ---

    Example Usage:

    \b
    # Create an organization named "My Org"
    $ geoseeq manage create-org "My Org"

    ---

    Command Arguments:

    \b
    [ORG_NAME] is the name of the organization to create.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex()
    if yes or click.confirm(f'Create Organization {org_name}?'):
        if exists_ok:
            org = Organization(knex, org_name).idem()
        else:
            org = Organization(knex, org_name).create()
        click.echo(f'Created Organization {org}', err=True)


@cli_manage.command('create-project')
@use_common_state
@yes_option
@private_option
@project_id_arg
def cli_create_project(state, yes, private, project_id):
    """Create a project.

    ---
    
    Example Usage:
    
    \b
    # Create a project named "My Project" in "My Org"
    $ geoseeq manage create-project "My Org/My Project"

    ---

    Command Arguments:

    \b
    [PROJECT_ID] is the name or ID of the project to create.

    ---
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes=yes, private=private)
    click.echo(f'Created Project {proj}', err=True)


@cli_manage.command('create-samples')
@use_common_state
@yes_option
@private_option
@project_id_arg
@click.argument('sample_names', nargs=-1)
def cli_create_samples(state, yes, private, project_id, sample_names):
    """Create samples in the specified project.
    
    ---
    
    Example Usage:
    
    \b
    # Create two samples in "My Org/My Project"
    $ geoseeq manage create-samples "My Org/My Project" "Sample 1" "Sample 2"

    ---

    Command Arguments:

    \b
    [PROJECT_ID] is the name or ID of the project to create samples in.

    \b
    [SAMPLE_NAMES]... is a list of sample names to create.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes=yes, private=private)
    for sample_name in sample_names:
        sample = proj.sample(sample_name).idem()
        click.echo(f'Created Sample {sample}', err=True)


@cli_manage.command('delete-samples')
@use_common_state
@yes_option
@sample_ids_arg
def cli_delete_samples(state, yes, sample_ids):
    """Delete samples by their ID or name.
    
    Confirm before deleting unless --yes is provided.

    ---

    Example Usage:

    \b
    # Delete two samples by ID
    $ geoseeq manage delete-samples c8141b43-398a-4ab5-a8cf-0aa1fd902a95 553ba5fc-5f8c-45a4-a057-3cf4b310c53d

    \b
    # Delete one samples by name
    $ geoseeq manage delete-samples "My Org/My Project" "My Sample"

    ---

    Command Arguments:

    \b
    [SAMPLE_IDS]... can be a list of sample names or IDs, files containing a list of sample names or IDs, or a mix of both.
    The first element in the list can optionally be a project ID or name.
    If a project ID is not provided, then sample ids must be UUIDs or GRNs, not names.
    If only a project ID is provided, then all samples in the project will be deleted.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex()
    samples = handle_multiple_sample_ids(knex, sample_ids)
    click.echo(f'Deleting {len(samples)} samples', err=True)
    if not yes:
        click.confirm('Continue?', abort=True)
    for sample in samples:
        sample.delete()
    click.echo(f'Deleted {len(samples)} samples', err=True)
