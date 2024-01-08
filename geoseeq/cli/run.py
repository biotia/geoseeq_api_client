import click

from .shared_params import (
    use_common_state,
    project_id_arg,
    pipeline_id_arg,
    handle_project_id,
    handle_pipeline_id,
)
from geoseeq.pipeline import PipelineOption

@click.group('app')
def cli_app():
    """Run pipelines on GeoSeeq."""
    pass


def prompt_for_option(pipeline_opt: PipelineOption):
    """Prompt the user for an option."""
    if pipeline_opt.is_set:
        return pipeline_opt.input_val
    if pipeline_opt.type == "checkbox":
        return click.confirm(pipeline_opt.description, default=pipeline_opt.default_value)
    elif pipeline_opt.type == "text":
        return click.prompt(pipeline_opt.description, default=pipeline_opt.default_value)
    elif pipeline_opt.type == "select":
        return click.prompt(
            pipeline_opt.description,
            default=pipeline_opt.default_value,
            type=click.Choice(pipeline_opt.options),
        )
    else:
        raise ValueError(f"Unknown option type: {pipeline_opt.type}")


@cli_app.command('run')
@use_common_state
@click.option('-po', '--pipeline-option', multiple=True, nargs=2, help='Set a pipeline option.')
@pipeline_id_arg
@project_id_arg
def run_pipeline(state, pipeline_option, pipeline_id, project_id):
    """Run a pipeline on all samples in a project.

    Some pipelines require options to be set. This command will prompt you to supply those options.
    Alternatively, you can set options with the `-po` flag. You can see required options using this
    command.

    `geoseeq view app "Pipeline Name"`

    ---

    Example Usage:

    \b
    # Run the "My Pipeline" pipeline on all samples in "My Org/My Project"
    $ geoseeq app run "My Pipeline" "My Org/My Project"

    \b
    # Run the "My Pipeline" pipeline on all samples in "My Org/My Project", setting the "my_option" option to "my_value"
    $ geoseeq app run "My Pipeline" "My Org/My Project" -po my_option my_value

    ---

    Command Arguments:

    \b
    [PIPELINE_ID] is the name or ID of the pipeline to run.
    [PROJECT_ID] is the name or ID of the project to run the pipeline on.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex()
    project = handle_project_id(knex, project_id, create=False)
    pipeline = handle_pipeline_id(knex, pipeline_id)
    for opt_name, opt_val in pipeline_option:
        pipeline.set_option(opt_name, opt_val)
    for pipeline_opt in pipeline.options():
        pipeline_opt.set_input(prompt_for_option(pipeline_opt))
    project.run_app(pipeline)


@cli_app.command('status')
@use_common_state
@pipeline_id_arg
@project_id_arg
def get_pipeline_runs(state, pipeline_id, project_id):
    """See the status of all runs of a pipeline on a project.
    
    ---
    
    Example Usage:

    \b
    # See the status of all runs of "My Pipeline" on "My Org/My Project"
    $ geoseeq app status "My Pipeline" "My Org/My Project"

    ---

    Command Arguments:

    \b
    [PIPELINE_ID] is the name or ID of the pipeline to see the status of.
    [PROJECT_ID] is the name or ID of the project to see the status of.

    ---

    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    knex = state.get_knex()
    project = handle_project_id(knex, project_id)
    pipeline_uuid = handle_pipeline_id(knex, pipeline_id).uuid
    response = knex.get(f'app_runs?sample_group_id={project.uuid}')
    for el in response['results']:
        if el['pipeline'] == pipeline_uuid:
            out = [
                el['pipeline_obj']['name'],
                el['uuid'],
                el['status'],
                el['created_at'],
            ]
            print('\t'.join(out), file=state.outfile)