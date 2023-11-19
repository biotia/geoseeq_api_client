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
    """Run a pipeline on a project."""
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
    """Get the status of a pipeline."""
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