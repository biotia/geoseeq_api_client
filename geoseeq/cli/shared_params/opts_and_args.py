import click

dryrun_option = click.option('--dryrun/--wetrun', default=False, help='Print what will be created without actually creating it')
overwrite_option = click.option('--overwrite/--no-overwrite', default=False, help='Overwrite existing samples, files, and data')
no_new_versions_option = click.option('--no-new-versions/--new-versions', default=False, help='Do not create new versions of the data')
def module_option(options, use_default=True, default=None):
    if use_default:
        default = default or options[0]
    if default and not use_default:
        raise ValueError('Cannot specify a default value without use_default=True')
    return click.option('-m', '--module-name', type=click.Choice(options), default=default, help='Name for the module that will store the data')

private_option = click.option('--private/--public', default=True, help='Make objects private (default) or public')
link_option = click.option(
    '--link-type',
    default='upload',
    type=click.Choice(['upload', 's3', 'ftp', 'azure', 'sra', 'http']),
    help='Link the files from a cloud storage service instead of copying them'
)
yes_option = click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
sample_manifest_option = click.option(
    "--sample-manifest", type=click.File("r"), help="List of sample names to download from"
)
ignore_errors_option = click.option('--ignore-errors/--no-ignore-errors', default=False, help='Ignore errors and continue')
org_arg = click.argument('org_name')
project_arg = click.argument('project_name')
sample_arg = click.argument('sample_name')
module_arg = click.argument('module_name')
field_name = click.argument('field_name')
org_id_arg = click.argument('org_id', nargs=1)
project_id_arg = click.argument('project_id', nargs=1)
pipeline_id_arg = click.argument('pipeline_id', nargs=1)
sample_ids_arg = click.argument('sample_ids', nargs=-1)
folder_id_arg = click.argument('folder_id', nargs=1)
folder_ids_arg = click.argument('folder_ids', nargs=-1)
project_or_sample_id_arg = click.argument('project_or_sample_id', nargs=1)
