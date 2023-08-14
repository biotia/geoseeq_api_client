import click

dryrun_option = click.option('--dryrun/--wetrun', default=False, help='Print what will be created without actually creating it')
overwrite_option = click.option('--overwrite/--no-overwrite', default=False, help='Overwrite existing samples, files, and data')
module_option = lambda x: click.option('-m', '--module-name', type=click.Choice(x), default=x[0], help='Name for the module that will store the data')
private_option = click.option('--private/--public', default=True, help='Make objects private (default) or public')
link_option = click.option(
    '--link-type',
    default='upload',
    type=click.Choice(['upload', 's3', 'ftp', 'azure', 'sra']),
    help='Link the files from a cloud storage service instead of copying them'
)
yes_option = click.option('--yes/--confirm', default=False, help='Skip confirmation prompts')
sample_manifest_option = click.option(
    "--sample-manifest", type=click.File("r"), help="List of sample names to download from"
)
org_arg = click.argument('org_name')
project_arg = click.argument('project_name')
sample_arg = click.argument('sample_name')
module_arg = click.argument('module_name')
field_name = click.argument('field_name')
org_id_arg = click.argument('org_id', nargs=1)
project_id_arg = click.argument('project_id', nargs=1)
sample_ids_arg = click.argument('sample_ids', nargs=-1)
folder_id_arg = click.argument('folder_id', nargs=1)