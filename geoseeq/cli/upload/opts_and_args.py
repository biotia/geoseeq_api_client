import click

dryrun_option = click.option('--dryrun/--wetrun', default=False, help='Print what will be created without actually creating it')
overwrite_option = click.option('--overwrite/--no-overwrite', default=False, help='Overwrite existing samples and data')
module_option = lambda x: click.option('-m', '--module-name', type=click.Choice(x), default=x[0], help='Name for the module that will store the data')
private_option = click.option('--private/--public', default=True, help='Make the reads private.')
link_option = click.option(
    '--link-type',
    default='upload',
    type=click.Choice(['upload', 's3', 'ftp', 'azure', 'sra']),
    help='Link the files from a cloud storage service instead of copying them'
)
org_arg = click.argument('org_name')
project_arg = click.argument('project_name')
sample_arg = click.argument('sample_name')
module_arg = click.argument('module_name')
field_name = click.argument('field_name')