# pylint: disable=line-too-long
"""Shared helpers for grouping files into samples via the server-side
``bulk_upload/validate_filenames`` and ``bulk_upload/group_files`` endpoints.

These functions are the canonical client-side grouping flow used by
``geoseeq upload reads`` and (planned) ``geoseeq link reads``. They are
extracted here so both commands produce identical sample/field names.

The functions intentionally retain their ``click.echo``/``click.confirm``
I/O coupling because both consumers are click commands; decoupling the
confirmation prompt is a separate concern.
"""
import click
import pandas as pd


def get_regex(knex, filepaths, module_name, lib, regex):
    """Return a regex that will group the files into samples.

    Tell the user how many files could not be matched using the regex.
    """
    _, seq_type = module_name.split('::')[:2]
    args = {
        'filenames': list(filepaths.keys()),
        'sequence_type': seq_type,
        'sample_group_id': lib.uuid,
    }
    if regex:
        args['custom_regex'] = regex
    result = knex.post('bulk_upload/validate_filenames', json=args)
    regex = result['regex_used']
    click.echo(f'Using regex: "{regex}"', err=True)
    if result['unmatched']:
        click.echo(f'{len(result["unmatched"])} files could not be grouped.', err=True)
    else:
        click.echo('All files successfully grouped.', err=True)
    return regex


def group_files(knex, filepaths, module_name, regex, yes, name_map=None):
    """Group the files into samples, confirm, and return the groups."""
    name_map_lookup = None
    if name_map is not None:
        name_map_filename, cur_col, new_col = name_map
        name_map_lookup = pd.read_csv(name_map_filename)[[cur_col, new_col]]
        name_map_lookup = name_map_lookup.set_index(cur_col)[new_col].to_dict()
    seq_length, seq_type = module_name.split('::')[:2]
    groups = knex.post('bulk_upload/group_files', json={
        'filenames': list(filepaths.keys()),
        'sequence_type': seq_type,
        'regex': regex
    })
    for group in groups:
        sample_name = group["sample_name"]
        if name_map_lookup:
            sample_name = name_map_lookup.get(sample_name, sample_name)
            group["sample_name"] = sample_name
        click.echo(f'sample_name: {sample_name}', err=True)
        click.echo(f'  module_name: {module_name}', err=True)
        for field_name, filename in group['fields'].items():
            path = filepaths[filename]
            click.echo(f'    {seq_length}::{field_name}: {path}', err=True)
    if not yes:
        click.confirm('Do you want to upload these files?', abort=True)
    return groups
