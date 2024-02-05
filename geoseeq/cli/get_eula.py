import click
import json
from .shared_params import use_common_state


def eula_section_to_markdown(section):
    """Return a markdown string"""
    out = ""
    if section.get('title', None):
        out += "## {}\n\n".format(section['title'])
    for paragraph in section.get('paragraphs', []):
        if section.get('as_list', False):
            out += "* {}\n".format(paragraph)
        else:
            out += "{}\n\n".format(paragraph)
    return out


@click.group('eula')
def cli_eula():
    """Commands for working with the EULA."""
    pass

@cli_eula.command('show')
@use_common_state
def cli_get_eula(state):
    """Write the EULA to stdout as a markdown document."""
    knex = state.get_knex()
    eula_blob = knex.get('eula')
    out = "# GeoSeeq End User License Agreement\n\n"
    for section in eula_blob['sections']:
        out += eula_section_to_markdown(section)
    out += "Effective Date: {}\n\n".format(eula_blob['effective_date'])
    print(out, file=state.outfile)


