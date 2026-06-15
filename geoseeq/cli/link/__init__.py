"""``geoseeq link`` CLI group.

Commands for registering files that already live at rest in external
storage (today: S3) as geoseeq result-file links — i.e. no byte transfer
from the client. The mental model is "register, don't push", which is
different enough from ``geoseeq upload`` to deserve its own verb.
"""
import click

from .link_reads import cli_link_reads


@click.group("link")
def cli_link():
    """Register files already at rest in cloud storage as GeoSeeq result-file links."""
    pass


cli_link.add_command(cli_link_reads)
