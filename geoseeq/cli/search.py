import click
from .shared_params import (
    use_common_state,
)
from geoseeq.search import Search



@click.command('search')
@use_common_state
@click.option('--sep', default='\t')
@click.argument('search_terms', nargs=-1)
def cli_search(state, sep, search_terms):
    """Run a search and write out a list of sample IDs.
    
    ---

    Example Usage:

    \b
    # Search for samples in London
    $ geoseeq search 'city: london'

    \b
    # Search for samples in london with Staphylolococcus
    $ geoseeq search 'city: london' 'staphylococcus'

    ---

    Command Arguments:

    \b
    [SEARCH_TERMS]... a list of search terms to search for

    ---
    
    Use of this tool implies acceptance of the GeoSeeq End User License Agreement.
    Run `geoseeq eula show` to view the EULA.
    """
    search = Search(state.get_knex())
    for search_term in search_terms:
        search.add_search_term(search_term)
    search.run_search()
    print(search.sample_table().to_csv(sep=sep), file=state.outfile)