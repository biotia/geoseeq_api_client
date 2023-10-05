from .sample import Sample
from .knex import GeoseeqInternalError, GeoseeqTimeoutError
from time import sleep, time
import json
from .id_constructors import sample_from_id
import pandas as pd


class Search:

    def __init__(self, knex, search_terms: [str]=None, search_uuid=None) -> None:
        self.knex = knex
        self.search_terms = search_terms or []
        self.search_has_been_run = False
        self.search_uuid = search_uuid
        self.search_result = None
    
    def add_search_term(self, search_term: str) -> None:
        self.search_terms.append(search_term)
    
    def run_search(self):
        """Call the search API, save the results in this object, and return this object."""
        self._run_search()
        self.search_has_been_run = True
        return self
    
    def _init_search(self) -> None:
        """Post a call to the search API and save the search UUID."""
        if self.search_uuid: return  # search has already been initialized
        url = 'search'
        result = self.knex.post(url, json={'clauses': self.search_terms})
        self.search_uuid = result['uuid']

    def _poll_search(self) -> None:
        """Check if the search has finished. If it has return the response.
        
        If the search is pending return None.
        If the search has failed raise an exception.
        """
        url = f'search_result/{self.search_uuid}'
        result = self.knex.get(url)
        status = result['status']
        if status in ['pending', 'working']:
            return None
        elif status == 'error':
            raise GeoseeqInternalError('search failed')
        elif status == 'success':
            return result['result']
        else:  # unexpected status
            raise GeoseeqInternalError(f'Unexpected status: {status}')
        
    def _run_search(self, timeout_ms=100000, poll_interval_ms=100, poll_backoff=1.2) -> None:
        """Run the search and save the result."""
        self._init_search()
        poll_start_time = time()
        while True:
            result = self._poll_search()
            if result:
                self.search_result = result
                return
            if time() - poll_start_time > timeout_ms / 1000:
                raise GeoseeqTimeoutError(f'Search timed out after {timeout_ms} ms.')
            sleep(poll_interval_ms / 1000)
            poll_interval_ms *= poll_backoff

    def sample_table(self) -> pd.DataFrame:
        """Return a pandas dataframe with sample metadata."""
        if not self.search_has_been_run:
            self.run_search()
        rows = []
        for sample_blob in self.search_result['samples']:
            blob = sample_blob['metadata']
            blob['uuid'] = sample_blob['uuid']
            blob['sample_name'] = sample_blob['name']
            rows.append(blob)
        return pd.DataFrame(rows)

    def sample_uuids(self) -> [str]:
        """Return a list of sample UUIDs matching the search terms."""
        if not self.search_has_been_run:
            self.run_search()
        return [sample_blob['uuid'] for sample_blob in self.search_result['samples']]

    def samples(self):
        """Yield samples matching the search terms."""
        if not self.search_has_been_run:
            self.run_search()
        for sample_uuid in self.sample_uuids():
            yield sample_from_id(self.knex, sample_uuid)