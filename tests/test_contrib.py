"""Test suite for experimental functions."""
import json
import random
from os import environ
from os.path import dirname, join
from unittest import TestCase

from geoseeq_api import Knex, Organization
from geoseeq_api.contrib.tagging import Tag

PACKET_DIR = join(dirname(__file__), 'built_packet')
ENDPOINT = environ.get('GEOSEEQ_API_TESTING_ENDPOINT', 'http://127.0.0.1:8000')


def random_str(len=12):
    """Return a random alphanumeric string of length `len`."""
    out = random.choices('abcdefghijklmnopqrtuvwxyzABCDEFGHIJKLMNOPQRTUVWXYZ0123456789', k=len)
    return ''.join(out)


class TestPangeaApiContribClient(TestCase):
    """Test suite for packet building."""

    def setUp(self):
        self.knex = Knex(ENDPOINT)
        # Creates a test user and an API token for the user in database. Returns the token.
        api_token = self.knex.post('/users/test-user', json={'email': f'{random_str()}@gmail.com'})
        self.knex.add_api_token(api_token)

    def test_create_tag(self):
        """Test that we can create an tag."""
        tag = Tag(self.knex, f'my_client_test_tag {random_str()[:4]}')
        tag.create()
        self.assertTrue(tag.uuid)

    def test_tag_sample(self):
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        # N.B. It should NOT be necessary to call <parent>.create()
        samp = grp.sample(f'my_client_test_sample {key}')
        samp.create()
        tag = Tag(self.knex, f'my_client_test_tag {key}')

        tag(samp, payload=json.dumps({'foo': 'bar', 'a': 2}))
        tagged_samples = list(tag.get_samples())
        tagged_uuids = {s.uuid for s in tagged_samples}
        self.assertIn(samp.uuid, tagged_uuids)
