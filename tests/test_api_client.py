"""Test suite for experimental functions."""
import random
from os import environ
from os.path import join, dirname
from requests.exceptions import HTTPError
from unittest import TestCase, skip

from pangea_api import (
    Knex,
    Sample,
    Organization,
    SampleGroup,
    User,
    RemoteObjectError,
)

PACKET_DIR = join(dirname(__file__), 'built_packet')
ENDPOINT = environ.get('PANGEA_API_TESTING_ENDPOINT', 'http://127.0.0.1:8000')


def random_str(len=12):
    """Return a random alphanumeric string of length `len`."""
    out = random.choices('abcdefghijklmnopqrtuvwxyzABCDEFGHIJKLMNOPQRTUVWXYZ0123456789', k=len)
    return ''.join(out)


class TestPangeaApiClient(TestCase):
    """Test suite for packet building."""

    def setUp(self):
        self.knex = Knex(ENDPOINT)
        self.user = User(self.knex, 'foo@bar.com', 'Foobar22')
        try:
            self.user.register()
        except HTTPError:
            self.user.login()

    def test_create_org(self):
        """Test that we can create an org."""
        org = Organization(self.knex, f'my_client_test_org {random_str()}')
        org.create()
        self.assertTrue(org.uuid)

    def test_get_org(self):
        """Test that we can get an org."""
        name = f'my_client_test_org {random_str()}'
        Organization(self.knex, name).create()
        org = Organization(self.knex, name).get()
        self.assertTrue(org.uuid)

    def test_idem_create_org(self):
        """Test that we can create an org using idem."""
        org = Organization(self.knex, f'my_client_test_org {random_str()}')
        org.idem()
        self.assertTrue(org.uuid)

    def test_idem_get_org(self):
        """Test that we can get an org using idem."""
        name = f'my_client_test_org {random_str()}'
        Organization(self.knex, name).create()
        org = Organization(self.knex, name).idem()
        self.assertTrue(org.uuid)

    def test_create_group(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        # N.B. It should NOT be necessary to call org.create()
        grp = org.sample_group(f'my_client_test_grp {key}')
        grp.create()
        self.assertTrue(org.uuid)
        self.assertTrue(grp.uuid)

    def test_create_library(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}').create()
        grp_name = f'my_client_test_grp {key}'
        org.sample_group(grp_name, is_library=True).create()
        grp = SampleGroup(self.knex, org, grp_name).get()
        self.assertTrue(org.uuid)
        self.assertTrue(grp.uuid)
        self.assertTrue(grp.is_library)

    def test_create_group_result(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}')
        # N.B. It should NOT be necessary to call <parent>.create()
        ar = grp.analysis_result(f'my_client_test_module_name')  # no {key} necessary
        ar.create()
        self.assertTrue(org.uuid)
        self.assertTrue(grp.uuid)
        self.assertTrue(ar.uuid)

    def test_create_group_result_field(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}')
        ar = grp.analysis_result(f'my_client_test_module_name')  # no {key} necessary
        # N.B. It should NOT be necessary to call <parent>.create()
        field = ar.field('my_client_test_field_name', {'foo': 'bar'})
        field.create()
        self.assertTrue(org.uuid)
        self.assertTrue(grp.uuid)
        self.assertTrue(ar.uuid)
        self.assertTrue(field.uuid)

    def test_create_sample(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        # N.B. It should NOT be necessary to call <parent>.create()
        samp = grp.sample(f'my_client_test_sample {key}')
        samp.create()
        self.assertTrue(org.uuid)
        self.assertTrue(grp.uuid)
        self.assertTrue(samp.uuid)

    def test_add_sample(self):
        """Test that we can add a sample to a (non-library) sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        lib = org.sample_group(f'my_client_test_lib {key}', is_library=True)
        samp = lib.sample(f'my_client_test_sample {key}').create()

        grp = org.sample_group(f'my_client_test_grp {key}', is_library=False).create()
        grp.add_sample(samp).save()
        self.assertIn(samp.uuid, {samp.uuid for samp in grp.get_samples()})

    def test_get_samples_in_group(self):
        """Test that we can get the samples in a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        samp_names = [f'my_client_test_sample_{i} {key}' for i in range(10)]
        for samp_name in samp_names:
            grp.sample(samp_name).create()
        retrieved = org.sample_group(f'my_client_test_grp {key}', is_library=True).get()
        retrieved_names = set()
        for samp in retrieved.get_samples():
            retrieved_names.add(samp.name)
            self.assertTrue(samp.uuid)
        for samp_name in samp_names:
            self.assertIn(samp_name, retrieved_names)

    def test_get_results_in_group(self):
        """Test that we can get the results in a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        result_names = [('my_client_test_module', f'replicate_{i}') for i in range(10)]
        for module_name, replicate in result_names:
            grp.analysis_result(module_name, replicate=replicate).create()
        retrieved = org.sample_group(f'my_client_test_grp {key}', is_library=True).get()
        retrieved_names = set()
        for result in retrieved.get_analysis_results():
            retrieved_names.add((result.module_name, result.replicate))
            self.assertTrue(result.uuid)
        for result_name_rep in result_names:
            self.assertIn(result_name_rep, retrieved_names)

    def test_get_results_in_sample(self):
        """Test that we can get the results in a sample."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        samp = grp.sample(f'my_client_test_sample {key}').create()
        result_names = [('my_client_test_module', f'replicate_{i}') for i in range(10)]
        for module_name, replicate in result_names:
            samp.analysis_result(module_name, replicate=replicate).create()
        retrieved = grp.sample(f'my_client_test_sample {key}').get()
        retrieved_names = set()
        for result in retrieved.get_analysis_results():
            retrieved_names.add((result.module_name, result.replicate))
            self.assertTrue(result.uuid)
        for result_name_rep in result_names:
            self.assertIn(result_name_rep, retrieved_names)

    def test_get_result_fields(self):
        """Test that we can get the fields of an analysis result."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        samp = grp.sample(f'my_client_test_sample {key}')
        ar = samp.analysis_result('my_client_test_module').create()
        self.assertTrue(grp.uuid)

        field_names = [f'field_{i}' for i in range(10)]
        for field_name in field_names:
            ar.field(field_name).create()

        retrieved = samp.analysis_result('my_client_test_module').get()
        retrieved_names = set()
        for result in retrieved.get_fields():
            retrieved_names.add(result.name)
            self.assertTrue(result.uuid)
        for result_name_rep in field_names:
            self.assertIn(result_name_rep, retrieved_names)

    @skip('nonfunctional currently')
    def test_delete_sample(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        # N.B. It should NOT be necessary to call <parent>.create()
        samp = grp.sample(f'my_client_test_sample {key}')
        samp.create()
        self.assertTrue(samp.uuid)
        samp.delete()
        self.assertRaises(lambda: setattr(samp, 'name', 'foo'), RemoteObjectError)
        retrieved = grp.sample(f'my_client_test_sample {key}')
        self.assertRaises(retrieved.get, HTTPError)
        self.assertRaises()

    def test_modify_sample_save(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        # N.B. It should NOT be necessary to call <parent>.create()
        samp = grp.sample(f'my_client_test_sample {key}')
        samp.create()
        self.assertTrue(grp.is_public)
        self.assertTrue(samp.uuid)
        self.assertTrue(samp._already_fetched)
        self.assertFalse(samp._modified)
        samp.metadata = {f'metadata_{key}': 'some_new_metadata'}
        self.assertTrue(samp._modified)
        samp.save()
        self.assertTrue(samp._already_fetched)
        self.assertFalse(samp._modified)
        retrieved = grp.sample(f'my_client_test_sample {key}').get()
        self.assertIn(f'metadata_{key}', retrieved.metadata)

    def test_modify_sample_idem(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        # N.B. It should NOT be necessary to call <parent>.create()
        samp = grp.sample(f'my_client_test_sample {key}')
        samp.create()
        self.assertTrue(samp.uuid)
        self.assertTrue(samp._already_fetched)
        self.assertFalse(samp._modified)
        samp.metadata = {f'metadata_{key}': 'some_new_metadata'}
        self.assertTrue(samp._modified)
        samp.idem()
        self.assertTrue(samp._already_fetched)
        self.assertFalse(samp._modified)
        retrieved = grp.sample(f'my_client_test_sample {key}').get()
        self.assertIn(f'metadata_{key}', retrieved.metadata)

    def test_create_sample_result(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        samp = grp.sample(f'my_client_test_sample {key}')
        # N.B. It should NOT be necessary to call <parent>.create()
        ar = samp.analysis_result(f'my_client_test_module_name')  # no {key} necessary
        ar.create()
        self.assertTrue(org.uuid)
        self.assertTrue(grp.uuid)
        self.assertTrue(samp.uuid)
        self.assertTrue(ar.uuid)

    def test_create_sample_result_field(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        samp = grp.sample(f'my_client_test_sample {key}')
        ar = samp.analysis_result(f'my_client_test_module_name')  # no {key} necessary
        # N.B. It should NOT be necessary to call <parent>.create()
        field = ar.field('my_client_test_field_name', {'foo': 'bar'})
        field.create()
        self.assertTrue(org.uuid)
        self.assertTrue(grp.uuid)
        self.assertTrue(samp.uuid)
        self.assertTrue(ar.uuid)
        self.assertTrue(field.uuid)

    def test_modify_sample_result_field_idem(self):
        """Test that we can create a sample group."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        samp = grp.sample(f'my_client_test_sample {key}')
        ar = samp.analysis_result(f'my_client_test_module_name')  # no {key} necessary      
        # N.B. It should NOT be necessary to call <parent>.create()
        field = ar.field(f'my_client_test_field_name {key}', {'foo': 'bar'})
        field.create()
        self.assertTrue(field.uuid)
        field.stored_data = {'foo': 'bizz'}  # TODO: handle deep modifications
        field.idem()
        retrieved = ar.field(f'my_client_test_field_name {key}').get()
        self.assertEqual(retrieved.stored_data['foo'], 'bizz')

    def test_get_sample_group_manifest(self):
        """Test that we can get a group manifest."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        samp = grp.sample(f'my_client_test_sample {key}')
        ar = samp.analysis_result(f'my_client_test_module_name')  # no {key} necessary      
        field = ar.field('my_client_test_field_name', {'foo': 'bar'})
        field.create()
        manifest = grp.get_manifest()
        self.assertTrue(manifest)

    def test_get_sample_manifest(self):
        """Test that we can get a group manifest."""
        key = random_str()
        org = Organization(self.knex, f'my_client_test_org {key}')
        grp = org.sample_group(f'my_client_test_grp {key}', is_library=True)
        samp = grp.sample(f'my_client_test_sample {key}')
        ar = samp.analysis_result(f'my_client_test_module_name')  # no {key} necessary
        field = ar.field('my_client_test_field_name', {'foo': 'bar'})
        field.create()
        manifest = samp.get_manifest()
        self.assertTrue(manifest)
