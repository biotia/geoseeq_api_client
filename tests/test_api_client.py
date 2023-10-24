"""Test suite for experimental functions."""
import random
import sys
from os import environ
from unittest import TestCase, skip

from geoseeq import (
    Knex,
    Organization,
    GeoseeqOtherError,
    GeoseeqNotFoundError,
)
from requests.exceptions import ConnectionError

ENDPOINT = environ.get("GEOSEEQ_API_TESTING_ENDPOINT", "http://127.0.0.1:8000")
TOKEN = environ.get("GEOSEEQ_API_TOKEN", "<no_token>")


def random_str(len=12):
    """Return a random alphanumeric string of length `len`."""
    out = random.choices("abcdefghijklmnopqrtuvwxyzABCDEFGHIJKLMNOPQRTUVWXYZ0123456789", k=len)
    return "".join(out)


class TestGeoseeqApiClient(TestCase):
    """Test suite for packet building."""

    def setUp(self):
        self.knex = Knex(ENDPOINT)
        # Creates a test user and an API token for the user in database. Returns the token.
        if TOKEN == "<no_token>":
            try:
                api_token = self.knex.post("/users/test-user",
                                           json={"email": f"clitestuser_{random_str()}@gmail.com"})
            except GeoseeqOtherError:
                print(f"Could not create test user on \"{ENDPOINT}\". If you are running this test suite "\
                      "against a live server, please set the GEOSEEQ_API_TOKEN environment variable to a "\
                      "valid API token.",
                      file=sys.stderr)
                raise
            except ConnectionError:
                print(f"Could not connect to GeoSeeq Server at \"{ENDPOINT}\".",
                      file=sys.stderr)
                raise
            self.knex.add_api_token(api_token)
        else:
            self.knex.add_api_token(TOKEN)
            try:
                me = self.knex.get("/users/me")  # Test that the token is valid
                self.username = me["name"]
                self.org_name = f"API_TEST_ORG 1 {self.username}"
            except GeoseeqNotFoundError:
                print(f"Could not connect to GeoSeeq Server at \"{ENDPOINT}\" with the provided token.  "\
                      "Is it possible that you set thd testing endpoint to a front end url instead of "\
                      "the corresponding backend url?",
                      file=sys.stderr)
                raise

    def test_create_org(self):
        """Test that we can create an org."""
        org = Organization(self.knex, self.org_name)
        org.idem()
        self.assertTrue(org.uuid)


    def test_create_project(self):
        """Test that we can create a project."""
        key = random_str()
        org = Organization(self.knex, self.org_name)

        proj = org.project(f"my_client_test_project {key}")
        proj.create()
        self.assertTrue(org.uuid)
        self.assertTrue(proj.uuid)

    def test_create_project_result_folder(self):
        """Test that we can create a result folder in a project."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        # N.B. It should NOT be necessary to call <parent>.create()
        result_folder = proj.result_folder(f"my_client_test_module_name")  # no {key} necessary
        result_folder.create()
        self.assertTrue(org.uuid)
        self.assertTrue(proj.uuid)
        self.assertTrue(result_folder.uuid)

    def test_create_project_result_file(self):
        """Test that we can create a result file in a project."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        result_folder = proj.result_folder(f"my_client_test_module_name")  # no {key} necessary
        # N.B. It should NOT be necessary to call <parent>.create()
        result_file = result_folder.result_file("my_client_test_field_name", {"foo": "bar"})
        result_file.create()
        self.assertTrue(org.uuid)
        self.assertTrue(proj.uuid)
        self.assertTrue(result_folder.uuid)
        self.assertTrue(result_file.uuid)

    def test_create_sample(self):
        """Test that we can create a sample."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        # N.B. It should NOT be necessary to call <parent>.create()
        samp = proj.sample(f"my_client_test_sample {key}")
        samp.create()
        self.assertTrue(org.uuid)
        self.assertTrue(proj.uuid)
        self.assertTrue(samp.uuid)

    def test_add_sample(self):
        """Test that we can create a sample and add it to a different project."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj1 = org.project(f"my_client_test_proj1 {key}")
        samp = proj1.sample(f"my_client_test_sample {key}").create()

        proj2 = org.project(f"my_client_test_proj2 {key}").create()
        proj2.add_sample(samp).save()
        self.assertIn(samp.uuid, {samp.uuid for samp in proj2.get_samples()})

    def test_get_samples_project(self):
        """Test that we can get the samples in a project."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        samp_names = [f"my_client_test_sample_{i} {key}" for i in range(10)]
        for samp_name in samp_names:
            proj.sample(samp_name).create()
        retrieved_proj = org.project(f"my_client_test_proj {key}").get()
        retrieved_names = set()
        for samp in retrieved_proj.get_samples():
            retrieved_names.add(samp.name)
            self.assertTrue(samp.uuid)
        for samp_name in samp_names:
            self.assertIn(samp_name, retrieved_names)

    def test_get_result_folders_in_project(self):
        """Test that we can get the result folders in a project."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        result_names = [("my_client_test_module", f"replicate_{i}") for i in range(10)]
        for module_name, replicate in result_names:
            proj.result_folder(module_name, replicate=replicate).create()
        retrieved_proj = org.project(f"my_client_test_proj {key}").get()
        retrieved_names = set()
        for result in retrieved_proj.get_result_folders():
            retrieved_names.add((result.module_name, result.replicate))
            self.assertTrue(result.uuid)
        for result_name_rep in result_names:
            self.assertIn(result_name_rep, retrieved_names)

    def test_get_result_folders_in_sample(self):
        """Test that we can get the result folders in a sample."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        samp = proj.sample(f"my_client_test_sample {key}").create()
        result_names = [("my_client_test_module", f"replicate_{i}") for i in range(10)]
        for module_name, replicate in result_names:
            samp.result_folder(module_name, replicate=replicate).create()
        retrieved = proj.sample(f"my_client_test_sample {key}").get()
        retrieved_names = set()
        for result in retrieved.get_result_folders():
            retrieved_names.add((result.module_name, result.replicate))
            self.assertTrue(result.uuid)
        for result_name_rep in result_names:
            self.assertIn(result_name_rep, retrieved_names)

    def test_get_result_files(self):
        """Test that we can get the files in a result folder."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        samp = proj.sample(f"my_client_test_sample {key}")
        result_folder = samp.result_folder("my_client_test_module").create()
        self.assertTrue(proj.uuid)

        field_names = [f"field_{i}" for i in range(10)]
        for field_name in field_names:
            result_folder.field(field_name).create()

        retrieved = samp.result_folder("my_client_test_module").get()
        retrieved_names = set()
        for result in retrieved.get_fields():
            retrieved_names.add(result.name)
            self.assertTrue(result.uuid)
        for result_name_rep in field_names:
            self.assertIn(result_name_rep, retrieved_names)

    def test_modify_sample(self):
        """Test that we can modify a sample after creation"""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        # N.B. It should NOT be necessary to call <parent>.create()
        samp = proj.sample(f"my_client_test_sample {key}")
        samp.create()
        self.assertTrue(samp.uuid)
        self.assertTrue(samp._already_fetched)
        self.assertFalse(samp._modified)
        samp.metadata = {f"metadata_{key}": "some_new_metadata"}
        self.assertTrue(samp._modified)
        samp.save()
        self.assertTrue(samp._already_fetched)
        self.assertFalse(samp._modified)
        retrieved = proj.sample(f"my_client_test_sample {key}").get()
        self.assertIn(f"metadata_{key}", retrieved.metadata)

    def test_create_sample_result_folder(self):
        """Test that we can create a result folder in a sample."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        samp = proj.sample(f"my_client_test_sample {key}")
        # N.B. It should NOT be necessary to call <parent>.create()
        result_folder = samp.result_folder(f"my_client_test_module_name")  # no {key} necessary
        result_folder.create()
        self.assertTrue(org.uuid)
        self.assertTrue(proj.uuid)
        self.assertTrue(samp.uuid)
        self.assertTrue(result_folder.uuid)

    def test_create_sample_result_file(self):
        """Test that we can create a result file in a sample."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        samp = proj.sample(f"my_client_test_sample {key}")
        result_folder = samp.result_folder(f"my_client_test_module_name")  # no {key} necessary
        # N.B. It should NOT be necessary to call <parent>.create()
        result_file = result_folder.result_file("my_client_test_field_name", {"foo": "bar"})
        result_file.create()
        self.assertTrue(org.uuid)
        self.assertTrue(proj.uuid)
        self.assertTrue(samp.uuid)
        self.assertTrue(result_folder.uuid)
        self.assertTrue(result_file.uuid)

    @skip("failing on server (2023-10-24)")
    def test_modify_sample_result_file(self):
        """Test that we can modify a result file in a sample."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        samp = proj.sample(f"my_client_test_sample {key}")
        result_folder = samp.result_folder(f"my_client_test_module_name")  # no {key} necessary
        # N.B. It should NOT be necessary to call <parent>.create()
        result_file = result_folder.result_file(f"my_client_test_file_name {key}", {"foo": "bar"})
        result_file.create()
        self.assertTrue(result_file.uuid)
        result_file.stored_data = {"foo": "bizz"}  # TODO: handle deep modifications
        result_file.save()
        retrieved = result_folder.result_file(f"my_client_test_file_name {key}").get()
        self.assertEqual(retrieved.stored_data["foo"], "bizz")

    def test_get_project_manifest(self):
        """Test that we can get a project manifest."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        samp = proj.sample(f"my_client_test_sample {key}")
        result_folder = samp.result_folder(f"my_client_test_module_name")  # no {key} necessary
        result_file = result_folder.result_file("my_client_test_field_name", {"foo": "bar"})
        result_file.create()
        manifest = proj.get_manifest()
        self.assertTrue(manifest)

    def test_get_sample_manifest(self):
        """Test that we can get a sample manifest."""
        key = random_str()
        org = Organization(self.knex, self.org_name)
        proj = org.project(f"my_client_test_proj {key}")
        samp = proj.sample(f"my_client_test_sample {key}")
        result_folder = samp.result_folder(f"my_client_test_module_name")  # no {key} necessary
        result_file = result_folder.result_file("my_client_test_field_name", {"foo": "bar"})
        result_file.create()
        manifest = samp.get_manifest()
        self.assertTrue(manifest)
