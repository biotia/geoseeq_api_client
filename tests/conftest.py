import os
import pytest
from click.testing import CliRunner
from geoseeq.knex import Knex
from geoseeq.cli import main

# Test organization and user configuration
TEST_ORG_NAME = "API Client Test Organization"
TEST_USER_EMAIL = "api-client-test@biotia.io"
TEST_USER_PASSWORD = os.getenv("GEOSEEQ_TEST_PASSWORD", "test-password-please-change")

@pytest.fixture(scope="session")
def test_knex():
    """Create a Knex instance for testing."""
    knex = Knex()
    # Login with test user
    knex.login(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    return knex

@pytest.fixture(scope="session")
def test_org(test_knex):
    """Get or create the test organization."""
    org = test_knex.get_org(TEST_ORG_NAME)
    if not org:
        org = test_knex.create_org(TEST_ORG_NAME)
    return org

@pytest.fixture(scope="function")
def test_project(test_org):
    """Create a temporary project for testing."""
    project = test_org.create_project("test-project")
    yield project
    # Cleanup after test
    project.delete()

@pytest.fixture(scope="function")
def test_sample(test_project):
    """Create a temporary sample for testing."""
    sample = test_project.create_sample("test-sample")
    yield sample
    # Cleanup after test
    sample.delete()

@pytest.fixture(scope="function")
def test_read_folder(test_sample):
    """Create a temporary read folder for testing."""
    read_folder = test_sample.create_read_folder("test-reads")
    yield read_folder
    # Cleanup after test
    read_folder.delete()

@pytest.fixture
def runner():
    """Create a CLI runner for testing."""
    return CliRunner() 