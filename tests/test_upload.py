import os
import tempfile
import unittest
import pytest
from pathlib import Path
from geoseeq.knex import Knex
from geoseeq.organization import Organization
from geoseeq.upload_download_manager import GeoSeeqUploadManager

class TestUpload(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Test configuration from environment variables
        cls.test_org_name = os.getenv("GEOSEEQ_TEST_ORG", "API Client Test Organization")
        cls.test_api_token = os.getenv("GEOSEEQ_TEST_API_TOKEN")
        if not cls.test_api_token:
            pytest.skip(
                "GeoSeeq integration tests require GEOSEEQ_TEST_API_TOKEN", allow_module_level=True
            )

    def setUp(self):
        # Set up test environment
        self.knex = Knex()
        self.knex.add_api_token(self.test_api_token)
        self.org = Organization(self.knex, self.test_org_name)
        self.org.get()  # Fetch the existing organization
        self.project = self.org.project("test-project")
        self.sample = self.project.sample("test-sample")
        self.read_folder = self.sample.result_folder("short_read::single_end")
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def create_test_fastq_file(self, name, content="ATCG"):
        """Create a test FASTQ file with the given content."""
        filepath = self.tmp_path / name
        with open(filepath, 'w') as f:
            f.write(f"@test\n{content}\n+\n{'I' * len(content)}\n")
        return filepath

    def test_programmatic_single_file_upload(self):
        """Test uploading a single file using the API directly."""
        # Create test file
        test_file = self.create_test_fastq_file("test.fastq")
        
        # Upload using the API
        result_file = self.read_folder.upload_file(test_file, "R1")
        
        # Verify upload
        self.assertTrue(result_file.exists())

    def test_programmatic_upload_manager(self):
        """Test uploading multiple files using the upload manager."""
        # Create test files
        files = [
            self.create_test_fastq_file(f"test_{i}.fastq", f"ATCG{i}") 
            for i in range(3)
        ]
        
        # Set up upload manager
        upload_manager = GeoSeeqUploadManager(n_parallel_uploads=2)
        
        # Add files to upload manager
        for i, file_path in enumerate(files):
            result_file = self.read_folder.result_file(f"R{i+1}")
            upload_manager.add_result_file(result_file, file_path)
        
        # Upload files
        upload_manager.upload_files()
        
        # Verify uploads
        for i in range(3):
            result_file = self.read_folder.result_file(f"R{i+1}")
            self.assertTrue(result_file.exists())

if __name__ == '__main__':
    unittest.main() 