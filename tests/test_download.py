import os
import tempfile
import unittest
import pytest
from pathlib import Path
from geoseeq.knex import Knex
from geoseeq.organization import Organization
from geoseeq.upload_download_manager import GeoSeeqDownloadManager
from time import sleep

class TestDownload(unittest.TestCase):
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
        self.project = self.org.project("test-download-project")
        self.sample = self.project.sample("test-single-sample")
        self.read_folder = self.sample.result_folder("short_read::single_end")
        self.read_folder.idem()  # Ensure the folder exists
        self.multi_sample = self.project.sample("test-multi-sample")
        self.multi_sample_folder = self.multi_sample.result_folder("short_read::single_end")
        self.multi_sample_folder.idem()  # Ensure the folder exists
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

    def test_programmatic_single_file_download(self):
        """Test downloading a single file using the API directly."""
        # Create and upload test file
        test_file = self.create_test_fastq_file("test.fastq", "ATCG")
        result_file = self.read_folder.result_file("R1")
        result_file.idem()  # Ensure the file exists
        result_file.upload_file(test_file)
        sleep(5)
        
        # Download the file
        download_path = self.tmp_path / "downloaded.fastq"
        result_file.download(str(download_path))
        
        # Verify download
        self.assertTrue(download_path.exists())
        
        # Compare contents
        with open(test_file) as f1, open(download_path) as f2:
            self.assertEqual(f1.read(), f2.read())

    def test_programmatic_download_manager(self):
        """Test downloading multiple files using the download manager."""
        # Create and upload test files
        files = [
            self.create_test_fastq_file(f"test_{i}.fastq", f"ATCG{i}") 
            for i in range(3)
        ]
        
        result_files = []
        for i, file_path in enumerate(files):
            result_file = self.multi_sample_folder.result_file(f"R{i+1}")
            result_file.idem()  # Ensure the file exists
            result_file.upload_file(file_path)
            result_files.append(result_file)

        sleep(5)
        
        # Set up download manager
        download_manager = GeoSeeqDownloadManager(n_parallel_downloads=2)
        
        # Add files to download manager
        download_paths = []
        for i, result_file in enumerate(result_files):
            download_path = self.tmp_path / f"downloaded_{i}.fastq"
            download_manager.add_download(result_file, str(download_path))
            download_paths.append(download_path)
        
        # Download files
        download_manager.download_files()
        
        # Verify downloads
        for i, (original_path, download_path) in enumerate(zip(files, download_paths)):
            self.assertTrue(download_path.exists())
            
            # Compare contents
            with open(original_path) as f1, open(download_path) as f2:
                self.assertEqual(f1.read(), f2.read())

if __name__ == '__main__':
    unittest.main() 