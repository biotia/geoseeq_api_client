import os
import tempfile
import unittest
import pytest
from pathlib import Path
from click.testing import CliRunner
from geoseeq.knex import Knex
from geoseeq.cli import main
from geoseeq.organization import Organization

class TestUploadCLI(unittest.TestCase):
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

    def test_cli_single_end_upload(self):
        """Test uploading single-end FASTQ files using the CLI."""
        # Create test files
        files = [
            self.create_test_fastq_file(f"sample_{i}.fastq", f"ATCG{i}") 
            for i in range(3)
        ]
        
        # Create a file list
        file_list = self.tmp_path / "file_list.txt"
        with open(file_list, 'w') as f:
            for file in files:
                f.write(f"{file}\n")
        
        # Run CLI command
        runner = CliRunner()
        result = runner.invoke(
            main, 
            ['upload', 'reads', 
             '--regex', r'(?P<sample_name>sample_\d+)\.fastq',
             '--module-name', 'short_read::single_end',
             '--yes',
             str(self.project.uuid),
             str(file_list)]
        )
        
        self.assertEqual(result.exit_code, 0)
        
        # Verify uploads
        for i in range(3):
            sample = self.project.sample(f"sample_{i}")
            read_folder = sample.result_folder("short_read::single_end")
            result_file = read_folder.result_file("R1")
            self.assertTrue(result_file.exists())

    def test_cli_paired_end_upload(self):
        """Test uploading paired-end FASTQ files using the CLI."""
        # Create test files
        for i in range(3):
            self.create_test_fastq_file(f"sample_{i}_R1.fastq", f"ATCG{i}")
            self.create_test_fastq_file(f"sample_{i}_R2.fastq", f"GCTA{i}")
        
        # Create a file list
        file_list = self.tmp_path / "file_list.txt"
        with open(file_list, 'w') as f:
            for i in range(3):
                f.write(f"{self.tmp_path}/sample_{i}_R1.fastq\n")
                f.write(f"{self.tmp_path}/sample_{i}_R2.fastq\n")
        
        # Run CLI command
        runner = CliRunner()
        result = runner.invoke(
            main, 
            ['upload', 'reads', 
             '--regex', r'(?P<sample_name>sample_\d+)_R(?P<pair_num>[12])\.fastq',
             '--module-name', 'short_read::paired_end',
             '--yes',
             str(self.project.uuid),
             str(file_list)]
        )
        
        self.assertEqual(result.exit_code, 0)
        
        # Verify uploads
        for i in range(3):
            sample = self.project.sample(f"sample_{i}")
            read_folder = sample.result_folder("short_read::paired_end")
            r1_file = read_folder.result_file("R1")
            r2_file = read_folder.result_file("R2")
            self.assertTrue(r1_file.exists())
            self.assertTrue(r2_file.exists())

if __name__ == '__main__':
    unittest.main() 