import os
import tempfile
import unittest
import pytest
from pathlib import Path
from click.testing import CliRunner
from geoseeq.knex import Knex
from geoseeq.cli import main
from geoseeq.organization import Organization

class TestDownloadCLI(unittest.TestCase):
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

    def test_cli_single_end_download(self):
        """Test downloading single-end FASTQ files using the CLI."""
        # Create and upload test files
        files = [
            self.create_test_fastq_file(f"sample_{i}.fastq", f"ATCG{i}") 
            for i in range(3)
        ]
        
        # Upload files
        for i, file_path in enumerate(files):
            sample = self.project.sample(f"sample_{i}")
            read_folder = sample.result_folder("short_read::single_end")
            read_folder.upload_file(file_path, "R1")
        
        # Create download directory
        download_dir = self.tmp_path / "downloads"
        download_dir.mkdir()
        
        # Run CLI command
        runner = CliRunner()
        result = runner.invoke(
            main,
            ['download',
             'reads',
             '--yes',
             str(self.project.uuid),
             str(download_dir)]
        )
        
        self.assertEqual(result.exit_code, 0)
        
        # Verify downloads
        for i in range(3):
            downloaded_file = download_dir / f"sample_{i}" / "short_read::single_end" / "R1.fastq"
            self.assertTrue(downloaded_file.exists())
            
            # Compare contents
            with open(files[i]) as f1, open(downloaded_file) as f2:
                self.assertEqual(f1.read(), f2.read())

    def test_cli_paired_end_download(self):
        """Test downloading paired-end FASTQ files using the CLI."""
        # Create and upload test files
        for i in range(3):
            r1_file = self.create_test_fastq_file(f"sample_{i}_R1.fastq", f"ATCG{i}")
            r2_file = self.create_test_fastq_file(f"sample_{i}_R2.fastq", f"GCTA{i}")
            
            sample = self.project.sample(f"sample_{i}")
            read_folder = sample.result_folder("short_read::paired_end")
            read_folder.upload_file(r1_file, "R1")
            read_folder.upload_file(r2_file, "R2")
        
        # Create download directory
        download_dir = self.tmp_path / "downloads"
        download_dir.mkdir()
        
        # Run CLI command
        runner = CliRunner()
        result = runner.invoke(
            main,
            ['download',
             'reads',
             '--yes',
             str(self.project.uuid),
             str(download_dir)]
        )
        
        self.assertEqual(result.exit_code, 0)
        
        # Verify downloads
        for i in range(3):
            sample_dir = download_dir / f"sample_{i}" / "short_read::paired_end"
            r1_downloaded = sample_dir / "R1.fastq"
            r2_downloaded = sample_dir / "R2.fastq"
            
            self.assertTrue(r1_downloaded.exists())
            self.assertTrue(r2_downloaded.exists())
            
            # Compare contents
            with open(Path(self.tmp_path) / f"sample_{i}_R1.fastq") as f1, open(r1_downloaded) as f2:
                self.assertEqual(f1.read(), f2.read())
            with open(Path(self.tmp_path) / f"sample_{i}_R2.fastq") as f1, open(r2_downloaded) as f2:
                self.assertEqual(f1.read(), f2.read())

if __name__ == '__main__':
    unittest.main() 