from os import environ
from os.path import join
from typing import Literal

FIVE_MB = 5 * (1024 ** 2)
FASTQ_MODULE_NAMES = [
    'short_read::paired_end',
    'short_read::single_end',
    'long_read::nanopore',
    'raw::raw_reads',
    'genome::fasta',
]
DEFAULT_ENDPOINT = "https://backend.geoseeq.com"

CONFIG_FOLDER = environ.get("XDG_CONFIG_HOME", join(environ["HOME"], ".config"))
CONFIG_DIR = environ.get("GEOSEEQ_CONFIG_DIR", join(CONFIG_FOLDER, "geoseeq"))
PROFILES_PATH = join(CONFIG_DIR, "profiles.json")

OBJECT_TYPE_STR = Literal['org', 'project', 'sample', 'sample_result_folder', 'project_result_folder', 'sample_result_file', 'project_result_file']