from os import environ
from os.path import join
from typing import Literal

FIVE_MB = 5 * (1024 ** 2)
FASTQ_MODULE_NAMES = [
    'short_read::paired_end',
    'short_read::single_end',
    'long_read::nanopore',
    'long_read::pacbio',
    'raw::raw_reads',
    'genome::fasta',
]

# Ordered preference for resolving "the" reads folder of a sample when more than
# one read type is present (highest priority first). Used by
# Sample.get_one_fastq_folder. These are server ``module_name`` values for
# read-bearing folders -- note this list deliberately excludes ``genome::fasta``
# (not reads) which appears in FASTQ_MODULE_NAMES for upload/download CLI choices.
FASTQ_READ_TYPE_PREFERENCE = [
    'short_read::paired_end',
    'short_read::single_end',
    'long_read::nanopore',
    'long_read::pacbio',
]

# Authoritative set of result-folder ``module_name`` values that hold reads.
# These mirror the ``folder_name`` column of the server's DATA_NAMES table
# (geoseeq_server pangea/core/views/data_views/fastq.py) and additionally include
# the legacy ``reads``/``raw_reads`` names that appear in server tests, so
# detection stays permissive across both conventions. Deliberately excludes
# ``genome::fasta`` (a fasta folder, not reads).
READS_MODULE_NAMES = {
    'raw::raw_reads',
    'raw::single_short_reads',
    'short_read::paired_end',
    'short_read::single_end',
    'long_read::nanopore',
    'long_read::pacbio',
    'reads',
    'raw_reads',
}

DEFAULT_ENDPOINT = "https://backend.geoseeq.com"

CONFIG_FOLDER = environ.get("XDG_CONFIG_HOME", join(environ["HOME"], ".config"))
CONFIG_DIR = environ.get("GEOSEEQ_CONFIG_DIR", join(CONFIG_FOLDER, "geoseeq"))
PROFILES_PATH = join(CONFIG_DIR, "profiles.json")

OBJECT_TYPE_STR = Literal[
    'org',
    'project',
    'sample',
    'sample_result_folder',
    'project_result_folder',
    'sample_result_file',
    'project_result_file',
]
