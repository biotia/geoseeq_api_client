# Geoseeq API Client

This package is a python library to interact with a Geoseeq server. It includes a command line interface that may be used to perform common tasks GeoSeeq tasks from the terminal.

GeoSeeq is a platform for sharing biological, climatological, and public health datasets. Learn more [here](https://www.geoseeq.com/).

This API client is a work in progress and we welcome suggestions, feedback, comments, and criticisms.

---

## Installation

### Install from PyPi

`pip install geoseeq`

### Install from source

Download this directory and run `python setup.py install`

---

## Using the Command Line

Run the command line by typing `geoseeq` into a terminal prompt. See available options by adding `--help`

```
$ geoseeq --help
```

The CLI is organized into command groups. Run `geoseeq <group> --help` for the
subcommands and options of each:

| Group | What it does |
|-------|--------------|
| `download` | Download files, fastqs, folders, or metadata from a project |
| `upload`   | Upload local files and reads into GeoSeeq-managed storage |
| `link`     | Register files already in S3-compatible storage as links, without copying bytes |
| `manage`   | Create, edit, and delete projects, samples, and folders |
| `search`   | Search GeoSeeq for organizations, projects, and samples |
| `view`     | Print details about GeoSeeq objects |
| `app`      | List and trigger GeoSeeq pipeline apps and runs |
| `s3`       | Register single files staged in a project's S3 credentials flow |
| `find-grn` | Resolve a GeoSeeq Resource Name (GRN) to an object |
| `eula`     | Accept end-user license agreements required by some pipelines |

### Configuration and Using an API token

For many tasks you will need an API token to interact with GeoSeeq. You can get this token by logging into the [GeoSeeq Portal](https://portal.geoseeq.com/) going to your user profile and clicking the "Tokens" tab.

Once you have a token you will need to configure GeoSeeq to use it. Run `geoseeq config` and leave the profile name and url blank. You will be prompted to enter your API token.

```
$ geoseeq config
Set custom profile name? (Leave blank for default) []:
Enter the URL to use for GeoSeeq (Most users can use the default) [https://backend.geoseeq.com]:
Enter your GeoSeeq API token:
Profile configured.
```

This command will store your token in a file called `~/.config/geoseeq/profiles.json` and will be used by all future commands.

### Example Commands

You can find more command line examples in `docs/`

#### Download Short Read Sequencing data from one sample in a project as a set of FASTQ files

This command will download data [from this project.](https://portal.geoseeq.com/sample-groups/ed59b913-91ec-489b-a1b9-4ea137a6e5cf/samples)

```
$ geoseeq download files --extension fastq.gz "GeoSeeq/Example CLI Project"
```

#### Uploading sequencing data

GeoSeeq can automatically group fastq files into samples according to their
sample name, read number, and lane number. It supports paired end, single end,
nanopore, and pacbio reads.

Assume you have data from a single ended sequencing run stored as fastq files:
 - Sample1_L1_R1.fastq.gz
 - Sample1_L1_R2.fastq.gz
 - Sample1_L2_R1.fastq.gz
 - Sample1_L2_R2.fastq.gz

You can upload these files to GeoSeeq using the command line. This example will upload 32 files in  parallel:

```
# navigate to the directory where the fastq files are stored
$ ls -1 *.fastq.gz > fastq_files.txt  # check that files are present

$ geoseeq upload reads --cores 32 "GeoSeeq/Example CLI Project" fastq_files.txt
Using regex: "(?P<sample_name>[^_]*)_L(?P<lane_num>[0-9]*)_R(?P<pair_num>1|2)\.fastq\.gz"
All files successfully grouped.
sample_name: Sample1
  module_name: short_read::paired_end
    short_read::paired_end::read_1::lane_1: Sample1_L1_R1.fastq.gz
    short_read::paired_end::read_2::lane_1: Sample1_L1_R2.fastq.gz
    short_read::paired_end::read_1::lane_2: Sample1_L2_R1.fastq.gz
    short_read::paired_end::read_2::lane_2: Sample1_L2_R2.fastq.gz
Do you want to upload these files? [y/N]: y
Uploading Sample: Sample1
```

GeoSeeq will automatically create a new sample named `Sample1` if it does not already exist.

This command would upload data [to this project.](https://portal.geoseeq.com/sample-groups/ed59b913-91ec-489b-a1b9-4ea137a6e5cf/samples). Since only organization members can upload data, you will need to replace `GeoSeeq` with your organization name.

To rename samples on the fly, provide a CSV file with current and new names using the `--name-map` option:

```
$ geoseeq upload reads --name-map sample_map.csv current_name new_name "GeoSeeq/Example CLI Project" fastq_files.txt
```

Note: You will need to have an API token set to use this command (see above)

If your reads already live in an S3-compatible bucket (AWS S3, Backblaze
B2, Wasabi, MinIO, ...) and you want GeoSeeq to point at them in place
rather than upload the bytes, see [Linking remote files](#linking-remote-files) below.

#### Linking remote files

If your fastq files already exist in an S3-compatible bucket — AWS S3,
Backblaze B2, Wasabi, MinIO, etc. — you can register them with GeoSeeq
as links instead of uploading the bytes. Pick the right command for the
shape of the work:

| Command | Use when |
|---------|----------|
| `geoseeq link reads`   | You have a whole S3 prefix of fastq files to register in bulk. GeoSeeq lists the prefix, groups files into samples server-side, and links each file in place. |
| `geoseeq upload reads` | You want GeoSeeq to manage the bytes — local files copied into managed storage. |
| `geoseeq s3 register`  | You have a single file already staged in GeoSeeq's per-project staging credentials flow. |

`link reads` requires the optional `[s3]` extras (boto3):

```
pip install 'geoseeq[s3]'
```

Example — register a Backblaze B2 prefix of paired-end reads:

```
$ geoseeq link reads "MyOrg/MyProject" \
      s3://my-bucket/path/to/reads/ \
      --endpoint-url https://s3.us-east-005.backblazeb2.com \
      --commit
```

Without `--commit` the command runs in dry-run mode and prints the
proposed `(sample, field, s3-uri)` tuples without writing. AWS
credentials follow the standard boto3 chain (env vars, shared config,
instance profile) — there are no new secrets for the CLI to manage.

Note: `geoseeq upload reads --link-type s3 <local-file-list>` is
deprecated as of this release; prefer `geoseeq link reads` (bulk) or
`geoseeq s3 register` (single staged file).

## Using the Python API in a program


Please see `geoseeq/cli/download.py` for examples of how to download data using the Python API directly.

## Development in GitHub Codespaces

This repository includes a `.devcontainer` configuration so it can be opened directly in [GitHub Codespaces](https://docs.github.com/codespaces). When the codespace is created the development dependencies are installed and `pre-commit` hooks are set up automatically.

---

## Notes

### Terminology

Some terms have changed in GeoSeeq since this package was written. The command line tool and code may contain references to old names.

| Old Name  | New Name  |
|---|---|
| Sample Group  | Project  |
| Library  | _defunct_  |
| Analysis Result  | ResultFolder  |
| Analysis Result Field | ResultFile |
---

## License and Credits

GeoSeeq is built and maintained by [Biotia](https://www.biotia.io/)

The GeoSeeq API client is licensed under the MIT license.
