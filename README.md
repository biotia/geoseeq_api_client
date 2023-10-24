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
and nanopore reads.

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

Note: You will need to have an API token set to use this command (see above)

## Using the Python API in a program

Please see `geoseeq_api/cli/download.py` for examples of how to download data using the Python API directly.

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
