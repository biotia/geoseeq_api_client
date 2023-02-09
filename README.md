# Geoseeq API Client

This package is a python library to interact with a Geoseeq server. It includes a command line interface that may be used to perform common tasks GeoSeeq tasks from the terminal.

GeoSeeq is a platform for sharing biological, climatological, and public health datasets. Learn more [here](https://www.geoseeq.com/).

GeoSeeq is built and maintained by [Biotia](https://www.biotia.io/)


## Installing and Testing the GeoSeeq API

### Install from source

Download this directory and run `python setup.py install`

### Install from PyPi

`pip install geoseeq-api`

### Testing

To test you will need a local version of geoseeq-django running.

Tests can be run using unittest

```
python -m unittest
```

## Example Command Line Usage

The following examples use data from a [public GeoSeeq project which you can find here](https://app.geoseeq.com/sample-groups/ed59b913-91ec-489b-a1b9-4ea137a6e5cf). Some examples (such as those for uploading data) won't work unless you replace this project with your own project since they require administrative rights.


### Using an API token

For many tasks you will need an API token to interact with GeoSeeq. You can get this token by logging into the [GeoSeeq App](https://app.geoseeq.com/) going to your user profile and clicking the "Tokens" tab.

Once you have a token you will need to set it as an environment variable like so:

```
$ export GEOSEEQ_API_TOKEN=<your token from the geoseeq app>
```


### Downloading Data from GeoSeeq


#### Download metadata from a GeoSeeq project as a CSV 

```
$ geoseeq-api download metadata GeoSeeq "Example CLI Project"
```


#### Download Short Read Sequencing data from one sample in a project as a set of FASTQ files

```
$ geoseeq-api download sample-results --module-name "short_read::paired_end" GeoSeeq "Example CLI Project" "s1"
```


#### Download all data from all sample in a project

Download all results for all smaples in the group `UW Madison Biotech 2020`

```
$ geoseeq-api download sample-results GeoSeeq "Example CLI Project"
```

### Upload Data to GeoSeeq

#### Upload single end short read sequencing data to a new sample in a project

Assume you have data from a single ended sequencing run stored as two fastq files: `mysamplename_L001.fastq.gz` and `mysamplename_L002.fastq.gz`

You can upload these files to GeoSeeq using the command line:

```
# navigate to the directory where the fastq files are stored
$ ls *.fastq.gz  # check that files are present
mysamplename_L001.fastq.gz mysamplename_L002.fastq.gz
$ echo "mysamplename_L001.fastq.gz\nmysamplename_L002.fastq.gz" > fastq_files.txt
$ geoseeq-api upload single-ended-reads GeoSeeq "Example CLI Project" fastq_files.txt
```

GeoSeeq will automatically create a new sample named `mysamplename` if it does not already exist.

Note: You will need to have an API token set to use this command (see above)

#### Upload nanopore sequencing data to a new sample in a project

Assume you have data from a nanopore sequening run stored as a single fastq file: `mysamplename.fastq.gz`

You can upload this file to GeoSeeq using the command line:

```
# navigate to the directory where the fastq file is stored
$ ls *.fastq.gz  # check that files are present
mysamplename.fastq.gz
$ echo "mysamplename.fastq.gz" > fastq_files.txt
$ geoseeq-api upload single-ended-reads --module-name "long_read::nanopore" GeoSeeq "Example CLI Project" fastq_files.txt
```

GeoSeeq will automatically create a new sample named `mysamplename` if it does not already exist.

Note: You will need to have an API token set to use this command (see above)

#### Upload paired end short read sequencing data to multiple new samples in a project

Assume you have paired ended sequencing data from two samples stored as eight fastq files:
 - `sample1_L001_R1.fastq.gz`
 - `sample1_L001_R2.fastq.gz`
 - `sample1_L002_R1.fastq.gz`
 - `sample1_L002_R2.fastq.gz`
 - `sample2_L001_R1.fastq.gz`
 - `sample2_L001_R2.fastq.gz`
 - `sample2_L002_R1.fastq.gz`
 - `sample2_L002_R2.fastq.gz`

You can upload these files to GeoSeeq using the command line:

```
# navigate to the directory where the fastq files are stored

$ ls -1 sample[1,2]_*.fastq.gz > fastq_files.txt
$ geoseeq-api upload reads GeoSeeq "Example CLI Project" fastq_files.txt
```

GeoSeeq will automatically create two new samples named `sample1` and `sample2` if they do not already exist.

Note: You will need to have an API token set to use this command (see above)

### Use `--help` to see more options

```
$ geoseeq-api download metadata --help
$ geoseeq-api download sample-results --help
```

### Using the Python API in a program

Please see `geoseeq_api/cli.py` for examples of how to download data using the Python API directly.

## Geoseeq Data Model

Geoseeq is based on a hierarchical data model with three components:

```
Sample Group -> Sample -> Analysis Result
```

- `Analysis Results` store actual data, the results of analyses and raw data (e.g short reads)
- `Samples` represent biological samples and group results
- `Sample Groups` are projects that group multiple samples

In practice this model includes additional components: `Result Fields` and `Analysis Results` for groups

```
Sample Group -> Sample -> Analysis Result -> Result Field
             |-> Analysis Result -> Result Field
```

- `Result Fields` provide a useful way to organize results with multiple files (e.g. the forward and reverse reads in paired end sequencing)
- `Group Analysis Results` make it possible to store results computed across many samples (e.g. a UMAP reduction)

Finally, some additonal components allow for privacy, permissions, and organization

```
User <-> Organization -> Sample Group -> Sample -> ...
                      |-> Analysis Result -> ...
```

- `Users` are people signed up on Geoseeq
- `Organizations` represent groups (e.g. a Lab). Organizations own groups and set permissions

### Storing Data on Geoseeq

Data is stored in `AnalysisResultFields`, attached to either `Samples` or `SampleGroups`. Each `AnalysisResultField` contains a JSON blob. This blob may be the data itself or, often, is a pointer to data stored somewhere else on the internet like `S3` or `FTP`.

If the stored data is anything other than a plain JSON blob it should include a special field `__type__` specifying how it should be handled.



