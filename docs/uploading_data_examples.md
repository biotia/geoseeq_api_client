
# Upload Data to GeoSeeq

The following examples use data from a [public GeoSeeq project which you can find here](https://app.geoseeq.com/sample-groups/ed59b913-91ec-489b-a1b9-4ea137a6e5cf). Some examples won't work unless you replace this project with your own project since they require administrative rights.

## Using an API token

For many tasks you will need an API token to interact with GeoSeeq. You can get this token by logging into the [GeoSeeq App](https://app.geoseeq.com/) going to your user profile and clicking the "Tokens" tab.

Once you have a token you will need to set it as an environment variable like so:

```
$ export GEOSEEQ_API_TOKEN=<your token from the geoseeq app>
```

## Uploading Sequencing Data


### Uploading Data Using the Upload Wizard

TODO

#### Linking files from S3, Wasabi, FTP, Azure, and other cloud storage services

TODO

### Uploading Sequencing Data Manually

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

## Uploading Other Data

### Upload a File to a Project

TODO

### Upload a File to a Sample

TODO