
# Upload Data to GeoSeeq

The following examples use data from a [public GeoSeeq project which you can find here](https://app.geoseeq.com/sample-groups/ed59b913-91ec-489b-a1b9-4ea137a6e5cf). Some examples won't work unless you replace this project with your own project since they require administrative rights.

## Using an API token

For many tasks you will need an API token to interact with GeoSeeq. You can get this token by logging into the [GeoSeeq App](https://app.geoseeq.com/) going to your user profile and clicking the "Tokens" tab.

Once you have a token you will need to set it as an environment variable like so:

```
$ export GEOSEEQ_API_TOKEN=<your token from the geoseeq app>
```


## Uploading sequencing data

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

$ geoseeq upload reads --cores 32 "Example GeoSeeq Org/Example CLI Project" fastq_files.txt
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

Note: You will need to have an API token set to use this command (see above)

### Linking reads from S3, Wasabi, FTP, Azure, and other cloud storage services

GeoSeeq allows you to link files stored on other cloud storage services without moving the files.

Assume you have data from a single ended sequencing run stored as fastq files on an s3 bucket: 
 - `https://s3.wasabisys.com/mybucketname/Sample1_L1_R1.fastq.gz`
 - `https://s3.wasabisys.com/mybucketname/Sample1_L1_R2.fastq.gz`
 - `https://s3.wasabisys.com/mybucketname/Sample1_L2_R1.fastq.gz`
 - `https://s3.wasabisys.com/mybucketname/Sample1_L2_R2.fastq.gz`

You can upload these files to GeoSeeq using the command line:

```
# Create a file that contains S3 URIs you want to link called fastq_urls.txt
$ cat fastq_urls.txt
https://s3.wasabisys.com/mybucketname/Sample1_L1_R1.fastq.gz
https://s3.wasabisys.com/mybucketname/Sample1_L1_R2.fastq.gz
https://s3.wasabisys.com/mybucketname/Sample1_L2_R1.fastq.gz
https://s3.wasabisys.com/mybucketname/Sample1_L2_R2.fastq.gz

$ geoseeq upload reads --link-type s3 "Example GeoSeeq Org" "Example CLI Project" fastq_urls.txt
Using regex: "(?P<sample_name>[^_]*)_L(?P<lane_num>[0-9]*)_R(?P<pair_num>1|2)\.fastq\.gz"
All files successfully grouped.
sample_name: Sample1
  module_name: short_read::paired_end
    short_read::paired_end::read_1::lane_1: https://s3.wasabisys.com/mybucketname/Sample1_L1_R1.fastq.gz
    short_read::paired_end::read_2::lane_1: https://s3.wasabisys.com/mybucketname/Sample1_L1_R2.fastq.gz
    short_read::paired_end::read_1::lane_2: https://s3.wasabisys.com/mybucketname/Sample1_L2_R1.fastq.gz
    short_read::paired_end::read_2::lane_2: https://s3.wasabisys.com/mybucketname/Sample1_L2_R2.fastq.gz
Do you want to upload these files? [y/N]: y
Uploading Sample: Sample1
```

This will create a new sample that links to your data without re-uploading any data. This is the fastest way
to link files which are already stored in the cloud.

You can link files from other types of cloud storage services such as:
 - FTP Servers
 - Azure Storage Accounts
 - The NCBI SRA
 - Any S3 compatible storage service (such as wasabi, amazon S3, backblaze)


## Uploading other files

You can upload any file to GeoSeeq regardless of type.

Imageine you are uploading an image file stored as a PNG. You would run the following command

```
$ geoseeq upload files "Example GeoSeeq Org/Example CLI Project/My Sample/My Images" image.png
```

Note: You will need to have an API token set to use this command (see above)


#### Uploading files to a project

The command above will upload a file to a sample. You can also upload files to a project that aren't grouped
with a specific sample. To do so:

```
$ geoseeq upload files "Example GeoSeeq Org/Example CLI Project/My Images" image.png
```
