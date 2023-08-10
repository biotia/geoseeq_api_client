
# Download Data From GeoSeeq

The following examples use data from a [public GeoSeeq project which you can find here](https://app.geoseeq.com/sample-groups/ed59b913-91ec-489b-a1b9-4ea137a6e5cf). Some examples won't work unless you replace this project with your own project since they require administrative rights.

## Using an API token

For many tasks you will need an API token to interact with GeoSeeq. You can get this token by logging into the [GeoSeeq App](https://app.geoseeq.com/) going to your user profile and clicking the "Tokens" tab.

Once you have a token you will need to set it as an environment variable like so:

```
$ export GEOSEEQ_API_TOKEN=<your token from the geoseeq app>
```

## Download metadata from a GeoSeeq project as a CSV 

```
$ geoseeq download metadata "GeoSeeq/Example CLI Project" > metadata.csv
```


## Download Short Read Sequencing data from one sample in a project as a set of FASTQ files

```
$ geoseeq download sample-results --module-name "short_read::paired_end" GeoSeeq "Example CLI Project" "s1"
```


## Download all data from all sample in a project

Download all results for all samples in the group `Example CLI Project`

```
$ geoseeq download sample-results GeoSeeq "Example CLI Project"
```
