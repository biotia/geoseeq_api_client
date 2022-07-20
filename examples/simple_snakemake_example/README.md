# Simple [Snakemake](https://snakemake.github.io/) Example

This is a very simple example of analyzing data from [Pangea](https://pangeabio.io) using [Snakemake](https://snakemake.github.io/). This script does the following:

1. Gets a list of samples in a Pangea group
2. Checks if the samples have already been processed
3. Downloads read from samples that have not been processed
4. Processes the reads
5. Uploads the new result

You can run the example as is on the [Pangea Demo Group](https://pangeabio.io/sample-groups/6c0f8eea-b183-47af-84df-99ab33d4292b) using the [Demo User](https://pangeabio.io/users/7fb84c58-933a-49e5-92ca-b25fa3360ff6). To run the example you will need to have the [pangea python api](https://github.com/LongTailBio/pangea-django/tree/master/api-client) installed.

From this folder run

```
snakemake --cores 1
```

When you run this command you should see sankemake output running 4 jobs.

To run the example on your own data you will need a [Pangea](https://pangeabio.io) login and a sample group to process. Once you have these edit the `config.yaml` file and run `snakemake --cores 1`.
