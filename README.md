# Geoseeq API Client

This is a python library to interact with the Geoseeq API.

## Downloading Data and Metadata

This package may be used to download data from Geoseeq.

An example download script can be seen [here](https://gist.github.com/dcdanko/c70304e5eb9c20fc81111929598edda0).

Geoseeq is designed to store massive amounts of data. Downloads of large files are likely to take a long time.

### Using the CLI

The simplest way to download data is using the CLI.

Download metadata from the sample group `UW Madison Biotech 2020`

```
$ geoseeq-api download metadata 'Mason Lab' 'UW Madison Biotech 2020'
```

Download the result `cap1::microbe_census` from sample `BC-0235081664`

```
$ geoseeq-api download sample-results --module-name 'cap1::microbe_census' 'Mason Lab' 'UW Madison Biotech 2020' BC-0235081664
```

Download all results for all smaples in the group `UW Madison Biotech 2020`

```
$ geoseeq-api download sample-results 'Mason Lab' 'UW Madison Biotech 2020'
```

Use `--help` to see more options

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
User <-> Organization -> (Group or Library) -> Sample -> ...
                      |-> Analysis Result -> ...
```

- `Users` are people signed up on Geoseeq
- `Organizations` represent groups (e.g. a Lab). Organizations own groups and set permissions
- `Libraries` are special `Sample Groups` that act as a home base for samples. Every sample has exactly one `Library`

### Storing Data on Geoseeq

Data is stored in `AnalysisResultFields`, attached to either `Samples` or `SampleGroups`. Each `AnalysisResultField` contains a JSON blob. This blob may be the data itself or, often, is a pointer to data stored somewhere else on the internet like `S3` or `FTP`.

If the stored data is anything other than a plain JSON blob it should include a special field `__type__` specifying how it should be handled.

## Installing and Testing

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

## Local development

### Linting, formatting

Recommended linter is pylint, general formatter black and isort to format imports on save. To setup in your development enviroment run:

```sh
  pip install pylint black isort
```

Add the following lines to the settings.json:

```sh
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": true,
  "python.linting.pylintArgs": [
    "--max-line-length=100",
  ],
  "python.sortImports.args": [
    "--profile", "black"
  ],
  "[python]": {
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    }
  },
  "python.formatting.provider": "black",
  "python.formatting.blackArgs": ["--line-length", "100"]
```

### Commit linting

We use conventional commits. [read](https://www.conventionalcommits.org)\
To setup pre-commit run:

```sh
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg
```
