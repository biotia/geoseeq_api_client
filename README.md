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


## Using the Command Line 

Run the command line by typing `geoseeq` into a terminal prompt. See available options by adding `--help`

```
$ geoseeq --help
```

You can find more command line examples in `docs/`


## Using the Python API in a program

Please see `geoseeq_api/cli/download.py` for examples of how to download data using the Python API directly.

---

## License and Credits

GeoSeeq is built and maintained by [Biotia](https://www.biotia.io/)

The GeoSeeq API client is licensed under the MIT license.
