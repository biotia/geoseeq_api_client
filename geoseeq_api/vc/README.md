# GeoSeeq Version Control

This module allows GeoSeeq to be integrated with version control systems like git.

## CLI

Cloning a project

```
geoseeq-api vc clone brn:gsr1:project:bed763fb-f6b8-4739-8320-95c06d68f442
```

Th
## Stub Files

GeoSeeq interfaces with version control by creating small stub files that represent a larger file (AKA result field) stored on GeoSeeq. This package can be used to download those files and validate that the checksum of local files matches them. Since these files are small they can be easily stored with a version control system while the larger files can be ignored.

The stub files are JSON and have the following structure:

```
{
    "brn": "brn:<instance name>:result_field:<uuid>",
    "checksum": {
        "value": "<value>",
        "method": "md5",
    },
    "local_path": "<filename>",  # a filepath relative to the stub file
}
```