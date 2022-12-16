# GeoSeeq Version Control

GeoSeeq VC is a toolkit that makes it easy to use geoseeq with version control systems like `git`.

Most of the files stored on GeoSeeq are too large to version control directly. This package creates lightweight stub files that record a files location and checksum. This package also contains tools to download files, and check that local files match those stored on the server. The stub files are small enough to version control directly.

## CLI

You can clone a project or sample from GeoSeeq. You will need to get the `brn` number from the project or sample then run these commands from your CLI 

```
geoseeq-api vc clone brn:gsr1:project:bed763fb-f6b8-4739-8320-95c06d68f442  # creates a directory tree with stub files
geoseeq-api vc download  # downloads the files that the stubs link to
geoseeq-api vc status  # checks that the local files match the files on GeoSeeq
```

If you are using git you probably want to add files from geoseeq to your `.gitignore`

```
geoseeq-api vc list >> .gitignore
```

## Shared Cache

To avoid downloading or storing the same file multiple times users can specify a cache by setting the `GEOSEEQ_VC_CACHE_DIR` envvar to an absolute filepath. When set geoseeq will download files to a location in the cahce directory and create symlinks to those files.

## Stub Files

GeoSeeq interfaces with version control by creating small stub files that represent a larger file (AKA result field) stored on GeoSeeq. This package can be used to download those files and validate that the checksum of local files matches them. Since these files are small they can be easily stored with a version control system while the larger files can be ignored.

The stub files are JSON and have the following structure:

```
{
    "__schema_version__": "v0",
    "brn": "brn:<instance name>:result_field:<uuid>",  # optional if stub is new and parent_info is set
    "checksum": {
        "value": "<value>",
        "method": "md5",
    },
    "local_path": "<filename>",  # a filepath relative to the stub file
    "parent_info": {  # optional if brn is set
        "parent_obj_brn": "brn:<brn info>",
        "result_module_name": <string>,
        "result_module_replicate": <string>,
    }  # NB if brn and parent_info are both set but disagree `brn` is correct
}
```