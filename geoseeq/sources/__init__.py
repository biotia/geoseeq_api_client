"""Read-source abstractions for the geoseeq client.

A "source" is a place where raw read files (e.g. FASTQs) live before they
are linked or uploaded into a GeoSeeq sample. Sources are deliberately
thin: they only expose what the CLI needs to enumerate files and turn
them back into addressable URIs.

Currently only :class:`S3Source` is implemented. An :class:`AzureSource`
counterpart is planned; a shared base class will be introduced when the
second backend lands so the abstraction stays grounded in real usage.
"""

from .s3 import S3Source

__all__ = ["S3Source"]
