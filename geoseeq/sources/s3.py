"""S3 read source.

Thin wrapper around boto3 used by the ``geoseeq link reads`` CLI to
enumerate read files in an S3 bucket. boto3 is imported lazily inside
methods so ``from geoseeq.sources import S3Source`` keeps working in
environments that did not install the ``[s3]`` extra.
"""

from __future__ import annotations

import fnmatch
from typing import Iterator, Optional
from urllib.parse import urlparse

_BOTO3_INSTALL_HINT = "Install with: pip install 'geoseeq[s3]'"


def _import_boto3():
    """Import and return the :mod:`boto3` module, with a friendly hint on failure."""
    try:
        import boto3  # noqa: WPS433 — intentional lazy import
    except ImportError as exc:  # pragma: no cover - exercised via patched sys.modules
        raise ImportError(_BOTO3_INSTALL_HINT) from exc
    if boto3 is None:
        # ``patch.dict(sys.modules, {"boto3": None})`` sets the entry to None,
        # which ``import boto3`` happily returns without raising.
        raise ImportError(_BOTO3_INSTALL_HINT)
    return boto3


class S3Source:
    """An S3 bucket + prefix that read files can be listed from.

    Parameters
    ----------
    bucket:
        S3 bucket name.
    prefix:
        Key prefix within the bucket. May be empty. Leading/trailing slashes
        are preserved as-given so callers control the exact prefix passed
        to ``list_objects_v2``.
    endpoint_url:
        Optional custom S3 endpoint (e.g. Wasabi, Backblaze B2, MinIO).
    aws_profile:
        Optional named AWS profile. When set, a :class:`boto3.session.Session`
        is constructed with ``profile_name``; otherwise the default credential
        chain is used.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        endpoint_url: Optional[str] = None,
        aws_profile: Optional[str] = None,
    ):
        self.bucket = bucket
        self.prefix = prefix
        self._endpoint_url = endpoint_url
        self.aws_profile = aws_profile

    @property
    def endpoint_url(self) -> Optional[str]:
        """Return the configured endpoint URL, if any."""
        return self._endpoint_url

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> "S3Source":
        """Parse an ``s3://bucket/prefix/`` URI into an :class:`S3Source`.

        A trailing slash on the prefix is preserved if present and omitted if
        not. Empty prefixes are allowed (``s3://bucket`` or ``s3://bucket/``).
        Extra keyword arguments (``endpoint_url``, ``aws_profile``) are
        forwarded to the constructor.
        """
        parsed = urlparse(uri)
        if parsed.scheme != "s3":
            raise ValueError(f"Expected s3:// URI, got: {uri!r}")
        bucket = parsed.netloc
        if not bucket:
            raise ValueError(f"S3 URI is missing a bucket: {uri!r}")
        prefix = parsed.path.lstrip("/")
        return cls(bucket=bucket, prefix=prefix, **kwargs)

    def to_s3_uri(self, key: str) -> str:
        """Return the fully-qualified ``s3://bucket/key`` URI for ``key``."""
        return f"s3://{self.bucket}/{key}"

    def _client(self):
        """Build and return a boto3 S3 client honoring profile + endpoint."""
        boto3 = _import_boto3()
        session = boto3.session.Session(profile_name=self.aws_profile)
        return session.client("s3", endpoint_url=self._endpoint_url)

    def list_keys(self, filter_pattern: str = "*.fastq.gz") -> Iterator[str]:
        """Yield keys under ``prefix`` matching ``filter_pattern`` (fnmatch).

        Uses the ``list_objects_v2`` paginator so buckets with more than
        1000 matching objects are handled transparently.
        """
        client = self._client()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for entry in page.get("Contents", []) or []:
                key = entry.get("Key")
                if key is None:
                    continue
                if fnmatch.fnmatch(key, filter_pattern):
                    yield key
