"""Unit tests for :class:`geoseeq.sources.S3Source`.

Boto3 is mocked rather than required at test time; the listing logic is
exercised through a fake paginator and the missing-import error path is
forced via ``sys.modules`` patching.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from geoseeq.sources import S3Source

# ---------------------------------------------------------------------------
# from_uri parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "uri, expected_bucket, expected_prefix",
    [
        ("s3://my-bucket/reads/", "my-bucket", "reads/"),
        ("s3://my-bucket/reads", "my-bucket", "reads"),
        ("s3://my-bucket/", "my-bucket", ""),
        ("s3://my-bucket", "my-bucket", ""),
        ("s3://my-bucket/a/b/c/", "my-bucket", "a/b/c/"),
    ],
)
def test_from_uri_parses_bucket_and_prefix(uri, expected_bucket, expected_prefix):
    """``from_uri`` should split bucket and prefix and preserve trailing slash."""
    src = S3Source.from_uri(uri)
    assert src.bucket == expected_bucket
    assert src.prefix == expected_prefix


def test_from_uri_forwards_kwargs():
    """Extra kwargs flow through to the constructor."""
    src = S3Source.from_uri("s3://b/p/", endpoint_url="https://s3.example", aws_profile="dev")
    assert src.endpoint_url == "https://s3.example"
    assert src.aws_profile == "dev"


def test_from_uri_rejects_non_s3_scheme():
    """Non-s3 schemes raise ValueError, not boto3 errors later."""
    with pytest.raises(ValueError):
        S3Source.from_uri("https://example.com/foo")


def test_from_uri_rejects_missing_bucket():
    """``s3:///foo`` has no bucket and should be rejected up-front."""
    with pytest.raises(ValueError):
        S3Source.from_uri("s3:///foo")


# ---------------------------------------------------------------------------
# to_s3_uri / endpoint_url property
# ---------------------------------------------------------------------------


def test_to_s3_uri_round_trip():
    """``to_s3_uri`` formats keys back into addressable S3 URIs."""
    src = S3Source("bkt", "reads/")
    assert src.to_s3_uri("reads/a_R1.fastq.gz") == "s3://bkt/reads/a_R1.fastq.gz"


def test_endpoint_url_property_reflects_constructor():
    """The ``endpoint_url`` property mirrors what was passed in."""
    assert S3Source("b", "p", endpoint_url="https://e.example").endpoint_url == (
        "https://e.example"
    )
    assert S3Source("b", "p").endpoint_url is None


# ---------------------------------------------------------------------------
# list_keys — filtering + pagination
# ---------------------------------------------------------------------------


def _fake_boto3_with_pages(pages):
    """Return a MagicMock standing in for the ``boto3`` module.

    The mock's ``session.Session(...).client("s3").get_paginator("list_objects_v2")
    .paginate(...)`` chain yields the given ``pages`` (a list of dicts).
    """
    boto3 = MagicMock()
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = iter(pages)
    client.get_paginator.return_value = paginator
    boto3.session.Session.return_value.client.return_value = client
    return boto3, client, paginator


def test_list_keys_filters_by_pattern():
    """Only keys matching the fnmatch pattern are yielded."""
    pages = [
        {
            "Contents": [
                {"Key": "reads/a_R1.fastq.gz"},
                {"Key": "reads/a_R1.bam"},
                {"Key": "reads/b_R2.fastq.gz"},
                {"Key": "reads/notes.txt"},
            ]
        }
    ]
    boto3_mock, _, _ = _fake_boto3_with_pages(pages)

    with patch("geoseeq.sources.s3._import_boto3", return_value=boto3_mock):
        src = S3Source("bkt", "reads/")
        keys = list(src.list_keys("*.fastq.gz"))

    assert keys == ["reads/a_R1.fastq.gz", "reads/b_R2.fastq.gz"]


def test_list_keys_paginates_across_multiple_pages():
    """Pagination across >1000 objects works: the paginator yields multiple pages."""
    page1_contents = [{"Key": f"reads/sample_{i:04d}.fastq.gz"} for i in range(1000)]
    page2_contents = [{"Key": f"reads/sample_{i:04d}.fastq.gz"} for i in range(1000, 1500)]
    pages = [{"Contents": page1_contents}, {"Contents": page2_contents}]
    boto3_mock, client, paginator = _fake_boto3_with_pages(pages)

    with patch("geoseeq.sources.s3._import_boto3", return_value=boto3_mock):
        src = S3Source("bkt", "reads/")
        keys = list(src.list_keys("*.fastq.gz"))

    assert len(keys) == 1500
    assert keys[0] == "reads/sample_0000.fastq.gz"
    assert keys[-1] == "reads/sample_1499.fastq.gz"
    paginator.paginate.assert_called_once_with(Bucket="bkt", Prefix="reads/")
    client.get_paginator.assert_called_once_with("list_objects_v2")


def test_list_keys_handles_empty_page():
    """Pages with no ``Contents`` key (empty bucket) are handled cleanly."""
    boto3_mock, _, _ = _fake_boto3_with_pages([{}])
    with patch("geoseeq.sources.s3._import_boto3", return_value=boto3_mock):
        src = S3Source("bkt", "reads/")
        assert list(src.list_keys()) == []


# ---------------------------------------------------------------------------
# Profile + endpoint pass-through
# ---------------------------------------------------------------------------


def test_aws_profile_passes_through_to_boto3_session():
    """``aws_profile`` reaches ``boto3.session.Session(profile_name=...)``."""
    boto3_mock, _, _ = _fake_boto3_with_pages([{"Contents": []}])
    with patch("geoseeq.sources.s3._import_boto3", return_value=boto3_mock):
        src = S3Source("bkt", "reads/", aws_profile="staging")
        list(src.list_keys())

    boto3_mock.session.Session.assert_called_once_with(profile_name="staging")
    boto3_mock.session.Session.return_value.client.assert_called_once_with("s3", endpoint_url=None)


def test_endpoint_url_passes_through_to_client():
    """``endpoint_url`` is forwarded to ``session.client('s3', endpoint_url=...)``."""
    boto3_mock, _, _ = _fake_boto3_with_pages([{"Contents": []}])
    with patch("geoseeq.sources.s3._import_boto3", return_value=boto3_mock):
        src = S3Source("bkt", "reads/", endpoint_url="https://s3.wasabisys.com")
        list(src.list_keys())

    boto3_mock.session.Session.return_value.client.assert_called_once_with(
        "s3", endpoint_url="https://s3.wasabisys.com"
    )


# ---------------------------------------------------------------------------
# Missing-boto3 error path
# ---------------------------------------------------------------------------


def test_missing_boto3_raises_install_hint():
    """Methods that need boto3 raise ImportError with the [s3] extra hint."""
    src = S3Source("bkt", "reads/")
    with patch.dict(sys.modules, {"boto3": None}):
        with pytest.raises(ImportError) as excinfo:
            list(src.list_keys())
    assert "pip install 'geoseeq[s3]'" in str(excinfo.value)


def test_import_succeeds_without_boto3():
    """``from geoseeq.sources import S3Source`` must not require boto3.

    We can't actually un-install boto3 here, but constructing an S3Source
    and accessing pure-Python attributes should never touch boto3 — verified
    by confirming the constructor + properties + ``to_s3_uri`` work with
    boto3 patched to None in sys.modules.
    """
    with patch.dict(sys.modules, {"boto3": None}):
        src = S3Source.from_uri("s3://bkt/reads/")
        assert src.bucket == "bkt"
        assert src.prefix == "reads/"
        assert src.endpoint_url is None
        assert src.to_s3_uri("x") == "s3://bkt/x"
