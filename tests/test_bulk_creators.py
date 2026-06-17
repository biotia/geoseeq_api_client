"""Unit tests for geoseeq.bulk_creators.

Regression coverage for the response-shape mismatch where the server's
bulk_sample_result_fields endpoint returns the list-shape serializer
(no nested `analysis_result_obj`) but the client previously tried to
build SampleResultFile objects via `sample_result_file_from_blob`,
which required that key.
"""

from unittest.mock import MagicMock

from geoseeq.bulk_creators import bulk_create_sample_result_files


def _make_file(parent_uuid, name):
    f = MagicMock()
    f.parent.uuid = parent_uuid
    f.name = name
    f.get_post_data.return_value = {"name": name, "analysis_result": parent_uuid}
    return f


def test_bulk_create_sample_result_files_parses_list_shape_response():
    """Server omits `analysis_result_obj` from this endpoint by design.

    The client must reuse the in-memory file objects passed in (matched
    by `(parent.uuid, name)`) and write the server-assigned uuid back,
    rather than calling `sample_result_file_from_blob` which would
    KeyError on the missing `analysis_result_obj`.
    """
    folder_uuid = "folder-uuid-001"
    f1 = _make_file(folder_uuid, "short_read::single_end::read_1::lane_1")
    f2 = _make_file(folder_uuid, "short_read::single_end::read_1::lane_2")
    knex = MagicMock()
    knex.post.return_value = {
        "sample_result_fields": [
            {
                "uuid": "file-uuid-1",
                "name": "short_read::single_end::read_1::lane_1",
                "analysis_result": folder_uuid,
                "stored_data": {},
                "created_at": "2026-06-17T00:00:00Z",
                "updated_at": "2026-06-17T00:00:00Z",
            },
            {},  # duplicate -- server returns falsy
            {
                "uuid": "file-uuid-2",
                "name": "short_read::single_end::read_1::lane_2",
                "analysis_result": folder_uuid,
                "stored_data": {},
                "created_at": "2026-06-17T00:00:00Z",
                "updated_at": "2026-06-17T00:00:00Z",
            },
        ],
    }

    created = bulk_create_sample_result_files(knex, [f1, f2])

    assert created == [f1, f2]
    assert f1.uuid == "file-uuid-1"
    assert f2.uuid == "file-uuid-2"
    f1.load_blob.assert_called_once()
    f2.load_blob.assert_called_once()


def test_bulk_create_sample_result_files_ignores_unknown_blobs():
    """If the server returns a blob that doesn't match an in-memory file
    (defensive: shouldn't happen in practice), skip it without crashing."""
    knex = MagicMock()
    f = _make_file("folder-uuid-001", "short_read::single_end::read_1::lane_1")
    knex.post.return_value = {
        "sample_result_fields": [
            {
                "uuid": "x",
                "name": "unknown-name",
                "analysis_result": "other-folder",
                "stored_data": {},
                "created_at": "2026-06-17T00:00:00Z",
                "updated_at": "2026-06-17T00:00:00Z",
            },
        ],
    }
    created = bulk_create_sample_result_files(knex, [f])
    assert created == []
