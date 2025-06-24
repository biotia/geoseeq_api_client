from geoseeq.remote_object import RemoteObject


class DummyRemote(RemoteObject):
    remote_fields = []
    parent_field = None


def test_updated_at_timestamp():
    obj = DummyRemote()
    obj.updated_at = "2024-03-13T09:06:56.000000Z"
    ts = obj.updated_at_timestamp
    assert round(ts) == 1710320816
