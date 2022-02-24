"""Test suite for experimental functions."""
import random
import json
from os import environ
from os.path import join, dirname
from requests.exceptions import HTTPError
from unittest import TestCase, skip

from pangea_api import (
    Knex,
    Sample,
    Organization,
    SampleGroup,
    User,
    RemoteObjectError,
)
from pangea_api.work_orders import WorkOrderProto, WorkOrder, JobOrder

ENDPOINT = environ.get('PANGEA_API_TESTING_ENDPOINT', 'http://127.0.0.1:8000')


def random_str(len=12):
    """Return a random alphanumeric string of length `len`."""
    out = random.choices('abcdefghijklmnopqrtuvwxyzABCDEFGHIJKLMNOPQRTUVWXYZ0123456789', k=len)
    return ''.join(out)


class TestPangeaApiWorkOrders(TestCase):
    """Test suite for packet building."""

    pass
