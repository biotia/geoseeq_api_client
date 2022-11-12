"""Test suite for experimental functions."""
import random
from os import environ
from unittest import TestCase

ENDPOINT = environ.get("GEOSEEQ_API_TESTING_ENDPOINT", "http://127.0.0.1:8000")


def random_str(len=12):
    """Return a random alphanumeric string of length `len`."""
    out = random.choices("abcdefghijklmnopqrtuvwxyzABCDEFGHIJKLMNOPQRTUVWXYZ0123456789", k=len)
    return "".join(out)


class TestGeoseeqApiWorkOrders(TestCase):
    """Test suite for packet building."""

    pass
