"""Test suite for plotting library."""
import random
from os import environ
from os.path import dirname, join
from unittest import TestCase, skip

from geoseeq.plotting.map import Map


class TestGeoseeqPlotting(TestCase):
    """Test suite for packet building."""

    def test_make_map_complex(self):
        """Test that we can create a map and turn it into a dict."""
        map = Map()\
            .set_center(0, 0)\
            .set_zoom(2)\
            .add_light_base_map()\
            .add_administrative_overlay()\
            .add_places_overlay()
        map.to_dict()

    def test_make_map_simple(self):
        """Test that we can create a map and turn it into a dict."""
        map = Map()\
            .add_light_base_map()
        map.to_dict()
        
        