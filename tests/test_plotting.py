"""Test suite for plotting library."""
from unittest import TestCase

from geoseeq.plotting.map import Map


class TestGeoseeqPlotting(TestCase):
    """Test basic map creation."""

    def test_make_map_complex(self):
        """Map with multiple layers converts to dict."""
        map_obj = (
            Map()
            .set_center(0, 0)
            .set_zoom(2)
            .add_light_base_map()
            .add_administrative_overlay()
            .add_places_overlay()
        )
        d = map_obj.to_dict()
        self.assertIn("baseLayers", d)
        self.assertGreaterEqual(len(d["baseLayers"]), 1)

    def test_make_map_simple(self):
        """Simple map converts to dict."""
        map_obj = Map().add_light_base_map()
        d = map_obj.to_dict()
        self.assertIn("baseLayers", d)
        self.assertEqual(len(d["baseLayers"]), 1)
