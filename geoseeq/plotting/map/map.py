from typing import Literal
from .base_layer import (
    BaseLayer,
    base_map_layer,
    light_base_map_layer,
    geography_base_layer,
    temperature_base_layer,
    humidity_base_layer,
    precipitation_base_layer
)
from .overlay import (
    Overlay,
    admin_overlay,
    places_overlay,
)


class Map:
    """Specify an interactive map plot on GeoSeeq."""

    def __init__(self) -> None:
        self.base_layers = []
        self.overlays = []
        self.samples = "project"
        self.show_histo = True
        self.center = [0, 0]
        self.zoom = 2
        pass
    
    def set_center(self, lat, lon, zoom=2):
        """Set the map zoom by setting the center and zoom level."""
        self.center = [lat, lon]
        self.zoom = zoom
        return self
    
    def set_zoom(self, zoom):
        """Set the map zoom level."""
        self.zoom = zoom
        return self
    
    def add_custom_background_layer(self, base_layer: BaseLayer):
        """Add a custom background layer to the map."""
        self.base_layers.append(base_layer)
        return self
    
    def add_base_map(self):
        """Add a light blue base map layer to the map."""
        self.base_layers.append(base_map_layer)
        return self
    
    def add_light_base_map(self):
        """Add a white base map layer to the map."""
        self.base_layers.append(light_base_map_layer)
        return self
    
    def add_geography_base_map(self):
        """Add a base map layer with sattelite geography to the map."""
        self.base_layers.append(geography_base_layer)
        return self
    
    def add_temperature_base_map(self):
        """Add a base map layer with temperature to the map."""
        self.base_layers.append(temperature_base_layer)
        return self
    
    def add_humidity_base_map(self):
        """Add a base map layer with humidity to the map."""
        self.base_layers.append(humidity_base_layer)
        return self
    
    def add_precipitation_base_map(self):
        """Add a base map layer with precipitation to the map."""
        self.base_layers.append(precipitation_base_layer)
        return self
    
    def add_administrative_overlay(self):
        """Add an overlay with administrative boundaries to the map."""
        self.overlays.append(admin_overlay)
        return self
    
    def add_places_overlay(self):
        """Add an overlay with place names to the map."""
        self.overlays.append(places_overlay)
        return self
    
    def add_custom_overlay(self, overlay: Overlay):
        """Add a custom overlay to the map."""
        self.overlays.append(overlay)
        return self
    
    def add_project_samples(self):
        return self
    
    def add_all_samples(self):
        return self
    
    def set_histogram(self, visible: bool=True, color: str="#BBC8E3"):
        self.show_histo = visible
        self.histo_color = color
        return self
    
    def set_samples(self, samples: Literal["project", "all", "none"], color: str="#004ae4"):
        self.samples = samples
        self.samples_color = color
        return self
    
    def to_dict(self):
        out = {
            "type": "geoseeq-map",
            "version": 1,
            "settings": {
                "center": self.center,
                "flyToInit": False,
                "zoom": self.zoom,
            },
            "baseLayers": [bl.to_dict() for bl in self.base_layers],
            "overlays": [ol.to_dict() for ol in self.overlays],
            "histogram": {
                "display": self.show_histo
            },
            "samples": {
                "display": self.samples
            }
        }
        if hasattr(self, "histo_color"):
            out["histogram"]["color"] = self.histo_color
        if hasattr(self, "samples_color"):
            out["samples"]["color"] = self.samples_color
        return out
