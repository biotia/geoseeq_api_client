from geoseeq.result import ResultFile
from typing import Literal


class BaseLayer:

    def __init__(self,
                 title,
                 type: Literal["vector-tile", "raster-tile", "image"],
                 source=None,
                 access_token=None,
                 info=""):
        self.title = title
        self.type = type
        self.source = source
        self.access_token = access_token
        self.info = info

    def set_source(self, source, access_token=None):
        self.source = source
        if access_token:
            self.access_token = access_token
        return self

    def set_info(self, info):
        self.info = info
        return self
    
    def set_bounds(self, lat_n, lon_w, lat_s, lon_e):
        self.bounds = [
            [lat_n, lon_w],
            [lat_s, lon_e]
        ]
        return self
    
    def set_legend(self, label, value_range):
        self.legend = {
            "label": label,
            "valueRange": value_range
        }
        return self
    
    def set_colors(self, *colors):
        self.color_range = list(colors)
        return self
    
    def set_connected_overlays(self, *overlays):
        self.connected_overlays = list(overlays)
        return self

    def to_dict(self):
        out = {
            "id": self.title.lower(),
            "title": self.title,
            "type": self.type,
            "source": self.source,
            "info": self.info,
        }
        if self.access_token:
            out["accessToken"] = self.access_token
        if hasattr(self, "connectedOverlays"):
            out["connectedOverlays"] = self.connected_overlays
        if hasattr(self, "legend"):
            out["legend"] = self.legend
        if hasattr(self, "colorRange"):
            out["colorRange"] = self.color_range
        if hasattr(self, "bounds"):
            out["bounds"] = self.bounds
        return out


base_map_layer = BaseLayer("Base", "vector-tile")\
    .set_source(
        "mapbox://styles/biotia/clmgqcx2103oq01p96ifm4kda",
         "pk.eyJ1IjoiYmlvdGlhIiwiYSI6ImNsZnY2cDlzMzA0N2gzcHFrbmlza2FxZGsifQ.zIR-YJ2grxJBPyBdyxhRew"
    )

light_base_map_layer = BaseLayer("Light", "vector-tile")\
    .set_source(
        "mapbox://styles/biotia/clmd9upef03co01qihm0fgaz6",
        "pk.eyJ1IjoiYmlvdGlhIiwiYSI6ImNsZnY2cDlzMzA0N2gzcHFrbmlza2FxZGsifQ.zIR-YJ2grxJBPyBdyxhRew"
    )

geography_base_layer = BaseLayer("Geography", "raster-tile")\
    .set_source(
        "https://api.mapbox.com/styles/v1/mapbox/satellite-v9/tiles/{z}/{x}/{y}?access_token=pk.eyJ1IjoidWxxdWlyb2xhIiwiYSI6ImNsa2pnZDBpNDBiajMzZm1xYTlsdDZ5dTUifQ.9XiWdIBNzXe09uiBzo3www"
    )

temperature_base_layer = BaseLayer("Temperature", "image")\
    .set_source("/maps/NASA_20221101_MOD_LSTD_M_3600x1800_3857_color.png")\
    .set_legend("Temperature (°C)", [-25, 45])\
    .set_colors("#56a3f0", "#ebdf76", "#f9693b")\
    .set_bounds(-90, -180, 90, 180)\
    .set_info("Daytime land surface temperatures detected by Terra satellite's MODIS device in clear-sky conditions through November 2022 and processed by NASA Earth Observatory.")\
    .set_connected_overlays("administration", "places")

humidity_base_layer = BaseLayer("Humidity", "image")\
    .set_source("/maps/NASA_20230201_EO_Aqua_MODIS_Water_vapor_3600x1800_3857_color.png")\
    .set_legend("Humidity (cm)", [0, 6])\
    .set_colors("#ababab6b", "#ffffff", "#9cd8e9", "#2e8bb3", "#018571")\
    .set_bounds(-90, -180, 90, 180)\
    .set_info("5 km² atmospheric column water vapor estimates from clear sky moisture profiles through February 2023 of Aqua satellite's moderate resolution imaging spectroradiometer (MODIS) processed by NASA Earth Observatory.")\
    .set_connected_overlays("administration", "places")

precipitation_base_layer = BaseLayer("Precipitation", "image")\
    .set_source("/maps/NASA_20230201_EO_Precipitation_3600x1800_3857_color.png")\
    .set_legend("Average Precipitation (mm)", [0, 3000])\
    .set_colors("#E6E7E8", "#005645")\
    .set_bounds(-90, -180, 90, 180)\
    .set_info("Integrated multi-satellite retrievals (IMERGI) of liquid and solid precipitation through February 2023 from the Global Precipitation Measurement (GPM) Mission's Core Observatory satellite constellation.")\
    .set_connected_overlays("administration", "places")
