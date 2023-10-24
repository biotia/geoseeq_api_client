from typing import Literal


class Overlay:

    def __init__(self, title: str, type: Literal["geojson", "vector-tile"]) -> None:
        self.title = title
        self.type = type
        self.source = None
        self.access_token = None
        self.info = ""
        self.style = {}
    
    def set_source(self, source: str, access_token: str=None):
        self.source = source
        if access_token:
            self.access_token = access_token
        return self
    
    def set_info(self, info: str):
        self.info = info
        return self
    
    def set_style(self, weight: float=1, fill_opacity: float=0, color: str="#737373"):
        self.style = {
            "weight": weight,
            "fillOpacity": fill_opacity,
            "color": color
        }
        return self
    
    def to_dict(self):
        out = {
            "id": self.title.lower(),
            "title": self.title,
            "type": self.type,
            "source": self.source,
            "info": self.info
        }
        if self.access_token:
            out["accessToken"] = self.access_token
        if self.style:
            out["style"] = self.style
        return out
    

admin_overlay = Overlay("Administration", "geojson")\
    .set_source("/maps/custom.geo.json")\
    .set_style(weight=1, fill_opacity=0, color="#737373")\
    .set_info("Static administrative boundaries. World Bank, Creative Commons Attribution 4.0 license, https://datacatalog.worldbank.org/search/dataset/0038272")

places_overlay = Overlay("Places", "vector-tile")\
    .set_source(
        "mapbox://styles/biotia/cllh2jweb01yq01mf5yl82s5e",
        "pk.eyJ1IjoiYmlvdGlhIiwiYSI6ImNsZnY2cDlzMzA0N2gzcHFrbmlza2FxZGsifQ.zIR-YJ2grxJBPyBdyxhRew"
    )\
    .set_info("Country, state, settlement labels")
