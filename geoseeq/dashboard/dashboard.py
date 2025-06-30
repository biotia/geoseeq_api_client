import logging
from typing import Literal

from geoseeq import ProjectResultFile
from geoseeq.id_constructors import result_file_from_blob
from geoseeq.remote_object import RemoteObject

logger = logging.getLogger("geoseeq_api")


class Dashboard(RemoteObject):
    parent_field = "project"
    remote_fields = ["is_default"]

    def __init__(self, knex, project, name="Default dashboard", is_default=False):
        super().__init__(self)
        self.knex = knex
        self.project = project
        self._name = name
        self.tiles = []
        self.is_default = is_default

    def _get(self, allow_overwrite=False):
        blob = self.knex.get(f"sample_groups/{self.project.uuid}/dashboard-list")
        blob = blob["dashboard_data"][self.name]
        for tile_blob in blob["tiles"]:
            tile = DashboardTile.from_blob(self, tile_blob)
            self.tiles.append(tile)
        blob.pop("tiles")
        self.load_blob(blob, allow_overwrite=allow_overwrite)

    def _save(self):
        self.save_tiles()

    def save_tiles(self):
        post_data = {"tiles": [tile._get_post_data() for tile in self.tiles]}
        blob = self.knex.post(
            f"sample_groups/{self.project.uuid}/dashboard/{self.name}/tiles",
            json=post_data,
            json_response=False,
        )
        print(blob)

    def _create(self):
        post_data = {"name": self.name, "is_default": self.is_default}
        blob = self.knex.post(
            f"sample_groups/{self.project.uuid}/dashboard", json=post_data
        )
        self.load_blob(blob)

    def tile(
        self,
        title,
        result_file,
        style: Literal["col-span-1", "col-span-2"] = "col-span-1",
    ):
        result_file.get()
        tile = DashboardTile(self.knex, self, title, result_file, style=style)
        self.tiles.append(tile)
        self._modified = True
        return tile

    def add_tile(self, tile):
        self.tiles.append(tile)
        self._modified = True

    @property
    def name(self):
        return self._name

    def __str__(self):
        return f'<Geoseeq Dashboard: {self.project.grn} "{self.name}"/>'

    def __repr__(self):
        return str(self)

    @property
    def grn(self):
        return f'grn:dashboard:{self.project.uuid}:"{self.name}"'

    def pre_hash(self):
        return "DASH" + self.project.uuid + self.name


class SampleDashboard(RemoteObject):
    """Dashboard client for a single sample."""

    parent_field = "sample"
    remote_fields = ["is_default"]

    def __init__(self, knex, sample, name="Default dashboard", is_default=False):
        super().__init__(self)
        self.knex = knex
        self.sample = sample
        self._name = name
        self.tiles = []
        self.is_default = is_default

    def _get(self, allow_overwrite=False):
        blob = self.knex.get(f"samples/{self.sample.uuid}/dashboard-list")
        blob = blob["dashboard_data"][self.name]
        for tile_blob in blob["tiles"]:
            tile = DashboardTile.from_blob(self, tile_blob)
            self.tiles.append(tile)
        blob.pop("tiles")
        self.load_blob(blob, allow_overwrite=allow_overwrite)

    def _save(self):
        self.save_tiles()

    def save_tiles(self):
        post_data = {"tiles": [tile._get_post_data() for tile in self.tiles]}
        self.knex.post(
            f"samples/{self.sample.uuid}/dashboard/{self.name}/tiles",
            json=post_data,
            json_response=False,
        )

    def _create(self):
        post_data = {"name": self.name, "is_default": self.is_default}
        blob = self.knex.post(f"samples/{self.sample.uuid}/dashboard", json=post_data)
        self.load_blob(blob)

    def tile(
        self,
        title,
        result_file,
        style: Literal["col-span-1", "col-span-2"] = "col-span-1",
    ):
        result_file.get()
        tile = DashboardTile(self.knex, self, title, result_file, style=style)
        self.tiles.append(tile)
        self._modified = True
        return tile

    def add_tile(self, tile):
        self.tiles.append(tile)
        self._modified = True

    @property
    def name(self):
        return self._name

    def __str__(self):
        return f'<Geoseeq SampleDashboard: {self.sample.brn} "{self.name}"/>'

    def __repr__(self):
        return str(self)

    @property
    def grn(self):
        return f'grn:dashboard:{self.sample.uuid}:"{self.name}"'

    def pre_hash(self):
        return "DASH" + self.sample.uuid + self.name


class DashboardTile:

    def __init__(self, knex, dashboard, title, result_file, style="col-span-1"):
        self.knex = knex
        self.dashboard = dashboard
        self.title = title
        self.style = style
        self.result_file = result_file

    def _get_post_data(self):
        out = {
            "field_uuid": self.result_file.uuid,
            "field_type": (
                "group" if isinstance(self.result_file, ProjectResultFile) else "sample"
            ),
            "style": self.style,
            "title": self.title,
            "has_related_field": False,
        }
        return out

    @classmethod
    def from_blob(cls, dashboard, blob):
        result_file = result_file_from_blob(dashboard.knex, blob["viz_field"])
        return cls(
            dashboard.knex, dashboard, blob["title"], result_file, style=blob["style"]
        )

    def __str__(self) -> str:
        return f'<Geoseeq DashboardTile: {self.dashboard.grn} "{self.title}" />'

    def __repr__(self) -> str:
        return str(self)
