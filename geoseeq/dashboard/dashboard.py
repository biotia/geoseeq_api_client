import logging
from typing import Literal

from geoseeq import ProjectResultFile
from geoseeq.id_constructors import result_file_from_blob
from geoseeq.id_constructors.from_ids import result_file_from_id
from geoseeq.remote_object import RemoteObject

logger = logging.getLogger("geoseeq_api")


class Dashboard(RemoteObject):
    parent_field = "project"
    remote_fields = ["uuid", "title", "default", "created_at", "updated_at"]

    def __init__(self, knex, project, title="Default dashboard", default=False):
        super().__init__(self)
        self.knex = knex
        self.project = project
        self.title = title
        self.tiles = []
        self.default = default

    def _get(self, allow_overwrite=False):
        blob = self.knex.get(f"sample_groups/{self.project.uuid}/dashboards")
        try:
            blob = [
                dashboard_blob
                for dashboard_blob in blob["results"]
                if dashboard_blob["title"] == self.title
            ][0]
        except IndexError:
            raise ValueError(f"There is no existing dashboard with title {self.title}")

        self.load_blob(blob, allow_overwrite=allow_overwrite)

        # Load tiles
        tiles_res = self.knex.get(f"sample_groups/dashboards/{self.uuid}/tiles")
        for tile_blob in tiles_res["results"]:
            tile = DashboardTile.from_blob(self, tile_blob)
            self.tiles.append(tile)

    def save(self):
        data = self._get_post_data()
        url = f"sample_groups/{self.project.uuid}/dashboard/{self.name}"
        self.knex.put(url, json=data)
        self.save_tiles()

    def save_tiles(self):
        post_data = {"tiles": [tile._get_post_data() for tile in self.tiles]}
        blob = self.knex.put(
            f"sample_groups/{self.project.uuid}/dashboard/{self.name}/tiles",
            json=post_data,
            json_response=False,
        )

    def delete(self):
        self.knex.delete(f"sample_groups/{self.project.uuid}/dashboard/{self.name}")
        self._already_fetched = False
        self._deleted = True

    def _create(self):
        post_data = {
            "title": self.title,
            "project": self.project.uuid,
            "default": self.default
            }
        blob = self.knex.post(
            f"sample_groups/{self.project.uuid}/dashboard", json=post_data
        )
        self.load_blob(blob)

    def _get_post_data(self):
        out = {
            "project": self.project.uuid,
            "title": self.title,
            "default": self.default,
        }
        return out

    def add_tile(self, result_file, title, width="half", order=None):
        result_file.get()
        tile = DashboardTile(
            self.knex, self, title, result_file, width=width, order=order
        )
        self.tiles.append(tile)
        self._modified = True

    @classmethod
    def from_blob(cls, project, blob):
        instance = cls(
            project.knex,
            project,
            blob["title"],
            blob["default"],
        )
        instance.uuid = blob["uuid"]
        return instance

    @property
    def name(self):
        return self.title

    def __str__(self):
        return f'<Geoseeq Dashboard: {self.project.grn} "{self.name}"/>'

    def __repr__(self):
        return str(self)

    @property
    def grn(self):
        return f'grn:dashboard:{self.project.uuid}:"{self.name}"'

    def pre_hash(self):
        return "DASH" + self.project.uuid + self.name


class DashboardTile:
    def __init__(self, knex, dashboard, title, result_file, width="half", order=None):
        self.knex = knex
        self.dashboard = dashboard
        self.title = title
        self.width = width
        self.result_file = result_file
        self.order = order

    def _get_post_data(self):
        out = {
            "field": self.result_file.uuid,
            "dashboard": self.dashboard.uuid,
            "width": self.width,
            "title": self.title,
            "order": self.order,
        }
        return out

    @classmethod
    def from_blob(cls, dashboard, blob):
        result_file = result_file_from_id(dashboard.knex, blob["field_obj"]["uuid"])
        return cls(
            dashboard.knex,
            dashboard,
            blob["title"],
            result_file,
            width=blob["width"],
            order=blob["order"],
        )

    def __str__(self) -> str:
        return f'<Geoseeq DashboardTile: {self.dashboard.grn} "{self.title}" />'

    def __repr__(self) -> str:
        return str(self)


class SampleDashboard(RemoteObject):
    """Dashboard client for a single sample."""

    parent_field = "sample"
    remote_fields = ["uuid", "title", "default", "created_at", "updated_at"]

    def __init__(self, knex, sample, title="Default dashboard", default=False):
        super().__init__(self)
        self.knex = knex
        self.sample = sample
        self.title = title
        self.tiles = []
        self.default = default

    def _get(self, allow_overwrite=False):
        blob = self.knex.get(f"samples/{self.sample.uuid}/dashboards")
        try:
            blob = [
                dashboard_blob
                for dashboard_blob in blob["results"]
                if dashboard_blob["title"] == self.title
            ][0]
        except IndexError:
            raise ValueError(f"There is no existing dashboard with title {self.title}")

        self.load_blob(blob, allow_overwrite=allow_overwrite)

        # Load tiles
        tiles_res = self.knex.get(f"samples/dashboards/{self.uuid}/tiles")
        for tile_blob in tiles_res["results"]:
            tile = SampleDashboardTile.from_blob(self, tile_blob)
            self.tiles.append(tile)

    def save(self):
        data = self._get_post_data()
        url = f"samples/{self.sample.uuid}/dashboards/{self.uuid}"
        self.knex.put(url, json=data)
        self.save_tiles()

    def save_tiles(self):
        post_data = {"tiles": [tile._get_post_data() for tile in self.tiles]}
        self.knex.put(
            f"samples/dashboards/{self.uuid}/tiles",
            json=post_data,
            json_response=False,
        )

    def delete(self):
        self.knex.delete(f"samples/{self.sample.uuid}/dashboards/{self.uuid}")
        self._already_fetched = False
        self._deleted = True

    def _create(self):
        post_data = {
            "title": self.title,
            "sample": self.sample.uuid,
            "default": self.default,
        }
        blob = self.knex.post(f"samples/{self.sample.uuid}/dashboards", json=post_data)
        self.load_blob(blob)

    def _get_post_data(self):
        out = {
            "sample": self.sample.uuid,
            "title": self.title,
            "default": self.default,
        }
        return out

    def add_tile(self, result_file, title, width="half", order=None):
        result_file.get()
        tile = SampleDashboardTile(
            self.knex, self, title, result_file, width=width, order=order
        )
        self.tiles.append(tile)
        self._modified = True

    @classmethod
    def from_blob(cls, sample, blob):
        instance = cls(
            sample.knex,
            sample,
            blob["title"],
            blob["default"],
        )
        instance.uuid = blob["uuid"]
        return instance

    @property
    def name(self):
        return self.title

    def __str__(self):
        return f'<Geoseeq SampleDashboard: {self.sample.brn} "{self.name}"/>'

    def __repr__(self):
        return str(self)

    @property
    def grn(self):
        return f'grn:dashboard:{self.sample.uuid}:"{self.name}"'

    def pre_hash(self):
        return "DASH" + self.sample.uuid + self.name


class SampleDashboardTile:
    def __init__(self, knex, dashboard, title, result_file, width="half", order=None):
        self.knex = knex
        self.dashboard = dashboard
        self.title = title
        self.width = width
        self.result_file = result_file
        self.order = order

    def _get_post_data(self):
        out = {
            "field": self.result_file.uuid,
            "dashboard": self.dashboard.uuid,
            "width": self.width,
            "title": self.title,
            "order": self.order,
        }
        return out

    @classmethod
    def from_blob(cls, dashboard, blob):
        result_file = result_file_from_id(dashboard.knex, blob["field_obj"]["uuid"])
        return cls(
            dashboard.knex,
            dashboard,
            blob["title"],
            result_file,
            width=blob["width"],
            order=blob["order"],
        )

    def __str__(self) -> str:
        return f'<Geoseeq DashboardTile: {self.dashboard.grn} "{self.title}" />'

    def __repr__(self) -> str:
        return str(self)
