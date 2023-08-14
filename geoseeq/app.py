from .remote_object import RemoteObject


class App(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "name",
        "description",
        "long_description",
        "organization_obj",
        "is_public",
        "input_elements",
    ]
    parent_field = None

    def __init__(self, knex, uuid):
        super().__init__(self)
        self.knex = knex
        self.uuid = uuid
        self.name = ""
        self.description = ""
        self.long_description = ""

    def get_post_data(self):
        data = {
            field: getattr(self, field)
            for field in self.remote_fields if hasattr(self, field)
        }
        return data

    def _save(self):
        data = self.get_post_data()
        url = f"pipelines/{self.uuid}"
        self.knex.put(url, json=data)

    def _get(self, allow_overwrite=False):
        """Fetch the result from the server."""
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(f"pipelines/{self.uuid}")
            self.load_blob(blob, allow_overwrite=allow_overwrite)
            self.cache_blob(blob)
        else:
            self.load_blob(blob, allow_overwrite=allow_overwrite)

    def _create(self):
        data = self.get_post_data()
        url = "pipelines?format=json"
        blob = self.knex.post(url, json=data)
        self.load_blob(blob)

    def __str__(self):
        return f"<Geoseeq::App {self.name} {self.uuid} />"

    def __repr__(self):
        return f"<Geoseeq::App {self.name} {self.uuid} />"

    def pre_hash(self):
        return "APP" + self.name

    # def module(self, name, version, metadata={}):
    #     return PipelineModule(
    #         self.knex,
    #         self,
    #         name,
    #         version,
    #         metadata=metadata,
    #     )

    # def get_modules(self):
    #     url = f"pipeline_modules?pipeline={self.uuid}"
    #     result = self.knex.get(url)
    #     for result_blob in result["results"]:
    #         result = self.module(result_blob["name"], result_blob["version"])
    #         result.load_blob(result_blob)
    #         # We just fetched from the server so we change the RemoteObject
    #         # meta properties to reflect that
    #         result._already_fetched = True
    #         result._modified = False
    #         yield result