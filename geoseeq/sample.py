from .result import SampleResultFolder
from .remote_object import RemoteObject


class Sample(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "name",
        "metadata",
        "library",
        "description",
    ]
    parent_field = "lib"
    url_prefix = "samples"

    def __init__(self, knex, lib, name, metadata={}):
        super().__init__(self)
        self.knex = knex
        self.lib = lib
        self.new_lib = None
        self.name = name
        self.metadata = metadata
        self._get_result_cache = []

    @property
    def project(self):
        return self.lib

    @property
    def brn(self):
        return f'brn:{self.knex.instance_code()}:sample:{self.uuid}'

    def nested_url(self):
        return self.lib.nested_url() + f"/samples/{self.name}"

    def change_library(self, new_lib):
        self.new_lib = new_lib
        self._modified = True

    def _save(self):
        data = self.get_post_data()
        url = f"samples/{self.uuid}"
        self.knex.put(url, json=data, url_options=self.inherited_url_options)
        if self.new_lib:
            self.lib = self.new_lib
            self.new_lib = None

    def _get(self, allow_overwrite=False):
        """Fetch the result from the server."""
        self.lib.get()
        blob = self.get_cached_blob()
        if not blob:
            url = self.nested_url()
            blob = self.knex.get(url, url_options=self.inherited_url_options)
            self.load_blob(blob, allow_overwrite=allow_overwrite)
            self.cache_blob(blob)
        else:
            self.load_blob(blob, allow_overwrite=allow_overwrite)

    def get_post_data(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        data["library"] = self.lib.uuid
        if self.new_lib:
            if isinstance(self.new_lib, RemoteObject):
                data["library"] = self.new_lib.uuid
            else:
                data["library"] = self.new_lib
        if data['uuid'] is None:
            data.pop('uuid')
        return data

    def _create(self):
        assert self.lib.is_library
        self.lib.idem()
        data = self.get_post_data()
        url = f"samples"
        blob = self.knex.post(url, json=data, url_options=self.inherited_url_options)
        self.load_blob(blob)

    def delete(self):
        url = f"samples/{self.uuid}"
        self.knex.delete(url)
        self._already_fetched = False
        self._deleted = True

    def result_folder(self, module_name, replicate=None, metadata=None):
        """Return a SampleResultFolder for this sample."""
        return SampleResultFolder(
            self.knex, self, module_name, replicate=replicate, metadata=metadata
        )

    def analysis_result(self, *args, **kwargs):
        """Return a SampleResultFolder for this sample.
        
        This is an alias for result_folder."""
        return self.result_folder(*args, **kwargs)
    
    def get_result_folders(self, cache=True):
        """Yield sample analysis results fetched from the server."""
        self.get()
        if cache and self._get_result_cache:
            for ar in self._get_result_cache:
                yield ar
            return
        url =  f"sample_ars?sample_id={self.uuid}"
        result = self.knex.get(url)
        for result_blob in result["results"]:
            result = self.analysis_result(result_blob["module_name"])
            result.load_blob(result_blob)
            # We just fetched from the server so we change the RemoteObject
            # meta properties to reflect that
            result._already_fetched = True
            result._modified = False
            if cache:
                self._get_result_cache.append(result)
            else:
                yield result
        if cache:
            for ar in self._get_result_cache:
                yield ar

    def get_analysis_results(self, cache=True):
        """Yield sample analysis results fetched from the server.
        
        This is an alias for get_result_folders.
        """
        return self.get_result_folders(cache=cache)

    def get_manifest(self):
        """Return a manifest for this sample."""
        url = f"samples/{self.uuid}/manifest"
        return self.knex.get(url)

    def __str__(self):
        return f"<Geoseeq::Sample {self.name} {self.uuid} />"

    def __repr__(self):
        return f"<Geoseeq::Sample {self.name} {self.uuid} />"

    def pre_hash(self):
        return "SAMPLE" + self.name + self.lib.pre_hash()

    def copy(self, sample_group, save=True):
        copied = sample_group.sample(self.name, self.metadata)
        for ar in self.get_analysis_results():
            ar.copy(copied, save=save)
        if save:
            copied.idem()
        return copied
