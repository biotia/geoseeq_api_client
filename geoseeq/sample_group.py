from .analysis_result import SampleGroupAnalysisResult
from .remote_object import RemoteObject
from .sample import Sample
from .utils import paginated_iterator


class SampleGroup(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "name",
        "is_library",
        "is_public",
        "metadata",
        "long_description",
        "description",
        "bucket",
        "storage_provider_name",
    ]
    parent_field = "org"

    def __init__(
        self,
        knex,
        org,
        name,
        metadata={},
        is_library=True,
        is_public=False,
        storage_provider="default",
    ):
        super().__init__(self)
        self.knex = knex
        self.org = org
        self.name = name
        self.is_library = is_library
        self.is_public = is_public
        self._sample_cache = []
        self._deleted_sample_cache = []
        self._get_sample_cache = []
        self._get_result_cache = []
        self.metadata = metadata
        self.storage_provider = storage_provider

    def nested_url(self):
        return self.org.nested_url() + f"/sample_groups/{self.name}"

    def _save_group_obj(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        data["organization"] = self.org.uuid
        data["description"] = self.description if self.description else self.name
        url = f"sample_groups/{self.uuid}"
        self.knex.put(url, json=data)

    def _save_sample_list(self):
        sample_uuids = []
        for sample in self._sample_cache:
            sample.idem()
            sample_uuids.append(sample.uuid)
        if sample_uuids:
            url = f"sample_groups/{self.uuid}/samples"
            self.knex.post(url, json={"sample_uuids": sample_uuids})
        self._sample_cache = []

    def _delete_sample_list(self):
        sample_uuids = []
        for sample in self._deleted_sample_cache:
            sample.idem()
            sample_uuids.append(sample.uuid)
        if sample_uuids:
            url = f"sample_groups/{self.uuid}/samples"
            self.knex.delete(url, json={"sample_uuids": sample_uuids})
        self._deleted_sample_cache = []

    def _save(self):
        self._save_group_obj()
        self._save_sample_list()
        self._delete_sample_list()

    def _get(self):
        """Fetch the result from the server."""
        self.org.idem()
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(self.nested_url())
            self.load_blob(blob)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def _create(self):
        self.org.idem()
        blob = self.knex.post(
            f"sample_groups?format=json",
            json={
                "organization": self.org.uuid,
                "name": self.name,
                "is_library": self.is_library,
                "is_public": self.is_public,
                "metadata": self.metadata,
                "storage_provider_name": self.storage_provider,
            },
        )
        self.load_blob(blob)

    def add_sample(self, sample):
        """Return this group and add a sample to this group.

        Do not contact server until `.save()` is called on this group.
        """
        self._sample_cache.append(sample)
        self._modified = True
        return self

    def remove_sample(self, sample):
        """Return this group and remove a sample to this group.

        Do not contact server until `.save()` is called on this group.
        """
        self._deleted_sample_cache.append(sample)
        self._modified = True
        return self

    def sample(self, sample_name, metadata={}):
        return Sample(self.knex, self, sample_name, metadata=metadata)

    def analysis_result(self, module_name, replicate=None, metadata={}):
        return SampleGroupAnalysisResult(
            self.knex, self, module_name, replicate=replicate, metadata=metadata
        )

    def get_samples(self, cache=True, error_handler=None):
        """Yield samples fetched from the server."""
        if cache and self._get_sample_cache:
            for sample in self._get_sample_cache:
                yield sample
            return
        url = f"sample_groups/{self.uuid}/samples"
        for sample_blob in paginated_iterator(self.knex, url, error_handler=error_handler):
            sample = self.sample(sample_blob["name"])
            sample.load_blob(sample_blob)
            sample.cache_blob(sample_blob)
            # We just fetched from the server so we change the RemoteObject
            # meta properties to reflect that
            sample._already_fetched = True
            sample._modified = False
            if cache:
                self._get_sample_cache.append(sample)
            else:
                yield sample
        if cache:
            for sample in self._get_sample_cache:
                yield sample

    def get_analysis_results(self, cache=True):
        """Yield group analysis results fetched from the server."""
        if cache and self._get_result_cache:
            for ar in self._get_result_cache:
                yield ar
            return
        url = f"sample_group_ars?sample_group_id={self.uuid}"
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

    def get_manifest(self):
        """Return a manifest for this group."""
        url = f"sample_groups/{self.uuid}/manifest"
        return self.knex.get(url)

    def get_module_counts(self):
        """Return a dictionary with module counts for samples in this group."""
        url = f"sample_groups/{self.uuid}/module_counts"
        return self.knex.get(url)

    def __str__(self):
        return f"<Geoseeq::SampleGroup {self.name} {self.uuid} />"

    def __repr__(self):
        return f"<Geoseeq::SampleGroup {self.name} {self.uuid} />"

    def pre_hash(self):
        return "SG" + self.name + self.org.pre_hash()
