import logging
import urllib

import pandas as pd

from .pipeline import Pipeline
from .remote_object import RemoteObject
from .result import ProjectResultFolder
from .sample import Sample
from .utils import paginated_iterator

logger = logging.getLogger("geoseeq_api")


class Project(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "name",
        "privacy_level",
        "description",
        "samples_count",
    ]
    optional_remote_fields = [
        "privacy_level",
        "samples_count",
    ]
    parent_field = "org"
    url_prefix = "sample_groups"

    def __init__(
        self,
        knex,
        org,
        name,
        metadata={},
        is_library=True,
        is_public=False,
        storage_provider="default",
        privacy_level=None,
    ):
        super().__init__(self)
        self.knex = knex
        self.org = org
        self.new_org = None
        self.name = name
        self.is_library = is_library
        self.is_public = is_public
        self._sample_cache = []
        self._deleted_sample_cache = []
        self._get_sample_cache = []
        self._get_result_cache = []
        self.metadata = metadata
        self.storage_provider = storage_provider
        self.privacy_level = privacy_level

    def change_org(self, org):
        self.new_org = org
        self._modified = True

    def get_post_data(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        data["organization"] = self.org.uuid
        data['description'] = self.description if hasattr(self, 'description') and self.description else self.name
        data['privacy_level'] = self.privacy_level if hasattr(self, 'privacy_level') and self.privacy_level else 'private'
        data['storage_provider_name'] = self.storage_provider
        if self.new_org:
            if isinstance(self.new_org, RemoteObject):
                data["organization"] = self.new_org.uuid
            else:
                data["organization"] = self.new_org
        if data["uuid"] is None:
            data.pop("uuid")
        return data

    def nested_url(self):
        escaped_name = urllib.parse.quote(self.name, safe="")
        return self.org.nested_url() + f"/sample_groups/{escaped_name}"

    def _save_group_obj(self):
        data = self.get_post_data()
        url = f"sample_groups/{self.uuid}"
        self.knex.put(url, json=data)

    def _save_sample_list(self):
        sample_uuids = []
        for sample_uuid in self._sample_cache:
            sample_uuids.append(sample_uuid)
        if sample_uuids:
            url = f"sample_groups/{self.uuid}/samples"
            chunk_size = 100
            for i in range(0, len(sample_uuids), chunk_size):
                self.knex.post(url, json={"sample_uuids": sample_uuids[i : i + chunk_size]})
        self._sample_cache = []

    def _delete_sample_list(self):
        sample_uuids = []
        for sample_uuid in self._deleted_sample_cache:
            sample_uuids.append(sample_uuid)
        if sample_uuids:
            url = f"sample_groups/{self.uuid}/samples"
            self.knex.delete(url, json={"sample_uuids": sample_uuids})
        self._deleted_sample_cache = []

    def _save(self):
        self._save_group_obj()
        self._save_sample_list()
        self._delete_sample_list()

    def _get(self, allow_overwrite=False):
        """Fetch the result from the server."""
        self.org.idem()
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(self.nested_url())
            self.load_blob(blob, allow_overwrite=allow_overwrite)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def _create(self):
        self.org.idem()
        post_data = self.get_post_data()
        blob = self.knex.post(
            f"sample_groups?format=json",
            json=post_data,
        )
        self.load_blob(blob)
    
    def add_sample_uuids(self, sample_uuids):
        """Return this group and add a sample to this group.

        Do not contact server until `.save()` is called on this group.
        """
        for sample_uuid in sample_uuids:
            self._sample_cache.append(sample_uuid)
        self._modified = True
        return self

    def add_sample(self, sample):
        """Return this group and add a sample to this group.

        Do not contact server until `.save()` is called on this group.
        """
        self._sample_cache.append(sample.uuid)
        self._modified = True
        return self

    def remove_sample(self, sample):
        """Return this group and remove a sample to this group.

        Do not contact server until `.save()` is called on this group.
        """
        self._deleted_sample_cache.append(sample.uuid)
        self._modified = True
        return self

    def sample(self, sample_name, metadata={}):
        return Sample(self.knex, self, sample_name, metadata=metadata)

    def result_folder(self, module_name, replicate=None, metadata={}):
        """Return a ProjectResultFolder object for this project."""
        return ProjectResultFolder(
            self.knex, self, module_name, replicate=replicate, metadata=metadata
        )

    def analysis_result(self, *args, **kwargs):
        """Return a ProjectResultFolder object for this project.
        
        Alias for result_folder."""
        return self.result_folder(*args, **kwargs)

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

    def get_sample_uuids(self, cache=True, error_handler=None):
        """Yield samples uuids fetched from the server."""
        if cache and self._get_sample_cache:
            for sample in self._get_sample_cache:
                yield sample.uuid
            return
        url = f"sample_groups/{self.uuid}/samples"
        for sample_blob in paginated_iterator(self.knex, url, error_handler=error_handler):
            yield sample_blob['uuid']

    def _batch_sample_uuids(self, batch_size, input_sample_uuids=[]):
        """Yield batches of sample uuids."""
        uuids_to_batch = input_sample_uuids if input_sample_uuids else self.get_sample_uuids()
        sample_uuids = []
        for sample_uuid in uuids_to_batch:
            sample_uuids.append(sample_uuid)
            if len(sample_uuids) == batch_size:
                yield sample_uuids
                sample_uuids = []
        if sample_uuids:
            yield sample_uuids

    def get_analysis_results(self, cache=True):
        """Yield ProjectResultFolder objects for this project fetched from the server.
        
        Alias for get_result_folders."""
        return self.get_result_folders(cache=cache)

    def get_result_folders(self, cache=True):
        """Yield ProjectResultFolder objects for this project fetched from the server."""
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

    def get_sample_metadata(self):
        """Return a pandas dataframe with sample metadata."""
        url = f"sample_groups/{self.uuid}/samples-list?page=1&page_size=500&&"
        rows = []
        while url:
            blob = self.knex.get(url)
            rows.extend(blob["results"])
            url = blob["next"]
        return pd.DataFrame(rows)
    
    
    @property
    def n_samples(self):
        """Return the number of samples in this project."""
        if hasattr(self, 'samples_count') and self.samples_count is not None:
            return self.samples_count
        return len(list(self.get_sample_uuids()))
    
    def bulk_find_files(self,
                        sample_uuids=[],
                        sample_name_includes=[],
                        folder_types="all",
                        folder_names=[],
                        file_names=[],
                        extensions=[],
                        with_versions=False,
                        use_batches_cutoff=500):
        """Return a dict with links to download files that match the given criteria.

        Options:
        - sample_uuids: list of sample uuids; if blank search all samples in project
        - sample_name_includes: list of strings; finds samples with names that include these strings
        - folder_types: "all", "project", "sample"; finds files in folders of these types
        - folder_names: list of strings; finds files in folders that have these strings in their names
        - file_names: list of strings; finds files that have these strings in their names
        - extensions: list of strings; finds files with these file extensions
        - with_versions: bool; if True, include all versions of files in results
        """
        def _my_bulk_find(sample_uuids=None):  # curry to save typing
            return self._bulk_find_files_batch(sample_uuids=sample_uuids or [],
                                             sample_name_includes=sample_name_includes,
                                             folder_types=folder_types,
                                             folder_names=folder_names,
                                             file_names=file_names,
                                             extensions=extensions,
                                             with_versions=with_versions)
        n_samples = len(sample_uuids) if sample_uuids else self.n_samples
        if n_samples < use_batches_cutoff:
            logger.debug(f"Using single batch bulk_find for {n_samples} samples")
            return _my_bulk_find(sample_uuids=sample_uuids)
        else:
            logger.debug(f"Using multi batch bulk_find for {n_samples} samples")
            merged_response = {'file_size_bytes': 0, 'links': {}, 'no_size_info_count': 0}
            for batch in self._batch_sample_uuids(use_batches_cutoff - 1, input_sample_uuids=sample_uuids):
                response = _my_bulk_find(sample_uuids=batch)
                merged_response['file_size_bytes'] += response['file_size_bytes']
                merged_response['links'].update(response['links'])
                merged_response['no_size_info_count'] += response['no_size_info_count']
            return merged_response
                
    def _bulk_find_files_batch(self,
                               sample_uuids=None,
                               sample_name_includes=None,
                               folder_types=None,
                               folder_names=None,
                               file_names=None,
                               extensions=None,
                               with_versions=False):
        data = {
            "sample_uuids": sample_uuids or [],
            "sample_names": sample_name_includes or [],
            "folder_type": folder_types or [],
            "folder_names": folder_names or [],
            "file_names": file_names or [],
            "extensions": extensions or [],
            "with_versions": with_versions
        }
        url = f"sample_groups/{self.uuid}/download"
        response = self.knex.post(url, data)
        return response
    
    def run_app(self, app: Pipeline, input_parameters=None):
        """Run an app on this group."""
        if not input_parameters:
            input_parameters = app.get_input_parameters()
        params = {
            'pipeline_id': app.uuid,
            'sample_uuids': list(self.get_sample_uuids()),
            'input_parameters': input_parameters,
        }
        self.knex.post(f"sample_groups/{self.uuid}/run_app", json=params, json_response=False)

    def __str__(self):
        return f"<Geoseeq::Project {self.name} {self.uuid} />"

    def __repr__(self):
        return f"<Geoseeq::Project {self.name} {self.uuid} />"

    def pre_hash(self):
        return "PROJ" + self.name + self.org.pre_hash()
    
    @property
    def grn(self):
        return f"grn::project:{self.uuid}"

SampleGroup = Project  # alias for backwards compatibility