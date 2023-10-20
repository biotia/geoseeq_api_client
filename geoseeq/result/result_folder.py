import json
import logging
import os
import time
import urllib.request
from os.path import basename, getsize, join
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests

from geoseeq.constants import FIVE_MB
from geoseeq.remote_object import RemoteObject, RemoteObjectError
from geoseeq.utils import download_ftp, md5_checksum

from .bioinfo import SampleBioInfoFolder
from .result_file import ProjectResultFile, SampleResultFile
from .utils import *


class ResultFolder(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "module_name",
        "replicate",
        "description",
        "is_private",
    ]

    @property
    def name(self):
        return self.module_name

    def _get(self, allow_overwrite=False):
        """Fetch the result from the server."""
        self.parent.idem()
        logger.debug(f"Getting AnalysisResult.")
        blob = self.get_cached_blob()
        if not blob:
            url = self.nested_url()
            if self.replicate:
                url += f"?replicate={self.replicate}"
            blob = self.knex.get(url, url_options=self.inherited_url_options)
            self.load_blob(blob, allow_overwrite=allow_overwrite)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def pre_hash(self):
        key = self.module_name + self.parent.pre_hash()
        key += self.replicate if self.replicate else ""
        return key

    def copy(self, new_parent, save=True):
        copied = new_parent.analysis_result(
            self.module_name, replicate=self.replicate, metadata=self.metadata
        )
        for field in self.get_fields():
            field.copy(copied, save=save)
        if save:
            copied.idem()
        return copied

AnalysisResult = ResultFolder # for backwards compatibility


class SampleResultFolder(ResultFolder, SampleBioInfoFolder):
    parent_field = "sample"

    def __init__(self, knex, sample, module_name, replicate=None, metadata={}, is_private=False):
        super().__init__(self)
        self.knex = knex
        self.sample = sample
        self.parent = self.sample
        self.module_name = module_name
        self.replicate = replicate
        self._get_field_cache = []
        self.metadata = metadata
        self.is_private = is_private

    def nested_url(self):
        return self.sample.nested_url() + f"/analysis_results/{self.module_name}"

    def _save(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        data["sample"] = self.sample.uuid
        url = f"sample_ars/{self.uuid}"
        d = {"data": data, "url": url, "sample_ar": self}
        logger.debug(f"Saving SampleAnalysisResult. {d}")
        self.knex.put(url, json=data, url_options=self.inherited_url_options)

    def get_post_data(self):
        """Return a dict that can be used to POST this result to the server."""
        data = {
            field: getattr(self, field)
            for field in self.remote_fields
            if hasattr(self, field) and getattr(self, field) is not None
        }
        data["sample"] = self.sample.uuid
        if self.replicate:
            data["replicate"] = self.replicate
        return data

    def _create(self):
        self.sample.idem()
        data = self.get_post_data()
        d = {"data": data, "sample_ar": self}
        logger.debug(f"Creating SampleAnalysisResult. {d}")
        blob = self.knex.post(
            f"sample_ars?format=json", json=data, url_options=self.inherited_url_options
        )
        self.load_blob(blob)

    def result_file(self, field_name, pipeline_run=None, data={}):
        d = {
            "data": data,
            "field_name": field_name,
            "pipeline_run": pipeline_run,
            "sample_ar": self,
        }
        logger.debug(f"Creating SampleAnalysisResultField for SampleAnalysisResult. {d}")
        return SampleResultFile(self.knex, self, field_name, pipeline_run=pipeline_run, data=data)

    def field(self, *args, **kwargs):
        return self.result_file(*args, **kwargs)

    def get_result_files(self, cache=True):
        """Return a list of ar-fields fetched from the server."""
        if cache and self._get_field_cache:
            for field in self._get_field_cache:
                yield field
            return
        url = f"sample_ar_fields?analysis_result_id={self.uuid}"
        # url = self.nested_url() + f"/fields"
        logger.debug(f"Fetching SampleAnalysisResultFields. {self}")
        result = self.knex.get(url)
        for result_blob in result["results"]:
            result = self.field(result_blob["name"])
            result.load_blob(result_blob)
            # We just fetched from the server so we change the RemoteObject
            # meta properties to reflect that
            result._already_fetched = True
            result._modified = False
            if cache:
                self._get_field_cache.append(result)
            else:
                yield result
        if cache:
            for field in self._get_field_cache:
                yield field

    def get_fields(self, *args, **kwargs):
        return self.get_result_files(*args, **kwargs)

    def __str__(self):
        return f"<Geoseeq::SampleResultFolder {self.module_name} {self.replicate} {self.uuid} />"

SampleAnalysisResult = SampleResultFolder # for backwards compatibility


class ProjectResultFolder(ResultFolder):
    parent_field = "grp"

    def __init__(self, knex, grp, module_name, replicate=None, metadata={}, is_private=False):
        super().__init__(self)
        self.knex = knex
        self.grp = grp
        self.parent = self.grp
        self.module_name = module_name
        self.replicate = replicate
        self.metadata = metadata
        self.is_private = is_private

    def nested_url(self):
        return self.grp.nested_url() + f"/analysis_results/{self.module_name}"

    def _save(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        data["sample_group"] = self.grp.uuid
        url = f"sample_group_ars/{self.uuid}"
        self.knex.put(url, json=data)

    def _create(self):
        self.grp.idem()
        data = {
            "sample_group": self.grp.uuid,
            "module_name": self.module_name,
        }
        if self.replicate:
            data["replicate"] = self.replicate
        if self.uuid:
            data["uuid"] = self.uuid
        blob = self.knex.post(f"sample_group_ars?format=json", json=data)
        self.load_blob(blob)

    def result_file(self, field_name, pipeline_run=None, data={}):
        return ProjectResultFile(self.knex, self, field_name, pipeline_run=pipeline_run, data=data)

    def field(self, *args, **kwargs):
        return self.result_file(*args, **kwargs)

    def get_result_files(self):
        """Return a list of ar-fields fetched from the server."""
        url = f"sample_group_ar_fields?analysis_result_id={self.uuid}"
        result = self.knex.get(url)
        for result_blob in result["results"]:
            result = self.field(result_blob["name"])
            result.load_blob(result_blob)
            # We just fetched from the server so we change the RemoteObject
            # meta properties to reflect that
            result._already_fetched = True
            result._modified = False
            yield result

    def get_fields(self, *args, **kwargs):
        return self.get_result_files(*args, **kwargs)

    def __str__(self):
        return f"<Geoseeq::ProjectResultFolder {self.module_name} {self.replicate} {self.uuid} />"

SampleGroupAnalysisResult = ProjectResultFolder # for backwards compatibility
