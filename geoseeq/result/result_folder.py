import os
import urllib
from os.path import basename, dirname, getsize, isdir, isfile, join
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
    
    def link_file(self, link_type, file_path, remote_name=None):
        """Link a file to this result."""
        result_file = self.result_file(remote_name or basename(file_path))
        result_file.idem()
        result_file.link_file(link_type, file_path)
        return result_file
    
    def upload_file(self, file_path, remote_name=None, progress_tracker=None, chunk_size=FIVE_MB):
        """Upload a local file to GeoSeeq. Return a ResultFile object."""
        result_file = self.result_file(remote_name or basename(file_path))
        result_file.idem()
        result_file.upload_file(file_path, progress_tracker=progress_tracker, chunk_size=chunk_size)
        return result_file
    
    def _prepare_folder_upload(self, folder_path, recursive, hidden_files, prefix):
        """Yield result_file, local_path pairs for all files in a folder."""
        if isfile(folder_path):
            yield self.result_file(prefix), folder_path
            return
        for file_name in os.listdir(folder_path):
            if not hidden_files and file_name.startswith("."):
                continue
            if file_name in ["..", "."]:
                continue
            if file_name.startswith(".geoseeq") or file_name.endswith(".gs_downloaded"):
                continue
            file_path = join(folder_path, file_name)
            if isfile(file_path):
                yield self.result_file(join(prefix or "", file_name)), file_path
            elif recursive:
                yield from self._prepare_folder_upload(
                    file_path, recursive, hidden_files, join(prefix or "", file_name)
                )
    
    def upload_folder(
            self,
            folder_path,
            recursive=True,
            hidden_files=False,
            prefix=None,
            chunk_size=FIVE_MB,
            progress_tracker_factory=None,
        ):
        """Upload the contents of a local folder to geoseeq.
        
        If recursive is True, folders inside the folder will be
        uploaded as well. GeoSeeq does not create actual nested
        folders, rather files will get names that include the path
        to the file such as `folder/file.txt`.

        If hidden_files is True, files starting with a dot will be
        uploaded as well. This does not apply to `.` and `..` which
        are always ignored. Also ignoe `.geoseeq` folders.
        """
        for result_file, local_path in self._prepare_folder_upload(folder_path, recursive, hidden_files, prefix):
            result_file.upload_file(
                local_path,
                progress_tracker=progress_tracker_factory and progress_tracker_factory(local_path),
                chunk_size=chunk_size
            )

    def download_folder(self, local_folder_path, hidden_files=True):
        """Download the contents of this result folder to a local folder.
        
        If hidden_files is True, files starting with a dot will be downloaded as well.
        """
        for field in self.get_fields():
            if not hidden_files and field.name.startswith("."):
                continue
            local_file_path = join(local_folder_path, field.name)
            os.makedirs(dirname(local_file_path), exist_ok=True)
            field.download(local_file_path)
        return self

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
        escaped_name = urllib.parse.quote(self.module_name, safe="")
        return self.sample.nested_url() + f"/analysis_results/{escaped_name}"

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
