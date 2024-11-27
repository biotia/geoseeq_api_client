import json
import logging
import os
import time
import urllib.request
import urllib.parse
from os.path import basename, getsize, join
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests

from geoseeq.constants import FIVE_MB
from geoseeq.remote_object import RemoteObject, RemoteObjectError
from geoseeq.utils import download_ftp, md5_checksum
from geoseeq.knex import GeoseeqOtherError

from .utils import *
from .file_upload import ResultFileUpload
from .file_download import ResultFileDownload


class ResultFile(RemoteObject, ResultFileUpload, ResultFileDownload):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "name",
        "stored_data",
        "pipeline_run",
    ]
    optional_remote_fields = [
        "pipeline_run",
    ]
    parent_field = "parent"

    def __init__(self, knex, parent, field_name, pipeline_run=None, data={}):
        super().__init__(self)
        self.knex = knex
        self.parent = parent
        self.name = field_name
        self.stored_data = data
        self.pipeline_run = pipeline_run
        self._cached_filename = None  # Used if the field points to S3, FTP, etc
        self._temp_filename = False

    @property
    def is_sample_result(self):
        return isinstance(self, SampleAnalysisResultField)

    @property
    def brn(self):
        obj_type = "sample" if self.canon_url() == "sample_ar_fields" else "project"
        brn = f"brn:{self.knex.instance_code()}:{obj_type}_result_field:{self.uuid}"

    def has_downloadable_file(self):
        """Return True if this field has a downloadable file."""
        try:
            self.download(head=10, cache=False)
            return True
        except Exception as e:
            return False

    def nested_url(self):
        escaped_name = urllib.parse.quote(self.name, safe="")
        return self.parent.nested_url() + f"/fields/{escaped_name}"

    def get_blob_filename(self):
        sname = self.parent.parent.name.replace(".", "-")
        mname = self.parent.module_name.replace(".", "-")
        fname = self.name.replace(".", "-")
        filename = join(self.parent.parent.name, f"{sname}.{mname}.{fname}.json").replace(
            "::", "__"
        )
        return filename

    def get_referenced_filename_ext(self):
        try:
            key = [k for k in ["filename", "uri", "url"] if k in self.stored_data][0]
        except IndexError:
            raise TypeError("Cannot make a reference filename for a BLOB type result field.")
        ext = self.stored_data[key].split(".")[-1]
        if ext in ["gz"]:
            ext = self.stored_data[key].split(".")[-2] + "." + ext
        return ext

    def get_referenced_filename(self):
        ext = self.get_referenced_filename_ext()
        sname = self.parent.parent.name.replace(".", "-").replace(" ", "_").lower()
        mname = self.parent.module_name.replace(".", "-").replace(" ", "_").lower()
        fname = self.name.replace(".", "-").replace(" ", "_").lower()
        filename = f"{sname}.{mname}.{fname}.{ext}".replace(
            "::", "__"
        )
        return filename

    def get_local_filename(self):
        """Return a filename that can be used to store this field locally."""
        return self.name
        # try:
        #     return basename(self.get_referenced_filename())
        # except TypeError:
        #     return basename(self.get_blob_filename())

    def _save(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        data["analysis_result"] = self.parent.uuid
        url = f"{self.canon_url()}/{self.uuid}"
        self.knex.put(url, json=data)

    def _get(self, allow_overwrite=False):
        """Fetch the result from the server."""
        self.parent.idem()
        blob = self.knex.get(self.nested_url())
        self.load_blob(blob, allow_overwrite=allow_overwrite)

    def _get_from_list(self, allow_overwrite=False):
        """Fetch the result from the server by listing the parent's children and finding this field.
        
        This function is a workaround for files with names that have funky characters that don't 
        work well in URLs.
        """
        self.parent.idem()
        for field in self.parent.get_result_files():
            if field.name == self.name:
                print(field)
                print()
                self.load_blob(field.get_blob(), allow_overwrite=allow_overwrite)
                self._already_fetched = True
                self._modified = False
                return

    def get_post_data(self):
        """Return a dict that can be used to POST this field to the server."""
        data = {
            "analysis_result": self.parent.uuid,
            "name": self.name,
            "stored_data": self.stored_data,
            "pipeline_run": self.pipeline_run,
        }
        return data

    def link_file(self, link_type, *args, **kwargs):
        if link_type == "s3":
            return self.link_s3(*args, **kwargs)
        elif link_type == "ftp":
            return self.link_ftp(*args, **kwargs)
        elif link_type == "sra":
            return self.link_sra(*args, **kwargs)
        elif link_type == "azure":
            return self.link_azure(*args, **kwargs)
        elif link_type == "http":
            return self.link_http(*args, **kwargs)
        assert False, f"Unknown link type: {link_type}"

    def link_s3(self, url, endpoint_url=None):
        """Link this field to an S3 object.

        Args:
            url (str): The URL of the S3 object.
            endpoint_url (str): The URL of the S3 endpoint.
        """
        if endpoint_url is None:
            if not url.startswith("https://"):
                raise ValueError("endpoint_url must be specified for non-HTTPS URLs.")
            endpoint_url = "https://" + url.split("https://")[1].split("/")[0]
            url = "s3://" + url.split(endpoint_url)[1][1:]
        self.stored_data = {
            "__type__": "s3",
            "uri": url,
            "endpoint_url": endpoint_url,
        }
        return self.save()

    def link_ftp(self, url):
        """Link this field to an FTP object.

        Args:
            url (str): The URL of the FTP object.
        """
        self.stored_data = {
            "__type__": "ftp",
            "uri": url,
        }
        return self.save()

    def link_sra(self, url):
        """Link this field to an SRA object.

        Args:
            url (str): The URL of the SRA object.
        """
        self.stored_data = {
            "__type__": "sra",
            "uri": url,
        }
        return self.save()

    def link_azure(self, url):
        """Link this field to an Azure object.

        Args:
            url (str): The URL of the Azure object.
        """
        endpoint_url = "https://" + url.split("https://")[1].split("/")[0]
        self.stored_data = {
            "__type__": "azure",
            "uri": url,
            "endpoint_url": endpoint_url,
        }
        return self.save()
    
    def link_http(self, url):
        """Link this field to an HTTP object.

        Args:
            url (str): The URL of the HTTP object.
        """
        self.stored_data = {
            "__type__": "http",
            "url": url,
        }
        return self.save()
    
    def idem(self):
        try:
            return super().idem()
        except GeoseeqOtherError as e:
            if "The fields analysis_result, name must make a unique set." in str(e):
                # this typically happens when the field name has a character that doesn't work well in URLs
                self._get_from_list(allow_overwrite=True)
            else:
                raise e
        return self

    def _create(self):
        check_json_serialization(self.stored_data)
        self.parent.idem()
        data = self.get_post_data()
        blob = self.knex.post(f"{self.canon_url()}?format=json", json=data)
        self.load_blob(blob)

    def __del__(self):
        if self._temp_filename and self._cached_filename:
            os.remove(self._cached_filename)

    def pre_hash(self):
        return self.name + self.parent.pre_hash()

    def copy(self, new_parent, save=True):
        copied = new_parent.field(self.name, data=self.stored_data)
        if save:
            copied.idem()
        return copied

    def checksum(self):
        """Return a checksum for this field as a blob.

        TODO
        """
        return {"value": "", "method": "none"}

AnalysisResultField = ResultFile

class SampleResultFile(ResultFile):
    def canon_url(self):
        return "sample_ar_fields"

    def __str__(self):
        return f"<Geoseeq::SampleResultFile {self.name} {self.uuid} />"

SampleAnalysisResultField = SampleResultFile


class ProjectResultFile(ResultFile):
    def canon_url(self):
        return "sample_group_ar_fields"

    def __str__(self):
        return f"<Geoseeq::ProjectResultFile {self.name} {self.uuid} />"

SampleGroupAnalysisResultField = ProjectResultFile