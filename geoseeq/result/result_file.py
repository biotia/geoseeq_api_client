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

from .utils import *


class ResultFile(RemoteObject):
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
    def brn(self):
        obj_type = "sample" if self.canon_url() == "sample_ar_fields" else "project"
        brn = f"brn:{self.knex.instance_code()}:{obj_type}_result_field:{self.uuid}"

    def nested_url(self):
        return self.parent.nested_url() + f"/fields/{self.name}"

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
        sname = self.parent.parent.name.replace(".", "-")
        mname = self.parent.module_name.replace(".", "-")
        fname = self.name.replace(".", "-")
        filename = join(self.parent.parent.name, f"{sname}.{mname}.{fname}.{ext}").replace(
            "::", "__"
        )
        return filename

    def get_local_filename(self):
        """Return a filename that can be used to store this field locally."""
        try:
            return basename(self.get_referenced_filename())
        except TypeError:
            return basename(self.get_blob_filename())

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

    def _create(self):
        check_json_serialization(self.stored_data)
        self.parent.idem()
        data = self.get_post_data()
        blob = self.knex.post(f"{self.canon_url()}?format=json", json=data)
        self.load_blob(blob)

    def get_download_url(self):
        """Return a URL that can be used to download the file for this result."""
        blob_type = self.stored_data.get("__type__", "").lower()
        if blob_type not in ["s3", "sra"]:
            raise TypeError("Cannot fetch a file for a BLOB type result field.")
        if blob_type == "s3":
            try:
                url = self.stored_data["presigned_url"]
            except KeyError:
                url = self.stored_data["uri"]
            if url.startswith("s3://"):
                url = self.stored_data["endpoint_url"] + "/" + url[5:]
            return url
        elif blob_type == "sra":
            url = self.stored_data["url"]
            return url

    def download_file(self, filename=None, cache=True, head=None):
        """Return a local filepath to the file this result points to."""
        if not filename:
            self._temp_filename = True
            myfile = NamedTemporaryFile(delete=False)
            myfile.close()
            filename = myfile.name
        blob_type = self.stored_data.get("__type__", "").lower()
        if cache and self._cached_filename:
            return self._cached_filename
        if blob_type == "s3":
            return self._download_s3(filename, cache, head=head)
        elif blob_type == "sra":
            return self._download_sra(filename, cache)
        elif blob_type == "ftp":
            return self._download_ftp(filename, cache)
        elif blob_type == "azure":
            return self._download_azure(filename, cache, head=head)
        else:
            raise TypeError("Cannot fetch a file for a BLOB type result field.")

    def _download_s3(self, filename, cache, head=None):
        logger.info(f"Downloading S3 file to {filename}")
        try:
            url = self.stored_data["presigned_url"]
        except KeyError:
            key = 'uri' if 'uri' in self.stored_data else 'url'
            url = self.stored_data[key]
        if url.startswith("s3://"):
            url = self.stored_data["endpoint_url"] + "/" + url[5:]
        _download_head(url, filename, head=head) 
        if cache:
            self._cached_filename = filename
        return filename

    def _download_azure(self, filename, cache, head=None):
        logger.info(f"Downloading Azure file to {filename}")
        try:
            url = self.stored_data["presigned_url"]
        except KeyError:
            key = 'uri' if 'uri' in self.stored_data else 'url'
            url = self.stored_data[key]
        _download_head(url, filename, head=head)
        if cache:
            self._cached_filename = filename
        return filename

    def _download_sra(self, filename, cache):
        return self._download_generic_url(filename, cache)

    def _download_ftp(self, filename, cache, head=None):
        logger.info(f"Downloading FTP file to {filename}")
        key = 'url' if 'url' in self.stored_data else 'uri'
        download_ftp(self.stored_data[key], filename, head=head)
        return filename

    def _download_generic_url(self, filename, cache):
        logger.info(f"Downloading generic URL file to {filename}")
        key = 'url' if 'url' in self.stored_data else 'uri'
        url = self.stored_data[key]
        urllib.request.urlretrieve(url, filename)
        if cache:
            self._cached_filename = filename
        return filename

    # DEV: to simplify the uplaod process we will use only the multipart upload. It works well for small files also.
    # This function is currently unused.
    def upload_small_file(self, filepath, optional_fields={}):
        url = f"/{self.canon_url()}/{self.uuid}/upload_s3"
        filename = basename(filepath)
        optional_fields.update(
            {
                "md5_checksum": md5_checksum(filepath),
                "file_size_bytes": getsize(filepath),
            }
        )
        data = {
            "filename": filename,
            "optional_fields": optional_fields,
        }
        response = self.knex.post(url, json=data)
        with open(filepath, "rb") as f:
            files = {"file": (filename, f)}
            requests.post(  # Not a call to geoseeq so we do not use knex
                response["url"], data=response["fields"], files=files
            )
        return self

    def multipart_upload_file(
        self,
        filepath,
        file_size,
        is_sample_result,
        optional_fields={},
        chunk_size=FIVE_MB,
        max_retries=3,
        session=None,
    ):
        """Upload a file to S3 using the multipart upload process."""
        logger.info(f"Uploading {filepath} to S3 using multipart upload.")
        n_parts = int(file_size / chunk_size) + 1
        optional_fields.update(
            {
                "md5_checksum": md5_checksum(filepath),
                "file_size_bytes": getsize(filepath),
            }
        )
        data = {
            "filename": basename(filepath),
            "optional_fields": optional_fields,
            "result_type": "sample" if is_sample_result else "group",
        }
        response = self.knex.post(f"/ar_fields/{self.uuid}/create_upload", json=data)
        upload_id = response["upload_id"]
        parts = [*range(1, n_parts + 1)]
        data = {
            "parts": parts,
            "stance": "upload-multipart",
            "upload_id": upload_id,
            "result_type": "sample" if is_sample_result else "group",
        }
        response = self.knex.post(f"/ar_fields/{self.uuid}/create_upload_urls", json=data)
        urls = response
        complete_parts = []
        logger.info(f'Starting upload for "{filepath}"')
        with open(filepath, "rb") as f:
            for num, url in enumerate(list(urls.values())):
                file_data = f.read(chunk_size)
                attempts = 0
                while attempts < max_retries:
                    try:
                        if session:
                            http_response = session.put(url, data=file_data)
                        else:
                            http_response = requests.put(url, data=file_data)
                        http_response.raise_for_status()
                        logger.debug(f"Upload for part {num + 1} succeeded.")
                        break
                    except requests.exceptions.HTTPError:
                        logger.warn(
                            f"Upload for part {num + 1} failed. Attempt {attempts + 1} of {max_retries}."
                        )
                        attempts += 1
                        if attempts == max_retries:
                            raise
                        time.sleep(10**attempts)  # exponential backoff, (10 ** 2)s default max
                complete_parts.append(
                    {"ETag": http_response.headers["ETag"], "PartNumber": num + 1}
                )

                logger.info(f'Uploaded part {num + 1} of {len(urls)} for "{filepath}"')
        response = self.knex.post(
            f"/ar_fields/{self.uuid}/complete_upload",
            json={
                "parts": complete_parts,
                "upload_id": upload_id,
                "result_type": "sample" if is_sample_result else "group",
            },
            json_response=False,
        )
        logger.info(f'Finished Upload for "{filepath}"')
        return self

    def upload_file(self, filepath, multipart_thresh=FIVE_MB, **kwargs):
        resolved_path = Path(filepath).resolve()
        file_size = getsize(resolved_path)
        is_sample_result = isinstance(self, SampleAnalysisResultField)
        return self.multipart_upload_file(filepath, file_size, is_sample_result, **kwargs)

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