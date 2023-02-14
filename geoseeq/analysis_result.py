import json
import logging
import os
import time
from os.path import basename, getsize, join
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.request import urlretrieve

import requests

from .constants import FIVE_MB
from .remote_object import RemoteObject, RemoteObjectError
from .utils import md5_checksum

logger = logging.getLogger("geoseeq_api")  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program


def diff_dicts(blob1, blob2):
    for problem in _diff_dicts("original", "$", blob1, blob2):
        yield problem
    for problem in _diff_dicts("serialized", "$", blob2, blob1):
        yield problem


def _diff_lists(suffix, depth, l1, l2):
    if len(l1) != len(l2):
        yield (f"MISMATCHED_LENGTHS:{suffix}:{depth}", len(l1), len(l2))
    for i in range(len(l1)):
        v1, v2 = l1[i], l2[i]
        if v1 != v2:
            mydepth = f"{depth}.index_{i}"
            if not isinstance(v1, type(v2)):
                yield (f"MISMATCHED_TYPES:{suffix}:{mydepth}", v1, v2)
            elif isinstance(v1, dict):
                for problem in _diff_dicts(suffix, mydepth, v1, v2):
                    yield problem
            elif isinstance(v1, list):
                for problem in _diff_lists(suffix, mydepth, v1, v2):
                    yield problem
            else:
                yield (f"MISMATCHED_LIST_VALUES:{suffix}:{mydepth}", v1, v2)


def _diff_dicts(suffix, depth, blob1, blob2):
    for k, v in blob1.items():
        if k not in blob2:
            yield (f"MISSING_KEY:{suffix}:{depth}", k, None)
        elif v != blob2[k]:
            if not isinstance(v, type(blob2[k])):
                yield (f"MISMATCHED_TYPES:{suffix}:{depth}", v, blob2[k])
            elif isinstance(v, dict):
                for problem in _diff_dicts(suffix, depth + "." + k, v, blob2[k]):
                    yield problem
            elif isinstance(v, list):
                for problem in _diff_lists(suffix, depth + "." + k, v, blob2[k]):
                    yield problem
            else:
                yield (f"MISMATCHED_DICT_VALUES:{suffix}:{depth}", v, blob2[k])


def check_json_serialization(blob):
    """Raise an error if serialization+deserialization fails to return an exact copy."""
    json_serialized = json.loads(json.dumps(blob))
    if json_serialized == blob:
        return
    if isinstance(blob, dict):
        issues = list(diff_dicts(blob, json_serialized))
    else:
        issues = [("MISMATCHED_VALUES:values_only:0", blob, json_serialized)]
    issues = [str(el) for el in issues]
    issues = "\n".join(issues)
    raise RemoteObjectError(f"JSON Serialization modifies object\nIssues:\n{issues}")


class AnalysisResult(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "module_name",
        "replicate",
        "metadata",
        "description",
        "is_private",
        "pipeline_module",
    ]

    def _get(self):
        """Fetch the result from the server."""
        self.parent.idem()
        logger.debug(f"Getting AnalysisResult.")
        blob = self.get_cached_blob()
        if not blob:
            url = self.nested_url()
            if self.replicate:
                url += f"?replicate={self.replicate}"
            blob = self.knex.get(url, url_options=self.inherited_url_options)
            self.load_blob(blob)
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


class SampleAnalysisResult(AnalysisResult):
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

    def field(self, field_name, data={}):
        d = {"data": data, "field_name": field_name, "sample_ar": self}
        logger.debug(f"Creating SampleAnalysisResultField for SampleAnalysisResult. {d}")
        return SampleAnalysisResultField(self.knex, self, field_name, data=data)

    def get_fields(self, cache=True):
        """Return a list of ar-fields fetched from the server."""
        if cache and self._get_field_cache:
            for field in self._get_field_cache:
                yield field
            return
        url = f"sample_ar_fields?analysis_result_id={self.uuid}"
        #url = self.nested_url() + f"/fields"
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

    def __str__(self):
        return f"<Geoseeq::SampleResult {self.module_name} {self.replicate} {self.uuid} />"


class SampleGroupAnalysisResult(AnalysisResult):
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
        blob = self.knex.post(f"sample_group_ars?format=json", json=data)
        self.load_blob(blob)

    def field(self, field_name, data={}):
        return SampleGroupAnalysisResultField(self.knex, self, field_name, data=data)

    def get_fields(self):
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

    def __str__(self):
        return f"<Geoseeq::SampleGroupResult {self.module_name} {self.replicate} {self.uuid} />"


class AnalysisResultField(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "name",
        "stored_data",
    ]
    parent_field = "parent"

    def __init__(self, knex, parent, field_name, data={}):
        super().__init__(self)
        self.knex = knex
        self.parent = parent
        self.name = field_name
        self.stored_data = data
        self._cached_filename = None  # Used if the field points to S3, FTP, etc
        self._temp_filename = False

    @property
    def brn(self):
        obj_type = 'sample' if self.canon_url() == 'sample_ar_fields' else 'project'
        brn = f'brn:{self.knex.instance_code()}:{obj_type}_result_field:{self.uuid}'

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
        except KeyError:
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

    def _get(self):
        """Fetch the result from the server."""
        self.parent.idem()
        blob = self.knex.get(self.nested_url())
        self.load_blob(blob)

    def get_post_data(self):
        """Return a dict that can be used to POST this field to the server."""
        data = {
            "analysis_result": self.parent.uuid,
            "name": self.name,
            "stored_data": self.stored_data,
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

    def download_file(self, filename=None, cache=True):
        """Return a local filepath to the file this result points to."""
        blob_type = self.stored_data.get("__type__", "").lower()
        if blob_type not in ["s3", "sra", "ftp"]:
            raise TypeError("Cannot fetch a file for a BLOB type result field.")
        if cache and self._cached_filename:
            return self._cached_filename
        if blob_type == "s3":
            return self._download_s3(filename, cache)
        elif blob_type == "sra":
            return self._download_sra(filename, cache)
        elif blob_type == "ftp":
            return self._download_ftp(filename, cache)

    def _download_s3(self, filename, cache):
        try:
            url = self.stored_data["presigned_url"]
        except KeyError:
            url = self.stored_data["uri"]
        if url.startswith("s3://"):
            url = self.stored_data["endpoint_url"] + "/" + url[5:]
        if not filename:
            self._temp_filename = True
            myfile = NamedTemporaryFile(delete=False)
            myfile.close()
            filename = myfile.name
        urlretrieve(url, filename)
        if cache:
            self._cached_filename = filename
        return filename

    def _download_sra(self, filename, cache):
        return self._download_generic_url(filename, cache)

    def _download_ftp(self, filename, cache):
        return self._download_generic_url(filename, cache)

    def _download_generic_url(self, filename, cache):
        url = self.stored_data["url"]
        if not filename:
            self._temp_filename = True
            myfile = NamedTemporaryFile(delete=False)
            myfile.close()
            filename = myfile.name
        urlretrieve(url, filename)
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
        optional_fields={},
        chunk_size=FIVE_MB,
        max_retries=3,
        logger=lambda x: x,
    ):
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
        }
        response = self.knex.post(f"/ar_fields/{self.uuid}/create_s3_upload", json=data)
        upload_id = response["upload_id"]
        parts = [*range(1, n_parts + 1)]
        data = {
            "parts": parts,
            "stance": 'upload-multipart',
            "upload_id": upload_id,
        }
        response = self.knex.post(f"/ar_fields/{self.uuid}/create_upload_urls", json=data)
        urls = response
        complete_parts = []
        logger(f'[INFO] Starting upload for "{filepath}"')
        with open(filepath, "rb") as f:
            for num, url in enumerate(list(urls.values())):
                file_data = f.read(chunk_size)
                attempts = 0
                while attempts < max_retries:
                    try:
                        http_response = requests.put(url, data=file_data)
                        http_response.raise_for_status()
                        break
                    except requests.exceptions.HTTPError:
                        logger(f"[WARN] Upload for part {num + 1} failed. Attempt {attempts + 1}")
                        attempts += 1
                        if attempts == max_retries:
                            raise
                        time.sleep(10**attempts)  # exponential backoff, (10 ** 2)s default max
                complete_parts.append({"ETag": http_response.headers["ETag"], "PartNumber": num + 1})
                logger(f'[INFO] Uploaded part {num + 1} of {len(urls)} for "{filepath}"')
        response = self.knex.post(
            f"/ar_fields/{self.uuid}/complete_upload_s3",
            json={
                "parts": complete_parts,
                "upload_id": upload_id,
            },
            json_response=False,
        )
        logger(f'[INFO] Finished Upload for "{filepath}"')
        return self

    def upload_file(self, filepath, multipart_thresh=FIVE_MB, **kwargs):
        resolved_path = Path(filepath).resolve()
        file_size = getsize(resolved_path)
        return self.multipart_upload_file(filepath, file_size, **kwargs)

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
        return {'value': '', 'method': 'none'}


class SampleAnalysisResultField(AnalysisResultField):
    
    def canon_url(self):
        return "sample_ar_fields"

    def __str__(self):
        return f"<Geoseeq::SampleResultField {self.name} {self.uuid} />"

class SampleGroupAnalysisResultField(AnalysisResultField):

    def canon_url(self):
        return "sample_group_ar_fields"

    def __str__(self):
        return f"<Geoseeq::SampleGroupResultField {self.name} {self.uuid} />"