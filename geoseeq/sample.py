import urllib

from .remote_object import RemoteObject
from .result import SampleResultFile, SampleResultFolder


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
        escaped_name = urllib.parse.quote(self.name, safe="")
        return self.lib.nested_url() + f"/samples/{escaped_name}"

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
    
    def _grn_to_file(self, grn):
        from geoseeq.id_constructors.from_blobs import sample_result_file_from_blob
        file_uuid = grn.split(":")[-1]
        file_blob = self.knex.get(f"sample_ar_fields/{file_uuid}")
        file = sample_result_file_from_blob(self.knex, file_blob)
        return file
    
    def get_one_fastq(self):
        """Return a 2-ple, a fastq ResultFile and a string with the read type.

        Does not download the file.
        """
        url = f"data/samples/{self.uuid}/one-fastq"
        blob = self.knex.get(url)
        file = self._grn_to_file(blob["grn"])
        return file, blob["read_type"]
    
    def get_one_fastq_folder(self, preference_order=None):
        """Return a 3-ple, <read_type:str>, <folder_name:str>, a list with reads.
        
        If the read type is paired end, the list will contain 2-ples with reads.

        Default preference order is:
            "short_read::paired_end"
            "short_read::single_end"
            "long_read::nanopore"
        """
        if preference_order is None:
            preference_order = [
                "short_read::paired_end",
                "short_read::single_end",
                "long_read::nanopore",
            ]
        all_fastqs = self.get_all_fastqs()
        for read_type in preference_order:
            if read_type in all_fastqs:
                for folder_name, reads in all_fastqs[read_type].items():
                    return read_type, folder_name, reads
        raise ValueError("No suitable fastq found")
    
    def get_all_fastqs(self):
        """Return a dict with the following structure:

        ```
        {
            "<read_type (paired end)>": {
                "<folder_name_1>": [
                    [
                        <ResultFile uuid=f12822f5-8801-49e0-9871-9647beae2cb7>,
                        <ResultFile uuid=819ec7a5-47ea-43a1-a861-589326b273c6>
                    ]
                ],
            },
            "<read_type (single end)>": {
                "<folder_name_1>": [
                        <ResultFile uuid=eaaaf0c4-883f-4e7b-89c0-8b57552596ea>
                ],
        }
        ```

        Does not download the files.
        """
        url = f"data/samples/{self.uuid}/all-fastqs"
        blob = self.knex.get(url)
        files = {}
        for read_type, folders in blob.items():
            files[read_type] = {}
            for folder_name, file_grns in folders.items():
                files[read_type][folder_name] = []
                if read_type in ["short_read::paired_end"]:
                    files[read_type][folder_name].append(
                        [
                            self._grn_to_file(file_grns[0][0]),
                            self._grn_to_file(file_grns[0][1]),
                        ]
                    )
                else:
                    files[read_type][folder_name].append(
                        self._grn_to_file(file_grns[0])
                    )
        return files
    
    def get_one_fasta(self):
        """Return a 2-ple, a fasta ResultFile and a string with the read type.

        Does not download the file.
        """
        url = f"data/samples/{self.uuid}/one-fasta"
        blob = self.knex.get(url)
        file = self._grn_to_file(blob["grn"])
        return file, blob["read_type"]

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
