from .remote_object import RemoteObject
from typing import Literal


class Pipeline(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "name",
        "description",
        "long_description",
        "input_elements",
    ]
    parent_field = None

    def __init__(self, knex, name):
        super().__init__(self)
        self.knex = knex
        self.name = name
        self.description = ""
        self.long_description = ""
        self._pipeline_options = []

    def _save(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        url = f"pipelines/{self.uuid}"
        self.knex.put(url, json=data)

    def _get(self):
        """Fetch the result from the server."""
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(f"pipelines/name/{self.name}")
            self.load_blob(blob)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def _create(self):
        data = {
            "name": self.name,
            "description": self.description,
            "long_description": self.long_description,
        }
        url = "pipelines?format=json"
        blob = self.knex.post(url, json=data)
        self.load_blob(blob)

    def __str__(self):
        return f"<Geoseeq::Pipeline {self.name} {self.uuid} />"

    def __repr__(self):
        return f"<Geoseeq::Pipeline {self.name} {self.uuid} />"

    def pre_hash(self):
        return "PIPELINE" + self.name

    def module(self, name, version, metadata={}):
        return PipelineModule(
            self.knex,
            self,
            name,
            version,
            metadata=metadata,
        )

    def get_modules(self):
        url = f"pipeline_modules?pipeline={self.uuid}"
        result = self.knex.get(url)
        for result_blob in result["results"]:
            result = self.module(result_blob["name"], result_blob["version"])
            result.load_blob(result_blob)
            # We just fetched from the server so we change the RemoteObject
            # meta properties to reflect that
            result._already_fetched = True
            result._modified = False
            yield result

    def options(self):
        """Return a list of PipelineOption objects for this pipeline."""
        if not self._pipeline_options:
            self._pipeline_options = [PipelineOption.from_blob(x) for x in self.input_elements]
        return self._pipeline_options
    
    def set_option(self, opt_name, opt_val):
        """Set the value of an option for this pipeline."""
        for opt in self.options():
            if opt.name == opt_name:
                opt.set_input(opt_val)
                return
        raise ValueError(f"Unknown option: {opt_name}")
    
    def get_input_parameters(self):
        """Return a dict of input parameters for this pipeline."""
        return {x.name: x.input_val for x in self.options()}


class PipelineOption:

    def __init__(self, name, type, default, description, default_value, options=[]):
        self.name = name
        self.type = type
        self.default = default
        self.description = description
        self.default_value = default_value
        self.options = options
        self._input_val = None

    def validate_input(self, input_val):
        """Return True in input_val is valid for this option."""
        if self.type == "checkbox":
            return input_val in [True, False]
        elif self.type == "select":
            return input_val in self.options
        elif self.type == "text":
            return isinstance(input_val, str)
        else:
            raise ValueError(f"Unknown option type: {self.type}")
    
    def set_input(self, input_val):
        """Set the input value for this option."""
        if not self.validate_input(input_val):
            raise ValueError(f"Invalid input value: {input_val}")
        self._input_val = input_val

    @property
    def input_val(self):
        """Return the input value for this option."""
        if self._input_val is None:
            return self.default_value
        return self._input_val
    
    @property
    def is_set(self):
        """Return True if this option has been set."""
        return self._input_val is not None
    
    def to_readable_description(self):
        """Return a human friendly description of this option."""
        out = f"{self.name} ({self.type})\n\t{self.description}\n\t"
        if self.type == "checkbox":
            out += f"(default: {self.default_value})"
        elif self.type == "select":
            out += f"options: {', '.join(self.options)}\n\t(default: {self.default_value})"
        elif self.type == "text":
            out += f"(default: {self.default_value})"
        else:
            raise ValueError(f"Unknown option type: {self.type}")
        return out
    
    @classmethod
    def from_blob(cls, blob):
        opts = [x['value'] for x in blob.get("options", {})]
        return cls(
            blob["name"],
            blob["type"],
            blob["default_value"],
            blob["description"],
            blob["default_value"],
            options=opts
        )




class PipelineModule(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "name",
        "version",
        "metadata",
        "description",
        "long_description",
        "dependencies",
    ]
    parent_field = "pip"

    def __init__(self, knex, pipeline, name, version, metadata={}):
        super().__init__(self)
        self.knex = knex
        self.pip = pipeline
        self.name = name
        self.version = version
        self.metadata = metadata
        self.description = ""
        self.long_description = ""
        self.dependencies = []

    def _save(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        url = f"pipeline_modules/{self.uuid}"
        self.knex.put(url, json=data)

    def _get(self):
        """Fetch the result from the server."""
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(f"pipelines/{self.pip.uuid}/modules/{self.name}/{self.version}")
            self.load_blob(blob)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def _create(self):
        data = {
            "pipeline": self.pip.uuid,
            "name": self.name,
            "version": self.version,
            "metadata": self.metadata,
            "description": self.description,
            "long_description": self.long_description,
        }
        url = "pipeline_modules?format=json"
        blob = self.knex.post(url, json=data)
        self.load_blob(blob)

    def add_dependency(self, upstream):
        self.dependencies.append(upstream.uuid)

    def __str__(self):
        return f'<Geoseeq::PipelineModule "{self.name}" "{self.version}" {self.uuid} />'

    def __repr__(self):
        return f'<Geoseeq::PipelineModule "{self.name}" "{self.version}" {self.uuid} />'

    def pre_hash(self):
        return "PIPELINE_MODULE" + self.name + self.version + self.pip.pre_hash()


class PipelineRun(RemoteObject):
    remote_fields = [
        "uuid",
        "created_at",
        "updated_at",
        "sample_group",
        "sample",
        "pipeline",
        "pipeline_version",
        "user",
        "finished_at",
        "status",
        "phase",
        "error_message",
        "input_fields",
        "input_parameters",
    ]
    parent_field = None

    def __init__(
        self,
        knex,
        sample_group,
        pipeline,
        pipeline_version,
        user=None,
        sample=None,
        finished_at=None,
        status=None,
        phase=None,
        error_message=None,
        input_fields=None,
        input_parameters=None,
    ):
        super().__init__(self)
        self.knex = knex
        self.sample_group = sample_group
        self.pipeline = pipeline
        self.pipeline_version = pipeline_version
        self.sample = sample
        self.user = user
        self.finished_at = finished_at
        self.status = status
        self.phase = phase
        self.error_message = error_message
        self.input_fields = input_fields
        self.input_parameters = input_parameters

    def set_status(self, status: Literal["Running", "Finished", "Error", "Pending"]):
        """Set the status of the run and save it to the server. Returns self."""
        self.status = status
        self.save()
        return self

    def _save(self):
        data = {field: getattr(self, field) for field in self.remote_fields if hasattr(self, field)}
        url = f"app_runs/{self.uuid}"
        self.knex.put(url, json=data)

    def _get(self):
        """Fetch the result from the server."""
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(f"app_runs/{self.uuid}")
            self.load_blob(blob)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def _create(self):
        data = {
            "sample_group": self.sample_group,
            "pipeline": self.pipeline,
            "pipeline_version": self.pipeline_version,
            "sample": self.sample,
            "user": self.user,
            "finished_at": self.finished_at,
            "status": self.status,
            "phase": self.phase,
            "error_message": self.error_message,
            "input_fields": self.input_fields,
            "input_parameters": self.input_parameters,
        }
        url = f"app_runs"
        blob = self.knex.post(url, json=data)
        self.load_blob(blob)

    def pre_hash(self):
        return "PIPELINE_RUN" + self.sample_group
