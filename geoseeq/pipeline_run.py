from .remote_object import RemoteObject


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
