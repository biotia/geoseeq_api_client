
import urllib

from .project import Project
from .remote_object import RemoteObject


class Organization(RemoteObject):
    remote_fields = [
        'uuid',
        'created_at',
        'updated_at',
        'name',
    ]
    parent_field = None
    url_prefix = 'organizations'

    def __init__(self, knex, name):
        super().__init__(self)
        self.knex = knex
        self.name = name

    def nested_url(self):
        escaped_name = urllib.parse.quote(self.name, safe="")
        return f'nested/{escaped_name}'

    def _save(self):
        data = {
            field: getattr(self, field)
            for field in self.remote_fields if hasattr(self, field)
        }
        url = f'{self.url_prefix}/{self.uuid}'
        self.knex.put(url, json=data)

    def _get(self, allow_overwrite=False):
        """Fetch the result from the server."""
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(self.nested_url())
            self.load_blob(blob, allow_overwrite=allow_overwrite)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def _create(self):
        blob = self.knex.post(self.url_prefix, json={'name': self.name})
        self.load_blob(blob)

    def sample_group(self, *args, **kwargs):
        """Create a new project in this organization.
        
        This is an alias for project() for backwards compatibility.
        """
        return self.project(*args, **kwargs)

    def project(self, project_name, metadata={}, is_public=False):
        """Create a new project in this organization."""
        return Project(self.knex, self, project_name, is_public=is_public, metadata=metadata)

    def get_projects(self):
        """Yield projects in this org fetched from the server."""
        url = f'sample_groups?organization_id={self.uuid}'
        result = self.knex.get(url)
        for result_blob in result['results']:
            result = self.sample_group(result_blob['name'])
            result.load_blob(result_blob)
            # We just fetched from the server so we change the RemoteObject
            # meta properties to reflect that
            result._already_fetched = True
            result._modified = False
            yield result

    def get_sample_groups(self):
        """Yield projects in this org fetched from the server.

        This is an alias for get_projects() for backwards compatibility.
        """
        return self.get_projects()


    def pre_hash(self):
        return 'ORG' + self.name
