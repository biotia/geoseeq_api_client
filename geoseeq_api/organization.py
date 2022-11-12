
from .remote_object import RemoteObject
from .sample_group import SampleGroup


class Organization(RemoteObject):
    remote_fields = [
        'uuid',
        'created_at',
        'updated_at',
        'name',
    ]
    parent_field = None

    def __init__(self, knex, name):
        super().__init__(self)
        self.knex = knex
        self.name = name

    def nested_url(self):
        return f'nested/{self.name}'

    def _save(self):
        data = {
            field: getattr(self, field)
            for field in self.remote_fields if hasattr(self, field)
        }
        url = f'organizations/{self.uuid}'
        self.knex.put(url, json=data)

    def _get(self):
        """Fetch the result from the server."""
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(self.nested_url())
            self.load_blob(blob)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def _create(self):
        blob = self.knex.post(f'organizations', json={'name': self.name})
        self.load_blob(blob)

    def sample_group(self, group_name, metadata={}, is_library=False, is_public=False):
        return SampleGroup(self.knex, self, group_name,
                           is_library=is_library, is_public=is_public,
                           metadata=metadata)

    def get_sample_groups(self):
        """Yield samplegroups fetched from the server."""
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

    def pre_hash(self):
        return 'ORG' + self.name
