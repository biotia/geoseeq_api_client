
from ...remote_object import RemoteObject
from ...sample import Sample
from ...sample_group import SampleGroup
from ...blob_constructors import sample_from_blob, sample_group_from_blob
from ...utils import paginated_iterator


class Tag(RemoteObject):
    remote_fields = [
        'uuid',
        'created_at',
        'updated_at',
        'name',
        'payload',
    ]
    parent_field = None

    def __init__(self, knex, name, payload=""):
        super().__init__(self)
        self.knex = knex
        self.name = name
        self.payload = payload

    def _save(self):
        data = {
            field: getattr(self, field)
            for field in self.remote_fields if hasattr(self, field)
        }
        url = f'contrib/tags/{self.uuid}'
        self.knex.put(url, json=data)

    def _get(self):
        """Fetch the result from the server."""
        blob = self.get_cached_blob()
        if not blob:
            blob = self.knex.get(f'contrib/tags/name/{self.name}')
            self.load_blob(blob)
            self.cache_blob(blob)
        else:
            self.load_blob(blob)

    def _create(self):
        data = {
            'name': self.name,
            'payload': self.payload,
        }
        url = 'contrib/tags/?format=json'
        blob = self.knex.post(url, json=data)
        self.load_blob(blob)

    def __call__(self, other, payload=""):
        return self.tag(other, payload=payload)

    def tag(self, other, payload=""):
        self.idem()
        if isinstance(other, Tag):
            return self._tag_tag(other, payload=payload)
        if isinstance(other, Sample):
            return self._tag_sample(other, payload=payload)
        if isinstance(other, SampleGroup):
            return self._tag_sample_group(other, payload=payload)

    def _tag_tag(self, tag, payload=""):
        url = f'contrib/tags/{self.uuid}/tags'
        data = {'tag_uuid': tag.uuid, 'payload': payload}
        self.knex.post(url, json=data)

    def _tag_sample(self, sample, payload=""):
        url = f'contrib/tags/{self.uuid}/samples'
        data = {'sample_uuid': sample.uuid, 'payload': payload}
        self.knex.post(url, json=data)

    def _tag_sample_group(self, sample_group, payload=""):
        url = f'contrib/tags/{self.uuid}/sample_groups'
        data = {'sample_group_uuid': sample_group.uuid, 'payload': payload}
        self.knex.post(url, json=data)

    def get_samples(self):
        url = f'contrib/tags/{self.uuid}/samples'
        for sample_blob in paginated_iterator(self.knex, url):
            yield sample_from_blob(self.knex, sample_blob)

    def get_sample_groups(self):
        url = f'contrib/tags/{self.uuid}/sample_groups'
        for sample_group_blob in paginated_iterator(self.knex, url):
            yield sample_group_from_blob(self.knex, sample_group_blob)

    def get_random_samples(self, n=100):
        url = f'contrib/tags/{self.uuid}/random_samples?n={n}'
        response = self.knex.get(url)
        for sample_blob in response['results']:
            yield sample_from_blob(self.knex, sample_blob)

    def __str__(self):
        return f'<Pangea::Contrib::Tag {self.name} {self.uuid} />'

    def __repr__(self):
        return str(self)

    def pre_hash(self):
        return 'TAG' + self.name
