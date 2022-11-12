
from .tag import Tag


def get_tags_for_sample(knex, sample):
    """Return an iterator of tags attached to `sample`."""
    response = knex.get(f'contrib/tags?sample={sample.uuid}')
    for tag_blob in response['results']:
        yield Tag(knex, tag_blob['name']).load_blob(tag_blob)
