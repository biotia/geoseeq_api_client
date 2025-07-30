import os
from geoseeq import Knex
from geoseeq.dashboard.dashboard import SampleDashboard
from geoseeq.id_constructors.from_uuids import (
    sample_from_uuid,
    sample_result_file_from_uuid,
)


endpoint = os.environ.get("GEOSEEQ_ENDPOINT", "")
token = os.environ.get("GEOSEEQ_API_TOKEN", "")
sample_id = ""
file_id = ""
file2_id = ""


knex = Knex(endpoint)
knex.add_api_token(token)


sample = sample_from_uuid(knex, sample_id)
dashboard = sample.get_or_create_default_dashbaord()

# Add tiles
file = sample_result_file_from_uuid(knex, file_id)
dashboard.add_tile(file, title="Tile title", width="full", order=2)
file2 = sample_result_file_from_uuid(knex, file2_id)
dashboard.add_tile(file2, title="Tile title 2", width="half", order=1)
dashboard.save()


dashboard2 = sample.get_or_create_dashbaord_by_title(title="BDX Dashboard")

# Rename dahboard
# dashboard2.title = "Another title"

# Remove existing tiles
dashboard2.tiles = []

dashboard2.add_tile(file, title="Tile title 2", width="half")
dashboard2.save()

# Delete dashboard
# dashboard2.delete()
