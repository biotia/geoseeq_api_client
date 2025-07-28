from geoseeq import Knex
from geoseeq.dashboard.dashboard import SampleDashboard
from geoseeq.id_constructors.from_uuids import (
    sample_from_uuid,
    sample_result_file_from_uuid,
)


endpoint = "http://localhost:8000"
token = "4924161176692280fa1407bed43e4148c74d40412b56b41a1bd1b940v0"
sample_id = "e0baa5bf-3487-493c-9af7-fa171e88adb1"
file_id = "4414c6b4-9685-46ac-a2f1-4c9838204d31"
file2_id = "dbfb6e4c-3a0d-4be3-b595-50a43843ef6c"


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
