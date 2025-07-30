import os
from geoseeq import Knex
from geoseeq.dashboard.dashboard import Dashboard
from geoseeq.id_constructors.from_uuids import (
    project_from_uuid,
    sample_group_ar_field_from_uuid,
)


endpoint = os.environ.get("GEOSEEQ_ENDPOINT", "")
token = os.environ.get("GEOSEEQ_API_TOKEN", "")
project_id = ""
file_id = ""
file2_id = ""


knex = Knex(endpoint)
knex.add_api_token(token)


project = project_from_uuid(knex, project_id)
dashboard = project.get_or_create_default_dashbaord()

# Rename dashboard
dashboard.title = "My default dashboard"

# Add tile
file = sample_group_ar_field_from_uuid(knex, file_id)
dashboard.add_tile(file, title="Tile title", width="full")
dashboard.save()

# Remove tiles
dashboard.tiles = []
dashboard.save()

# Add multiple tiles
file = sample_group_ar_field_from_uuid(knex, file_id)
dashboard.add_tile(file, title="Tile title", width="full")
file2 = sample_group_ar_field_from_uuid(knex, file2_id)
dashboard.add_tile(file2, title="Tile title 2", width="half")
dashboard.save()


dashboard2 = project.get_or_create_dashbaord_by_title(title="Dashboard Dix")
dashboard2.title = "Renamed dashboard"

# Set dashboard to default
dashboard2.default = True
dashboard2.add_tile(file, title="Best tile", width="half")
dashboard2.save()

# Delete dashboard
# dashboard2.delete()

# Using the constructors
dashboard3 = Dashboard(knex=knex, project=project, title="Third dashboard")
dashboard3.create()

dashboard2 = Dashboard(knex=knex, project=project, title="Renamed dashboard")
dashboard2.get()  # Load tiles
print(dashboard.tiles)
