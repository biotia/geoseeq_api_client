from geoseeq import Knex
from geoseeq.id_constructors.from_uuids import (
    sample_from_uuid,
    sample_result_file_from_uuid,
)
from geoseeq.smart_table import SmartTable

endpoint = "https://backend.geoseeq.com"
token = ""
sample_id = ""
file_id = ""

knex = Knex(endpoint)
knex.add_api_token(token)


sample = sample_from_uuid(knex, sample_id)

dashbaord = sample.get_or_create_default_dashboard()

# Or create/use a specific dashboard 

# dashbaord = sample.get_or_create_dashboard(
#     title="BDX Urine Dashboard",
#     default=False,
# )

# Get the file we want to add
file = sample_result_file_from_uuid(knex, file_id)
sample.add_tile_to_dashboard(
    dashboard_id=dashbaord["uuid"],
    file=file,
    title="Reads",  # If empty, the file name will be used
    width="full",  # Can be "half" or "full" (default is "full")
)
