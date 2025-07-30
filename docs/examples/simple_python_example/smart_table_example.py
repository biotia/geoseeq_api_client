import os
import pandas as pd

from geoseeq import Knex
from geoseeq.id_constructors.from_uuids import (
    project_result_folder_from_uuid,
    smart_table_from_uuid,
)
from geoseeq.smart_table import SmartTable

endpoint = os.environ.get("GEOSEEQ_ENDPOINT", "")
token = os.environ.get("GEOSEEQ_API_TOKEN", "")
folder_id = ""

knex = Knex(endpoint)
knex.add_api_token(token)


table = SmartTable(knex, "Flu report")

# Need a target folder for creating table
folder = project_result_folder_from_uuid(knex, folder_id)
table.create(result_folder=folder)

# Import table data as python lists
columns = ["text", "number", "date"]
rows = [["text", 8, "2012-02-12"], ["line2", 12, "2023-06-21"]]
column_types = {
    "number": "number",
    "date": "date",
}  # string is the default no need to specify
table.import_data(column_names=columns, rows=rows, column_types=column_types)

# Import table data from csv
table.import_csv("test.csv")

# Import table data from pandas dataframe
d = {"col1": [1, 2], "col2": [3, 4]}
df = pd.DataFrame(data=d)
table.import_dataframe("df")

table_id = table.uuid  # Or add your table id here

# Table can be constructed from uuid
table = smart_table_from_uuid(knex, table_id)

# Create new empty column
table.create_column(
    "Surface material",
    data_type="singleselect",
    select_options=["metal", "wood", "plastic"],
    description="Surface material of the sampling",
)

# Order table columns by a list of column names
table.order_columns(["date", "number", "text"])

# Hide columns by default
table.hide_columns(["text"])
