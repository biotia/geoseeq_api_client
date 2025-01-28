import math
from typing import Literal

import pandas as pd

from geoseeq.id_constructors.from_blobs import smart_table_from_blob
from geoseeq.id_constructors.from_uuids import (
    project_result_file_from_uuid,
    result_file_from_uuid,
    sample_result_file_from_uuid,
)
from geoseeq.remote_object import RemoteObject
from geoseeq.result.result_folder import ProjectResultFolder, SampleResultFolder


def convert_pandas_col_type_to_geoseeq_type(col_type: str):
    if col_type == "int64":
        return "number"
    elif col_type == "float64":
        return "number"
    elif col_type == "category":
        return None
    return None


class SmartTable(RemoteObject):
    remote_fields = [
        "uuid",
        "description",
        "created_at",
        "updated_at",
        "sample_group",
        "columns",
        "connected_file_id",
    ]
    parent_field = None

    def __init__(self, knex, name, connected_file_id=None, description=""):
        super().__init__(self)
        self.knex = knex
        self.name = name
        self.description = description
        self.connected_file_id = connected_file_id

    def create(
        self,
        result_folder,
        description="",
        without_default_columns=True,
    ):
        """
        Creating a smart table includes creating a file for it in the selected result folder

        without_default_columns: if False the server creates 3 example columns.
        """

        data = {
            "name": self.name,
            "folder_id": result_folder.uuid,
            "description": description,
        }
        url = f"table?without_default_columns={without_default_columns}"
        blob = self.knex.post(url, json=data)
        result_file = result_file_from_uuid(self.knex, blob["connected_file_id"])
        result_file.upload_json({"__type__": "smartTable", "tableId": blob["uuid"]})
        self.load_blob(blob)
        return self

    def refetch(self):
        """After operations like import create column, columns in not in the response so we have to update"""
        if not self.uuid:
            return
        blob = self.knex.get(f"table/{self.uuid}")
        self.load_blob(blob, allow_overwrite=True)
        return self

    def import_data(
        self, column_names, rows, column_types={}, id_column=None, overwrite=False
    ):
        url = f"table/{self.uuid}/import"
        rowLimit = math.ceil(10000 / len(column_names))
        roundsNeeded = math.ceil(len(rows) / rowLimit)
        round = 0
        while round < roundsNeeded:
            data = {
                "columns": column_names,
                "rows": rows[round * rowLimit : (round + 1) * rowLimit],
                "column_types": column_types,
            }
            self.knex.put(url, json=data, json_response=False)
            round += 1
        self.refetch()  # columns attribute has to be updated

    def import_dataframe(self, df: pd.DataFrame, column_types={}):
        df_column_types = {
            col_name: convert_pandas_col_type_to_geoseeq_type(col_type)
            for col_name, col_type in df.dtypes.items()
            if convert_pandas_col_type_to_geoseeq_type(col_type) is not None
        }
        my_column_types = {**df_column_types, **column_types}
        df_dict = df.to_dict(orient="split")
        self.import_data(
            column_names=df_dict["columns"],
            rows=df_dict["data"],
            column_types=my_column_types,
        )

    def import_csv(self, file_path, column_types={}, **kwargs):
        df = pd.read_csv(file_path, **kwargs)
        df_dict = df.to_dict(orient="split")
        self.import_data(
            column_names=df_dict["columns"],
            rows=df_dict["data"],
            column_types=column_types,
        )

    def delete(self):
        """Delete the table record and also the connected file."""

        # The server deletes the table record also if the connected file is deleted
        if self.connected_file_id:
            result_file = result_file_from_uuid(self.connected_file_id)
            result_file.delete()

    def create_column(
        self,
        name,
        data_type="string",
        description=None,
        select_options=None,
        hidden=False,
    ):
        """Create a new column for the table"""

        data = {
            "table": self.uuid,
            "name": name,
            "data_type": data_type,
            "description": description,
            "select_options": select_options,
            "hidden": hidden,
        }
        blob = self.knex.post(f"table/{self.uuid}/columns", data)
        self.refetch()  # columns attribute has to be updated

    def order_columns(self, ordered_column_names):
        """Set the order of table's columns based on the input column name list"""

        data = {"columns": []}
        unordered_col_count = 0
        for column in sorted(self.columns, key=lambda col: col["order"]):
            if column["name"] in ordered_column_names:
                order = ordered_column_names.index(column["name"])
            else:
                order = len(ordered_column_names) + unordered_col_count
                unordered_col_count += 1

            data["columns"].append({"uuid": column["uuid"], "order": order})

        self.knex.put(f"table/{self.uuid}/columns", json=data, json_response=False)
        self.refetch()  # Update columns attribute

    def hide_columns(self, column_names):
        """Set the input columns hidden by default when table is opened"""

        data = {"columns": []}
        for column in sorted(self.columns, key=lambda col: col["order"]):
            if column["name"] in column_names:
                data["columns"].append({"uuid": column["uuid"], "hidden": True})
            else:
                continue

        self.knex.put(f"table/{self.uuid}/columns", json=data, json_response=False)
        self.refetch()

    def __str__(self):
        return f"<Geoseeq::SmartTable {self.name} {self.uuid} />"

    def __repr__(self):
        return f"<Geoseeq::SmartTable {self.name} {self.uuid} />"
