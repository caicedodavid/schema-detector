from typing import List
import json
from dataclasses import dataclass
from apache_beam import DoFn
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


@dataclass
class UpdateSchema(DoFn):
    """A DoFn to update the schema of a BigQuery table."""

    project: str
    dataset: str
    table: str

    def process(self, schema: List[dict]):
        """Updates the schema of a BigQuery table.

        Args:
            schema (List[dict]): The new schema for the table.
        """
        client = bigquery.Client()
        schema = [bigquery.SchemaField.from_api_repr(field) for field in schema]
        table_ref = client.dataset(self.dataset, self.project).table(self.table)
        try:
            table = client.get_table(table_ref)
            table.schema = schema
            client.update_table(table, ["schema"])
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)

        return [True]
