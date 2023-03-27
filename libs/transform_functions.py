import json
from typing import Any, List
from apache_beam.io.gcp.internal.clients.bigquery.bigquery_v2_messages import (
    TableSchema,
    TableFieldSchema,
)
from libs.constants import MODE_REQUIRED, MODE_REPEATED
from libs.exceptions import HeterogeneousListException, UnidentifiedTypeException
from libs.schema_merger import SchemaMerger


def parse_element(element: str):
    """
    Parses a string to a json object

    :param element: str - the input record as string
    :return: dict - the generated schema
    """
    return json.loads(element) or {}


def generate_schema(element: dict) -> dict:
    """
    Generates a schema from a record.

    :param element: dict - the input record as dict
    :return: dict - the generated schema
    """
    schema_with_nulls = {
        key: get_big_query_types(key, value) for key, value in element.items()
    }
    return {key: value for key, value in schema_with_nulls.items() if value is not None}


def get_big_query_types(key: str, value: Any) -> dict:
    """
    Generates a BigQuery schema for a given key-value pair.

    :param key: str - the key
    :param value: Any - the value
    :return: dict - the generated BigQuery schema
    """
    value_type = type(value).__name__
    base_schema = {"name": key, "mode": MODE_REQUIRED}
    if value_type == "NoneType":
        return
    if value_type == "str":
        return base_schema | {"type": "STRING"}
    if value_type == "int":
        return base_schema | {"type": "INTEGER"}
    if value_type == "bool":
        return base_schema | {"type": "BOOLEAN"}
    if value_type == "float":
        return base_schema | {"type": "FLOAT"}
    if value_type == "list":
        if not value:
            return None
        schema_list = [get_big_query_types(key, item) for item in value]
        if not is_elements_of_same_type(schema_list):
            raise HeterogeneousListException("Encountered a non-homogeneous list.")
        if schema_list[0].get("type") == "RECORD":
            result = schema_list[0].get("fields")

            for input_schema in schema_list[1:]:
                merger = SchemaMerger(result, input_schema.get("fields"))
                result = merger.merge()

            schema_list[0]["fields"] = result

        return schema_list[0] | {"mode": MODE_REPEATED}

    if value_type == "dict":
        if not value:
            return None
        fields = {key: get_big_query_types(key, value) for key, value in value.items()}
        return base_schema | {
            "type": "RECORD",
            "fields": {
                key: value for key, value in fields.items() if value is not None
            },
        }

    raise UnidentifiedTypeException(
        f"The type {value_type} was found in one of the records."
    )


def is_elements_of_same_type(elements: list) -> bool:
    """
    Checks if all elements in a list are the same type.

    :param elements: list - the input list
    :return: bool - True if all elements are the same, False otherwise
    """
    if not elements:
        return True
    first_element = elements[0]
    for element in elements[1:]:
        if element.get("type") != first_element.get("type"):
            return False
    return True


def get_big_query_schema(schema: dict) -> dict:
    """
    Generates our final BigQuery schema by replacing the fields dictionary for a list.

    :param element: dict - the input dictionary
    :return: dict - the generated BigQuery schema
    """
    bigquery_schema = []
    for _, value in schema.items():
        fields = value.get("fields")
        if fields is not None:
            value["fields"] = get_big_query_schema(fields)
        bigquery_schema.append(value)

    return bigquery_schema


def convert_schema_to_table_schema(schema_list: List[dict]):
    """
    Converts a list of schema dictionaries to a TableSchema object.

    :param schema_list: List[dict] - A list of dictionaries representing the schema,
    :return: TableSchema - The converted TableSchema object containing the schema.
    """
    table_schema = TableSchema()
    table_schema.fields = []

    for field in schema_list:
        field_schema = TableFieldSchema()
        field_schema.name = field["name"]
        field_schema.type = field["type"]
        field_schema.mode = field["mode"]

        if field["type"] == "RECORD":
            field_schema.fields = [
                TableFieldSchema(name=f["name"], type=f["type"], mode=f["mode"])
                for f in field["fields"]
            ]

        table_schema.fields.append(field_schema)

    return table_schema
