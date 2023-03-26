import json
from typing import List, Any
import apache_beam as beam


MODE_REQUIRED = "REQUIRED"
MODE_REPEATED = "REPEATED"
MODE_NULLABLE = "NULLABLE"


class HeterogeneousListException(Exception):
    """Raised when a non-homogeneous list is encountered."""


class UnidentifiedTypeException(Exception):
    """Raised when an unidentified type is found in one of the records."""


class IncompatibleSchemasExceptions(Exception):
    """Raised when two schemas are incompatible."""


def parse_element(element: str):
    return json.loads(element) or {}

def all_elements_are_same(elements: list):
    """
    Checks if all elements in a list are the same.

    :param elements: list - the input list
    :return: bool - True if all elements are the same, False otherwise
    """
    if not elements:
        return True
    first_element = elements[0]
    for element in elements[1:]:
        if element != first_element:
            return False
    return True


def get_big_query_schema(key: str, value: Any) -> dict:
    """
    Generates a BigQuery schema for a given key-value pair.

    :param key: str - the key
    :param value: Any - the value
    :return: dict - the generated BigQuery schema
    """
    value_type = type(value).__name__
    base_schema = {"name": key, "mode": MODE_REQUIRED}
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
        schema_list = [get_big_query_schema(key, item) for item in value]
        if not all_elements_are_same(schema_list):
            raise HeterogeneousListException("Encountered a non-homogeneous list.")
        return schema_list[0] | {"mode": MODE_REPEATED}
    
    if value_type == "dict":
        fields = {key: get_big_query_schema(key, value) for key, value in value.items()}
        return base_schema | {
            "type": "RECORD",
            "fields": {key: value for key, value in fields.items() if value is not None}
        }

    
    raise UnidentifiedTypeException(f"The type {value_type} was found in one of the records.")
            

def generate_schema(element: dict) -> dict:
    """
    Generates a BigQuery schema for a dictionary.

    :param element: dict - the input dictionary
    :return: dict - the generated BigQuery schema
    """
    return {key: get_big_query_schema(key, value) for key, value in element.items()}


class SchemaCombine(beam.CombineFn):
    """
    An implementation of a CombineFn class that combines dictionaries into one.
    """
    def create_accumulator(self) -> dict:
        """
        Creates an accumulator for combining dictionaries.
        
        :return: dict - an empty dictionary
        """
        return {}
    
    def _merge_schemas(self, schema1 : dict, schema2: dict) -> dict:
        field_name = schema1.get("name")
        if schema1.get("type") != schema2.get("type"):
            type1 = schema1.get("type")
            type2 = schema1.get("type")
            raise IncompatibleSchemasExceptions(f"Got different types for field {field_name} {type1} AND {type2}")
        
        if schema1.get("type") != "RECORD":
            schema1_mode = schema1.get("mode")
            schema2_mode = schema1.get("mode")
            if (schema1_mode == MODE_REPEATED) and (schema1_mode != schema2_mode):
                raise IncompatibleSchemasExceptions(f"The field {field_name} is not repeated for all the records")

            if (schema1_mode == MODE_REPEATED):
                return schema1

            if (schema1_mode == MODE_REQUIRED) and (schema1_mode == schema2_mode):
                return schema1
            
            return schema1 | {"mode": MODE_NULLABLE}


        if schema1.get("type") == "RECORD":
            schema2_fields = schema2.get("fields")
            for key, value in schema1.get("fields").items():
                existing_field = schema2_fields.get(key)
            if existing_field:
                schema1["fields"][key] = self._merge_schemas(value, existing_field)
                schema2_fields.pop(key)
            else:
                value["mode"] = MODE_NULLABLE
                schema1["fields"][key] = value
            
            schema1["fields"] | {key: value | {"mode": MODE_NULLABLE} for key, value in schema2.get("fields").items()}

    def add_input(self, accumulator_schema: dict, input_schema: dict) -> dict:
        """
        Adds an input dictionary to the result dictionary.
        
        :param accumulator_dict: dict - the result dictionary
        :param input_dict: dict - the input dictionary
        :return: dict - the combined result dictionary
        """
        if not accumulator_schema:
            return input_schema

        for key, value in accumulator_schema.items():
            existing_field = input_schema.get(key)
            if existing_field:
                accumulator_schema[key] = self._merge_schemas(value, existing_field)
                input_schema.pop(key)
            else:
                value["mode"] = MODE_NULLABLE
                accumulator_schema[key] = value
        
        return accumulator_schema | {key: value | {"mode": MODE_NULLABLE} for key, value in input_schema.items()}

    def merge_accumulators(self, accumulators: List[dict]) -> dict:
        """
        Merges multiple accumulators into one.
        
        :param accumulators: List[dict] - a list of accumulators
        :return: dict - the merged accumulator
        """
        result = accumulators[0]
        for input_schema in accumulators[1:]:
            for key, value in result.items():
                existing_field = input_schema.get(key)
                if existing_field:
                    result[key] = self._merge_schemas(value, existing_field)
                else:
                    value["mode"] = MODE_NULLABLE
                    result[key] = value
        
        return result

    def extract_output(self, result_dict: dict) -> dict:
        """
        Extracts the output from the result dictionary.
        
        :param result_dict: dict - the result dictionary
        :return: dict - the output dictionary
        """
        return result_dict
