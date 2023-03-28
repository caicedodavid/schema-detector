from dataclasses import dataclass
from schema_detector.libs.exceptions import IncompatibleSchemasException
from schema_detector.libs.constants import MODE_NULLABLE, MODE_REPEATED, MODE_REQUIRED


@dataclass
class SchemaMerger:
    schema1: dict
    schema2: dict

    def merge(self) -> dict:
        return self._merge_record_schemas(self.schema1, self.schema2)

    def _merge_record_schemas(self, accumulator_schema: dict, input_schema: dict):
        """
        Merges two record schemas into one.

        Args:
            accumulator_schema (dict): The accumulator schema.
            input_schema (dict): The input schema.

        Returns:
            dict: The merged schema.
        """
        for key, value in accumulator_schema.items():
            existing_field = input_schema.get(key)
            if existing_field:
                accumulator_schema[key] = self._merge_schemas(value, existing_field)
                input_schema.pop(key)
            else:
                is_repeated_mode = value["mode"] == MODE_REPEATED
                if not is_repeated_mode:
                    value["mode"] = MODE_NULLABLE

                accumulator_schema[key] = value

        for key, value in input_schema.items():
            is_repeated_mode = value["mode"] == MODE_REPEATED
            if not is_repeated_mode:
                value["mode"] = MODE_NULLABLE

        return accumulator_schema | input_schema

    def _merge_schemas(self, schema1: dict, schema2: dict) -> dict:
        """
        Merges two schemas into one.

        Args:
            schema1 (dict): The first schema to be merged.
            schema2 (dict): The second schema to be merged.

        Returns:
            dict: The merged schema.
        """
        field_name = schema1.get("name")
        schema1_type = schema1.get("type")
        schema2_type = schema2.get("type")
        if schema1_type != schema2_type:
            raise IncompatibleSchemasException(
                f"Got different types for field {field_name} {schema1_type} AND {schema2_type}"
            )

        schema1_mode = schema1.get("mode")
        schema2_mode = schema2.get("mode")
        is_same_mode = schema1_mode == schema2_mode
        mode = {}
        if (schema1_mode == MODE_REPEATED) and is_same_mode:
            mode = {"mode": MODE_REPEATED}
        elif (
            (schema1_mode == MODE_REPEATED) or (schema2_mode == MODE_REPEATED)
        ) and not is_same_mode:
            raise IncompatibleSchemasException(
                f"The field {field_name} is not repeated for all the records"
            )
        elif (schema1_mode == MODE_REQUIRED) and is_same_mode:
            mode = {"mode": MODE_REQUIRED}
        else:
            mode = {"mode": MODE_NULLABLE}

        if schema1_type != "RECORD":
            return schema1 | mode

        schema1_fields = schema1.get("fields")
        schema2_fields = schema2.get("fields")
        result = (
            schema1
            | mode
            | {"fields": self._merge_record_schemas(schema1_fields, schema2_fields)}
        )
        return result
