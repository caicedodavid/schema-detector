from typing import List
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import Create, CombineGlobally
from schema_detector.libs.schema_combine import SchemaCombine


@pytest.fixture
def schemas() -> List[dict]:
    return [
        {"age": {"name": "age", "mode": "REQUIRED", "type": "INTEGER"}, "gender": {"name": "gender", "mode": "REQUIRED", "type": "STRING"}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}},
        {"age": {"name": "age", "mode": "REQUIRED", "type": "INTEGER"}, "gender": {"name": "gender", "mode": "REQUIRED", "type": "STRING"}, "income": {"name": "income", "mode": "REQUIRED", "type": "INTEGER"}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}},
        {"age": {"name": "age", "mode": "REQUIRED", "type": "INTEGER"}, "gender": {"name": "gender", "mode": "REQUIRED", "type": "STRING"}, "is_student": {"name": "is_student", "mode": "REQUIRED", "type": "BOOLEAN"}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}},
        {"gender": {"name": "gender", "mode": "REQUIRED", "type": "STRING"}, "hobbies": {"name": "hobbies", "mode": "REPEATED", "type": "STRING"}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}},
        {"gender": {"name": "gender", "mode": "REQUIRED", "type": "STRING"}, "is_employed": {"name": "is_employed", "mode": "REQUIRED", "type": "BOOLEAN"}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}},
        {"gender": {"name": "gender", "mode": "REQUIRED", "type": "STRING"}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}},
        {"income": {"name": "income", "mode": "REQUIRED", "type": "INTEGER"}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}},
        {"location": {"name": "location", "mode": "REQUIRED", "type": "RECORD", "fields": {"city": {"name": "city", "mode": "REQUIRED", "type": "STRING"}, "state": {"name": "state", "mode": "REQUIRED", "type": "STRING"}}}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}},
        {"name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}, "relationship_status": {"name": "relationship_status", "mode": "REQUIRED", "type": "STRING"}},
        {"name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}, "height": {"name": "height", "mode": "REQUIRED", "type": "FLOAT"}}
    ]

@pytest.fixture
def expected() -> dict:
    return {"age": {"name": "age", "mode": "NULLABLE", "type": "INTEGER"}, "gender": {"name": "gender", "mode": "NULLABLE", "type": "STRING"}, "name": {"name": "name", "mode": "REQUIRED", "type": "STRING"}, "income": {"name": "income", "mode": "NULLABLE", "type": "INTEGER"}, "is_student": {"name": "is_student", "mode": "NULLABLE", "type": "BOOLEAN"}, "hobbies": {"name": "hobbies", "mode": "REPEATED", "type": "STRING"}, "is_employed": {"name": "is_employed", "mode": "NULLABLE", "type": "BOOLEAN"}, "location": {"name": "location", "mode": "NULLABLE", "type": "RECORD", "fields": {"city": {"name": "city", "mode": "REQUIRED", "type": "STRING"}, "state": {"name": "state", "mode": "REQUIRED", "type": "STRING"}}}, "relationship_status": {"name": "relationship_status", "mode": "NULLABLE", "type": "STRING"}, "height": {"name": "height", "mode": "NULLABLE", "type": "FLOAT"}}

def test_schema_combine(pipeline, schemas):
    expected = [[{'name': 'age', 'mode': 'NULLABLE', 'type': 'INTEGER'}, {'name': 'gender', 'mode': 'NULLABLE', 'type': 'STRING'}, {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}, {'name': 'income', 'mode': 'NULLABLE', 'type': 'INTEGER'}, {'name': 'is_student', 'mode': 'NULLABLE', 'type': 'BOOLEAN'}, {'name': 'hobbies', 'mode': 'REPEATED', 'type': 'STRING'}, {'name': 'is_employed', 'mode': 'NULLABLE', 'type': 'BOOLEAN'}, {'name': 'location', 'mode': 'NULLABLE', 'type': 'RECORD', 'fields': [{'name': 'city', 'mode': 'REQUIRED', 'type': 'STRING'}, {'name': 'state', 'mode': 'REQUIRED', 'type': 'STRING'}]}, {'name': 'relationship_status', 'mode': 'NULLABLE', 'type': 'STRING'}, {'name': 'height', 'mode': 'NULLABLE', 'type': 'FLOAT'}]]
    result = (
        pipeline
        | Create(schemas)
        | CombineGlobally(SchemaCombine())
    )

    assert_that(result, equal_to(expected))

def test_create_accumulator():
    schema_combine = SchemaCombine()
    assert schema_combine.create_accumulator() == {}

def test_add_input(schemas, expected):
    schema_combine = SchemaCombine()
    age_field = expected.pop("age")
    input_schema = schemas[0]
    acc = schema_combine.add_input(expected, input_schema)
    assert acc == ({"age": age_field} | expected)

def test_merge_accumulators(schemas, expected):
    schema_combine = SchemaCombine()
    acc = schema_combine.merge_accumulators(schemas)
    assert acc == expected
    