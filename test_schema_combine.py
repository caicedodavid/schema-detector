import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import Create, CombineGlobally
from transforms import (SchemaCombine, all_elements_are_same, get_big_query_schema, generate_schema, UnidentifiedTypeException, HeterogeneousListException, )


@pytest.fixture(scope='module')
def test_pipeline():
    with TestPipeline() as p:
        yield p

def test_schema_combine(test_pipeline):
    input_schemas = [
        {'age': {'name': 'age', 'mode': 'REQUIRED', 'type': 'INTEGER'}, 'gender': {'name': 'gender', 'mode': 'REQUIRED', 'type': 'STRING'}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'age': {'name': 'age', 'mode': 'REQUIRED', 'type': 'INTEGER'}, 'gender': {'name': 'gender', 'mode': 'REQUIRED', 'type': 'STRING'}, 'income': {'name': 'income', 'mode': 'REQUIRED', 'type': 'INTEGER'}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'age': {'name': 'age', 'mode': 'REQUIRED', 'type': 'INTEGER'}, 'gender': {'name': 'gender', 'mode': 'REQUIRED', 'type': 'STRING'}, 'is_student': {'name': 'is_student', 'mode': 'REQUIRED', 'type': 'BOOLEAN'}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'gender': {'name': 'gender', 'mode': 'REQUIRED', 'type': 'STRING'}, 'hobbies': {'name': 'hobbies', 'mode': 'REPEATED', 'type': 'STRING'}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'gender': {'name': 'gender', 'mode': 'REQUIRED', 'type': 'STRING'}, 'is_employed': {'name': 'is_employed', 'mode': 'REQUIRED', 'type': 'BOOLEAN'}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'gender': {'name': 'gender', 'mode': 'REQUIRED', 'type': 'STRING'}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'income': {'name': 'income', 'mode': 'REQUIRED', 'type': 'INTEGER'}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'location': {'name': 'location', 'mode': 'REQUIRED', 'type': 'RECORD', 'fields': {'city': {'name': 'city', 'mode': 'REQUIRED', 'type': 'STRING'}, 'state': {'name': 'state', 'mode': 'REQUIRED', 'type': 'STRING'}}}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}, 'relationship_status': {'name': 'relationship_status', 'mode': 'REQUIRED', 'type': 'STRING'}},
        {'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}, 'height': {'name': 'height', 'mode': 'REQUIRED', 'type': 'FLOAT'}}
    ]

    expected_output = [
        {'age': {'name': 'age', 'mode': 'NULLABLE', 'type': 'INTEGER'}, 'gender': {'name': 'gender', 'mode': 'NULLABLE', 'type': 'STRING'}, 'name': {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}, 'income': {'name': 'income', 'mode': 'NULLABLE', 'type': 'INTEGER'}, 'is_student': {'name': 'is_student', 'mode': 'NULLABLE', 'type': 'BOOLEAN'}, 'hobbies': {'name': 'hobbies', 'mode': 'NULLABLE', 'type': 'STRING'}, 'is_employed': {'name': 'is_employed', 'mode': 'NULLABLE', 'type': 'BOOLEAN'}, 'location': {'name': 'location', 'mode': 'NULLABLE', 'type': 'RECORD', 'fields': {'city': {'name': 'city', 'mode': 'REQUIRED', 'type': 'STRING'}, 'state': {'name': 'state', 'mode': 'REQUIRED', 'type': 'STRING'}}}, 'relationship_status': {'name': 'relationship_status', 'mode': 'NULLABLE', 'type': 'STRING'}, 'height': {'name': 'height', 'mode': 'NULLABLE', 'type': 'FLOAT'}},
    ]

    schema_combine = SchemaCombine()
    result = (
        test_pipeline
        | Create(input_schemas)
        | CombineGlobally(schema_combine)
    )

    assert_that(result, equal_to(expected_output))


def test_all_elements_are_same():
    assert all_elements_are_same([1, 1, 1]) == True
    assert all_elements_are_same([1, 2, 1]) == False
    assert all_elements_are_same([]) == True


def test_get_big_query_schema():
    assert get_big_query_schema("field1", "hello") == {"name": "field1", "type": "STRING", "mode": "REQUIRED"}
    assert get_big_query_schema("field2", 42) == {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"}
    assert get_big_query_schema("field3", 3.14) == {"name": "field3", "type": "FLOAT", "mode": "REQUIRED"}
    assert get_big_query_schema("field4", True) == {"name": "field4", "type": "BOOLEAN", "mode": "REQUIRED"}

    with pytest.raises(UnidentifiedTypeException):
        get_big_query_schema("field5", None)

    with pytest.raises(HeterogeneousListException):
        get_big_query_schema("field6", [1, "hello"])

    assert get_big_query_schema("field7", [{"a": 1}]) == {
        "name": "field7",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": {"a": {"name": "a", "type": "INTEGER", "mode": "REQUIRED"}}
    }

    assert get_big_query_schema("field8", {"a": 1}) == {
        "name": "field8",
        "type": "RECORD",
        "fields": {"a": {"name": "a", "type": "INTEGER", "mode": "REQUIRED"}}
    }


def test_generate_schema():
    assert generate_schema({"field1": "hello", "field2": 42}) == {
        "field1": {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
        "field2": {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
    }


def test_create_accumulator(test_pipeline):
    schema_combine = SchemaCombine()
    assert schema_combine.create_accumulator() == {}


def test_add_input(test_pipeline):
    schema_combine = SchemaCombine()
    acc = schema_combine.create_accumulator()
    input_schema = {
        "field1": {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
        "field2": {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
    }
    acc = schema_combine.add_input(acc, input_schema)
    assert acc == input_schema


def test_merge_accumulators(test_pipeline):
    schema_combine = SchemaCombine()
    acc1 = {
        "field1": {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
        "field2": {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
    }
    acc2 = {
        "field1": {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
        "field2": {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
        "field3": {"name": "field3", "type": "BOOLEAN", "mode": "REQUIRED"},
    }