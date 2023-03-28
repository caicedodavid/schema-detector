import pytest

from schema_detector.libs.exceptions import HeterogeneousListException, UnidentifiedTypeException
from schema_detector.libs.transform_functions import (
    parse_element, is_elements_of_same_type, get_big_query_types, generate_schema,
    get_big_query_schema, convert_schema_to_table_schema
)
from apache_beam.io.gcp.internal.clients.bigquery.bigquery_v2_messages import (
    TableSchema
)


@pytest.mark.parametrize("element, expected", [
    ('{"name": "John"}', {"name": "John"}),
    ('{"age": 25}', {'age': 25}),
])
def test_parse_element(element, expected):
    assert parse_element(element) == expected

def test_generate_schema():
    #TODO Maybe consider the cases where one records comes with an empty list
    # or empty object since right now i'm just discarding
    record = {"field1": "hello", "field2": 42, "field3": [], "field4": None, "field6": {}}
    assert generate_schema(record) == {
        "field1": {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
        "field2": {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
    }

def test_get_big_query_types_string():
    result = get_big_query_types("name", "John")
    assert result == {"name": "name", "mode": "REQUIRED", "type": "STRING"}

def test_get_big_query_types_integer():
    result = get_big_query_types("age", 30)
    assert result == {"name": "age", "mode": "REQUIRED", "type": "INTEGER"}

def test_get_big_query_types_boolean():
    result = get_big_query_types("active", True)
    assert result == {"name": "active", "mode": "REQUIRED", "type": "BOOLEAN"}

def test_get_big_query_types_float():
    result = get_big_query_types("score", 4.5)
    assert result == {"name": "score", "mode": "REQUIRED", "type": "FLOAT"}

def test_get_big_query_types_list():
    result = get_big_query_types("tags", ["tag1", "tag2"])
    assert result == {"name": "tags", "mode": "REPEATED", "type": "STRING"}

def test_get_big_query_types_empty_list():
    result = get_big_query_types("empty_list", [])
    assert result is None

def test_get_big_query_types_heterogeneous_list():
    with pytest.raises(HeterogeneousListException):
        get_big_query_types("mixed_tags", ["tag1", 2])

def test_get_big_query_types_empty_dict():
    result = get_big_query_types("empty_dict", {})
    assert result is None

def test_get_big_query_types_dict():
    result = get_big_query_types("address", {"street": "Main St", "city": "New York"})
    expected = {
        "name": "address",
        "mode": "REQUIRED",
        "type": "RECORD",
        "fields": {
            "street": {"name": "street", "mode": "REQUIRED", "type": "STRING"},
            "city": {"name": "city", "mode": "REQUIRED", "type": "STRING"},
        },
    }
    assert result == expected

def test_get_big_query_types_dict_and_list():
    result = get_big_query_types("address", {
        "street": "Main St",
        "city": "New York",
        "contact": [
            {"phones": [{"number": "911"}, {"number": "811"}]}, 
            {"media_account": "https://my-media.com"}, 
            {"phones": [{"ext": 9}]}
        ]
    })
    expected = {"name": "address", "mode": "REQUIRED", "type": "RECORD", "fields": {
        "street": {"name": "street", "mode": "REQUIRED", "type": "STRING"},
        "city": {"name": "city", "mode": "REQUIRED", "type": "STRING"},
        "contact": {"name": "contact", "mode": "REPEATED", "type": "RECORD", "fields": {
            "media_account": {"name": "media_account", "mode": "NULLABLE", "type": "STRING"},
            "phones": {"name": "phones", "mode": "REPEATED", "type": "RECORD", "fields": {
                "number": {"name": "number", "mode": "NULLABLE", "type": "STRING"},
                "ext": {"name": "ext", "mode": "NULLABLE", "type": "INTEGER"},
            }},
        }},
    }}
    assert result == expected

def test_get_big_query_types_unidentified_type():
    class CustomType:
        pass

    custom_value = CustomType()
    with pytest.raises(UnidentifiedTypeException):
        get_big_query_types("custom", custom_value)

def test_is_elements_of_same_type():
    assert is_elements_of_same_type([{"name": "field1", "type": "INTEGER", "mode": "REPEATED"}, {"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}, {"name": "field1", "type": "INTEGER", "mode": "NULLABLE"}]) == True
    assert is_elements_of_same_type([{"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}, {"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}, {"name": "field1", "type": "STRING", "mode": "NULLABLE"}]) == False
    assert is_elements_of_same_type([]) == True

def test_get_big_query_schema():
    schema = {"name": {"name": "address", "mode": "REQUIRED", "type": "RECORD", "fields": {
        "street": {"name": "street", "mode": "REQUIRED", "type": "STRING"},
        "city": {"name": "city", "mode": "REQUIRED", "type": "STRING"},
        "contact": {"name": "contact", "mode": "REPEATED", "type": "RECORD", "fields": {
            "media_account": {"name": "media_account", "mode": "NULLABLE", "type": "STRING"},
            "phones": {"name": "phones", "mode": "REPEATED", "type": "RECORD", "fields": {
                "number": {"name": "number", "mode": "NULLABLE", "type": "STRING"},
                "ext": {"name": "ext", "mode": "NULLABLE", "type": "INTEGER"},
            }},
        }},
    }}}
    expected = [
        {"name": "address", "mode": "REQUIRED", "type": "RECORD", "fields": [
            {"name": "street", "mode": "REQUIRED", "type": "STRING"},
            {"name": "city", "mode": "REQUIRED", "type": "STRING"},
            {"name": "contact", "mode": "REPEATED", "type": "RECORD", "fields": [
                {"name": "media_account", "mode": "NULLABLE", "type": "STRING"},
                {"name": "phones", "mode": "REPEATED", "type": "RECORD", "fields":[
                    {"name": "number", "mode": "NULLABLE", "type": "STRING"},
                    {"name": "ext", "mode": "NULLABLE", "type": "INTEGER"}
                ]}
            ]}
        ]}
    ]
    assert get_big_query_schema(schema) == expected


def test_convert_schema_to_table_schema():
    schema_list = [
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
        {
            "name": "address",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {"name": "street", "type": "STRING", "mode": "REQUIRED"},
                {"name": "city", "type": "STRING", "mode": "REQUIRED"},
            ],
        },
    ]

    result = convert_schema_to_table_schema(schema_list)
    assert isinstance(result, TableSchema)
    assert len(result.fields) == 3

    assert result.fields[0].name == "name"
    assert result.fields[0].type == "STRING"
    assert result.fields[0].mode == "REQUIRED"

    assert result.fields[1].name == "age"
    assert result.fields[1].type == "INTEGER"
    assert result.fields[1].mode == "NULLABLE"

    assert result.fields[2].name == "address"
    assert result.fields[2].type == "RECORD"
    assert result.fields[2].mode == "NULLABLE"
    assert len(result.fields[2].fields) == 2

    assert result.fields[2].fields[0].name == "street"
    assert result.fields[2].fields[0].type == "STRING"
    assert result.fields[2].fields[0].mode == "REQUIRED"

    assert result.fields[2].fields[1].name == "city"
    assert result.fields[2].fields[1].type == "STRING"
    assert result.fields[2].fields[1].mode == "REQUIRED"

