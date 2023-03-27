import pytest

from libs.exceptions import IncompatibleSchemasException
from libs.schema_merger import SchemaMerger


def test_merge_schemas_different_types():
    schema1 = {"name": "field1", "mode": "REQUIRED", "type": "INTEGER"}
    schema2 = {"name": "field1", "mode": "REQUIRED", "type": "STRING"}
    schema_combine = SchemaMerger(schema1, schema2)
    with pytest.raises(IncompatibleSchemasException):
        schema_combine._merge_schemas(schema1, schema2)

    with pytest.raises(IncompatibleSchemasException):
        schema_combine._merge_schemas(schema1, schema2)

def test_merge_schemas_one_repeated():
    schema1 = {"name": "field1", "mode": "REQUIRED", "type": "INTEGER"}
    schema2 = {"name": "field1", "mode": "REPEATED", "type": "INTEGER"}
    schema_combine = SchemaMerger(schema1, schema2)
    with pytest.raises(IncompatibleSchemasException):
        schema_combine._merge_schemas(schema1, schema2)
    
    with pytest.raises(IncompatibleSchemasException):
        schema_combine._merge_schemas(schema2, schema1)

def test_merge_schemas_both_repeated():
    schema1 = {"name": "field1", "mode": "REPEATED", "type": "INTEGER"}
    schema2 = {"name": "field1", "mode": "REPEATED", "type": "INTEGER"}
    schema_combine = SchemaMerger(schema1, schema2)
    assert schema_combine._merge_schemas(schema1, schema2) == schema1

def test_merge_schemas_both_required():
    schema1 = {"name": "field1", "mode": "REQUIRED", "type": "INTEGER"}
    schema2 = {"name": "field1", "mode": "REQUIRED", "type": "INTEGER"}
    schema_combine = SchemaMerger(schema1, schema2)
    assert schema_combine._merge_schemas(schema1, schema2) == schema1

def test_merge_schemas_one_required():
    schema1 = {"name": "field1", "mode": "REQUIRED", "type": "INTEGER"}
    schema2 = {"name": "field1", "mode": "NULLABLE", "type": "INTEGER"}
    schema_combine = SchemaMerger(schema1, schema2)
    assert schema_combine._merge_schemas(schema1, schema2) == schema2
    assert schema_combine._merge_schemas(schema2, schema1) == schema2

def test_merge_schemas_both_nullable():
    schema1 = {"name": "field1", "mode": "NULLABLE", "type": "INTEGER"}
    schema2 = {"name": "field1", "mode": "NULLABLE", "type": "INTEGER"}
    schema_combine = SchemaMerger(schema1, schema2)
    assert schema_combine._merge_schemas(schema1, schema2) == schema1

def test_merge_schemas_records_both_required():
    schema1 = {"name": "field1", "mode": "REQUIRED", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "REQUIRED", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}}}
    schema2 = {"name": "field1", "mode": "REQUIRED", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}
    expect1 = {"name": "field1", "mode": "REQUIRED", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "NULLABLE", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "NULLABLE", "type": "INTEGER"}}}
    schema_combine = SchemaMerger(schema1, schema2)
    assert schema_combine._merge_schemas(schema1, schema2) == expect1

def test_merge_schemas_records_one_nullable():
    schema1 = {"name": "field1", "mode": "REQUIRED", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "REQUIRED", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}}}
    schema2 = {"name": "field1", "mode": "NULLABLE", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}
    expect1 = {"name": "field1", "mode": "NULLABLE", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "NULLABLE", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "NULLABLE", "type": "INTEGER"}}}
    copy_of_schema2 = {"name": "field1", "mode": "NULLABLE", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}
    schema_combine = SchemaMerger(schema1, schema2)
    assert schema_combine._merge_schemas(schema1, schema2) == expect1
    assert schema_combine._merge_schemas(copy_of_schema2, schema1) == expect1

def test_merge_record_schemas():
    schema1 = {"fieldN": {"name": "fieldN", "mode": "REQUIRED", "type": "INTEGER"}, "field1": {"name": "field1", "mode": "REQUIRED", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "REQUIRED", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}}}}
    schema2 = {"field1": {"name": "field1", "mode": "NULLABLE", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}}
    expect1 = {"fieldN": {"name": "fieldN", "mode": "NULLABLE", "type": "INTEGER"}, "field1": {"name": "field1", "mode": "NULLABLE", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "NULLABLE", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "NULLABLE", "type": "INTEGER"}}}}
    copy_of_schema2 = {"field1": {"name": "field1", "mode": "NULLABLE", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}}
    schema_combine = SchemaMerger(schema1, schema2)
    assert schema_combine.merge() == expect1
    schema_combine = SchemaMerger(copy_of_schema2, schema1)
    assert schema_combine.merge() == expect1


def test_merge_record_schemas_one_repeated():
    schema1 = {"fieldN": {"name": "fieldN", "mode": "REQUIRED", "type": "INTEGER"}, "field1": {"name": "field1", "mode": "NULLABLE", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "REQUIRED", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}}}}
    schema2 = {"field1": {"name": "field1", "mode": "REPEATED", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}}
    copy_of_schema2 = {"field1": {"name": "field1", "mode": "REPEATED", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}}
    schema_combine = SchemaMerger(schema1, schema2)
    with pytest.raises(IncompatibleSchemasException):
        schema_combine.merge()
    
    schema_combine = SchemaMerger(copy_of_schema2, schema1)
    with pytest.raises(IncompatibleSchemasException):
        schema_combine.merge()

def test_merge_record_schemas_repeated():
    schema1 = {"fieldN": {"name": "fieldN", "mode": "REQUIRED", "type": "INTEGER"}, "field1": {"name": "field1", "mode": "REPEATED", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "REQUIRED", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}}}}
    schema2 = {"field1": {"name": "field1", "mode": "REPEATED", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}}
    expect1 = {"fieldN": {"name": "fieldN", "mode": "NULLABLE", "type": "INTEGER"}, "field1": {"name": "field1", "mode": "REPEATED", "type": "RECORD", "fields": {"field2": {"name": "field2", "mode": "NULLABLE", "type": "INTEGER"}, "field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "NULLABLE", "type": "INTEGER"}}}}
    copy_of_schema2 = {"field1": {"name": "field1", "mode": "REPEATED", "type": "RECORD", "fields": {"field3": {"name": "field3", "mode": "REQUIRED", "type": "INTEGER"}, "field4": {"name": "field4", "mode": "REQUIRED", "type": "INTEGER"}}}}
    schema_combine = SchemaMerger(schema1, schema2)
    assert schema_combine.merge() == expect1
    schema_combine = SchemaMerger(copy_of_schema2, schema1)
    assert schema_combine.merge() == expect1
