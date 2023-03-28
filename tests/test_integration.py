from apache_beam import Map
from apache_beam.transforms import Create, CombineGlobally
from apache_beam.testing.util import assert_that, equal_to

from schema_detector.libs.transform_functions import generate_schema
from schema_detector.libs.schema_combine import SchemaCombine

def test_integrate_generate_schema_with_schema_combine(pipeline):
    records = [
        {"age": 20, "gender": "female", "name": "Alice"},
        {"age": 25, "gender": "male", "income": 50000, "name": "Wookie"},
        {"age": 30, "gender": "male", "is_student": False, "name": "miss"},
        {"gender": "female", "hobbies": ["reading"], "name": "missu"},
        {"gender": "male", "is_employed": True, "name": "missaa"},
        {"gender": "non-binary", "name": "Sam"},
        {"income": 100000, "name": "John"},
        {"location": {"city": "San Francisco", "state": "CA"}, "name": "miu"},
        {"name": "Sarah", "relationship_status": "single"},
        {"name": "David", "height": 6.2}
    ]
    expected = [[{'name': 'age', 'mode': 'NULLABLE', 'type': 'INTEGER'}, {'name': 'gender', 'mode': 'NULLABLE', 'type': 'STRING'}, {'name': 'name', 'mode': 'REQUIRED', 'type': 'STRING'}, {'name': 'income', 'mode': 'NULLABLE', 'type': 'INTEGER'}, {'name': 'is_student', 'mode': 'NULLABLE', 'type': 'BOOLEAN'}, {'name': 'hobbies', 'mode': 'REPEATED', 'type': 'STRING'}, {'name': 'is_employed', 'mode': 'NULLABLE', 'type': 'BOOLEAN'}, {'name': 'location', 'mode': 'NULLABLE', 'type': 'RECORD', 'fields': [{'name': 'city', 'mode': 'REQUIRED', 'type': 'STRING'}, {'name': 'state', 'mode': 'REQUIRED', 'type': 'STRING'}]}, {'name': 'relationship_status', 'mode': 'NULLABLE', 'type': 'STRING'}, {'name': 'height', 'mode': 'NULLABLE', 'type': 'FLOAT'}]]
    result = (
        pipeline
        | Create(records)
        | Map(generate_schema)
        | CombineGlobally(SchemaCombine())
    )

    assert_that(result, equal_to(expected))
