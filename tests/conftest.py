import pytest
from apache_beam.testing.test_pipeline import TestPipeline

@pytest.fixture(scope='module')
def pipeline():
    with TestPipeline() as p:
        yield p