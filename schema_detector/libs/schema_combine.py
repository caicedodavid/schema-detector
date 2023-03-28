from typing import List
from apache_beam import CombineFn

from schema_detector.libs.schema_merger import SchemaMerger
from schema_detector.libs.transform_functions import get_big_query_schema


class SchemaCombine(CombineFn):
    """
    An implementation of a CombineFn class that combines dictionaries into one.

    Args:
        beam (beam.CombineFn): The Beam CombineFn object.

    Returns:
        dict: A combined dictionary.
    """

    def create_accumulator(self) -> dict:
        """
        Creates an accumulator for combining dictionaries.

        :return: dict - an empty dictionary
        """
        return {}

    def add_input(self, accumulator_schema: dict, input_schema: dict) -> dict:
        """
        Adds an input dictionary to the result dictionary.

        :param accumulator_dict: dict - the result dictionary
        :param input_dict: dict - the input dictionary
        :return: dict - the combined result dictionary
        """
        if not accumulator_schema:
            return input_schema

        merger = SchemaMerger(accumulator_schema, input_schema)
        return merger.merge()

    def merge_accumulators(self, accumulators: List[dict]) -> dict:
        """
        Merges multiple accumulators into one.

        :param accumulators: List[dict] - a list of accumulators
        :return: dict - the merged accumulator
        """
        result = accumulators[0]
        for input_schema in accumulators[1:]:
            merger = SchemaMerger(result, input_schema)
            result = merger.merge()

        return result

    def extract_output(self, result_dict: dict) -> dict:
        """
        Extracts the output from the result dictionary.

        :param result_dict: dict - the result dictionary
        :return: dict - the output dictionary
        """
        return get_big_query_schema(result_dict)
