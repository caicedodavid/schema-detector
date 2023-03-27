import time
from typing import List, Tuple
from argparse import ArgumentParser, Namespace
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pvalue import AsSingleton
from libs.transform_functions import (
    parse_element,
    generate_schema,
    convert_schema_to_table_schema,
)
from libs.schema_combine import SchemaCombine
from libs.update_schema import UpdateSchema


def parse_arguments() -> Tuple[Namespace, List[str]]:
    """
    Parses command line arguments.

    :return: Tuple[Namespace, list] - the parsed arguments and any remaining arguments
    """
    parser = ArgumentParser(description="Load from Json into BigQuery")
    parser.add_argument("--project", required=True, help="Specify Google Cloud project")
    parser.add_argument("--region", required=False, help="Specify Google Cloud region")
    parser.add_argument("--runner", required=True, help="Specify Apache Beam Runner")
    parser.add_argument("--inputPath", required=True, help="Path to events.json")
    parser.add_argument(
        "--outputDataset", required=True, help="Dataset name to write records"
    )
    parser.add_argument(
        "--outputTable", required=True, help="Table name to write records"
    )
    parser.add_argument(
        "--customGcsTempLocation",
        required=True,
        help="GCS bucket location to write temporary results",
    )

    return parser.parse_known_args()


def run_pipeline(opts: Namespace, pipeline_opts: List[str]) -> None:
    """
    Reads in all JSONL files from a GCS path and uses the JsonDictParse Map to parse each record as a dictionary.
    Utilizes a SchemaCombine global function to combine all dictionaries into one final output.

    :param opts: Namespace - the parsed command line arguments
    :param pipeline_opts: List[str] - the list of pipeline options
    :return: dict - the combined output from all JSONL files as a single dictionary
    """
    opts, pipeline_opts = parse_arguments()
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).job_name = "{0}{1}".format(
        "my-pipeline-", time.time_ns()
    )
    options.view_as(StandardOptions).runner = opts.runner

    with beam.Pipeline() as pipeline:
        parsed_records = (
            pipeline
            | "Read Files" >> ReadFromText(opts.inputPath + "*.jsonl")
            | "Parse JSON to Dictionary" >> beam.Map(parse_element)
        )
        schema = (
            parsed_records
            | "Generate schema for each record" >> beam.Map(generate_schema)
            | "Combine Dictionaries" >> beam.CombineGlobally(SchemaCombine())
        )
        (
            schema
            | "Update table Schema"
            >> beam.ParDo(
                UpdateSchema(opts.project, opts.outputDataset, opts.outputTable)
            )
        )
        (
            parsed_records
            | "Write to BigQuery"
            >> WriteToBigQuery(
                table=opts.outputTable,
                dataset=opts.outputDataset,
                project=opts.project,
                schema=lambda _, schema: convert_schema_to_table_schema(schema),
                schema_side_inputs=(AsSingleton(schema),),
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location=opts.customGcsTempLocation,
            )
        )


if __name__ == "__main__":
    opts, pipeline_opts = parse_arguments()
    combined_dict_output = run_pipeline(opts, pipeline_opts)

