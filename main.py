from typing import List, Tuple
from argparse import ArgumentParser, Namespace
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pvalue import AsSingleton
from schema_detector.libs.transform_functions import (
    parse_element,
    generate_schema,
    convert_schema_to_table_schema,
)
from schema_detector.libs.schema_combine import SchemaCombine
from schema_detector.libs.update_schema import UpdateSchema


def parse_arguments() -> Tuple[Namespace, List[str]]:
    """
    Parses command line arguments.

    :return: Tuple[Namespace, list] - the parsed arguments and any remaining arguments
    """
    parser = ArgumentParser(description="Load from Json into BigQuery")
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
    parser.add_argument(
        "--projectId",
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
    options = PipelineOptions(pipeline_opts)
    pipeline_opts.append("--project=visualizacion-1559665805251")
    with beam.Pipeline(options=options) as pipeline:
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
        placeholder = (
            schema
            | "Update table Schema"
            >> beam.ParDo(
                UpdateSchema(opts.projectId, opts.outputDataset, opts.outputTable)
            )
        )
        (
            parsed_records
            | "Write to BigQuery"
            >> WriteToBigQuery(
                table=opts.outputTable,
                dataset=opts.outputDataset,
                project=opts.projectId,
                schema=lambda _, schema, ignore: convert_schema_to_table_schema(schema),
                schema_side_inputs=(AsSingleton(schema), AsSingleton(placeholder),),
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location=opts.customGcsTempLocation,
            )
        )


if __name__ == "__main__":
    opts, pipeline_opts = parse_arguments()
    run_pipeline(opts, pipeline_opts)
