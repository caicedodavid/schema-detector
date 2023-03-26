"""
Runs a pipeline to read in all JSONL files from a GCS path and uses the JsonDictParse Map to parse each record as a dictionary.
Utilizes a SchemaCombine global function to yield one final output.
"""
import time
from typing import List, Tuple
from argparse import ArgumentParser, Namespace
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from transforms import parse_element, generate_schema, SchemaCombine

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input')
        parser.add_argument('--output')

def parse_arguments() -> Tuple[Namespace, List[str]]:
    """
    Parses command line arguments.
    
    :return: Tuple[Namespace, list] - the parsed arguments and any remaining arguments
    """
    parser = ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=False, help='Specify Google Cloud region')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--inputPath', required=True, help='Path to events.json')
    parser.add_argument('--outputDataset', required=False, help='Dataset name to write records')
    parser.add_argument('--outputTable', required=False, help='Table name to write records')

    return parser.parse_known_args()



def run_pipeline(opts:Namespace, pipeline_opts: List[str]) -> None:
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
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    #input_path = opts.inputPath
    #output_path = opts.outputPath
    #table_name = opts.tableName

    with beam.Pipeline() as pipeline:
        output = (
            pipeline
            | "Read Files" >> ReadFromText("/Users/d0c0b74/Personal/schema-detector/test.jsonl") #ReadFromText(opts.inputPath + "*.jsonl")
            | "Parse JSON to Dictionary" >> beam.Map(parse_element)
            | "Generate schema for each record" >> beam.Map(generate_schema)
            #| "Combine Dictionaries" >> beam.CombineGlobally(SchemaCombine())
            | "Write Output to File" >> WriteToText("output.json", num_shards=1)
        )


if __name__ == "__main__":
    opts, pipeline_opts = parse_arguments()
    combined_dict_output = run_pipeline(opts, pipeline_opts)
