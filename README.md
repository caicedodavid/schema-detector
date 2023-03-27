# Schema Detector
Schema Detector is a Google Cloud Dataflow pipeline designed to analyze JSONL files stored in Google Cloud Storage and automatically detect and create a BigQuery schema for the data. This tool simplifies the process of loading data from JSONL files into BigQuery tables.

## Features
- Analyzes JSONL files in Google Cloud Storage
- Automatically generates a BigQuery schema based on the input data
- Creates or Updates BigQuery table using the detected schema
- Compatible with Google Cloud Dataflow and Apache Beam

## Requirements
- Python 3.9 or higher
- Pipenv
- Google Cloud SDK (gcloud)

## Installation
1. Clone the repository:

`git clone https://github.com/caicedodavid/schema-detector.git`

2. Change into the cloned repository directory:

`cd schema-detector`

3. Install the required dependencies:

`pipenv install --dev`

4. Run with direct runner:

`python3 main.py --project your-project --region region --runner direct --inputPath gs://bucket/jsonl_path/ --outputDataset dataset --outputTable table --tempGcsLocation gs://temp_bucket`
