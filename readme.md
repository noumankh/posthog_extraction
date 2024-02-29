# Posthog Data Extractor

This repository contains scripts to extract events from the Posthog API, manage timestamps in DynamoDB, and send the data to Amazon Kinesis Firehose.

## Files

### 1. `dynamodb_manager/manager.py`

This module includes a `DynamoDBManager` class responsible for managing timestamps in DynamoDB. The class provides methods to get and set timestamps.

### 2. `kinesis_sender/sender.py`

The `FirehoseSender` class in this module is used to send data to Amazon Kinesis Firehose. It includes a method `send_to_firehose` that sends records to a specified Firehose delivery stream.

### 3. `posthog_extractor/extractor.py`

The `PosthogEventsExtractor` class is the main script for extracting events from the Posthog API. It utilizes the `DynamoDBManager` and `FirehoseSender` classes to manage timestamps and send data to Firehose.

### 4. `lambda_handler.py`

This script serves as a Lambda handler. It can be used in an AWS Lambda function to automate the extraction and streaming process. Ensure that Lambda has the necessary permissions to interact with DynamoDB and Kinesis Firehose.

## Usage

1. Replace the placeholder values in the `__main__` section of each script with your actual values.

2. Run the `posthog_extractor/extractor.py` script to extract events from the Posthog API, manage timestamps in DynamoDB, and send data to Kinesis Firehose.

```bash
python posthog_extractor/extractor.py
```

## Using Docker

You can use Docker to containerize your application. A `Dockerfile` is included in the repository. To build and run the container:

```bash
docker build -t posthog-extractor .
docker run posthog-extractor
```

Make sure to set the required environment variables in the Dockerfile or through other means.

## Using Poetry

This project uses Poetry for dependency management. To build the project using Poetry:

```bash
poetry install
poetry run python posthog_extractor/extractor.py
```

Make sure to create and activate a virtual environment using Poetry.

## Using in Lambda Function

1. Create a new Lambda function in the AWS Management Console.

2. Upload a ZIP file containing the entire project (excluding virtual environment files and unnecessary files).

3. Set the handler to `lambda_handler.lambda_handler`.

4. Configure environment variables in the Lambda function settings with your actual values.

5. Ensure that the Lambda function has the necessary permissions to interact with DynamoDB and Kinesis Firehose.

## Dependencies

- `requests` (for making HTTP requests)
- `boto3` (for interacting with AWS services)

Install these dependencies using:

```bash
pip install requests boto3
```
