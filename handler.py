import os
from posthog_extractor.extractor import PosthogEventsExtractor
import json
import logging

# Configure logging to send logs to CloudWatch
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    project_id = os.environ.get("PROJECT_ID")
    api_key = os.environ.get("API_KEY")
    dynamodb_table = os.environ.get("DYNAMODB_TABLE")
    kinesis_stream_name = os.environ.get("KINESIS_STREAM_NAME")

    extractor = PosthogEventsExtractor(project_id, api_key, dynamodb_table, kinesis_stream_name)

    try:
        # Example: Iterate over events
        for event_data in extractor:
            # Process event_data as needed
            logger.info(f"Processed event: {event_data}")

        return {
            'statusCode': 200,
            'body': json.dumps('Extraction completed successfully!')
        }
    except Exception as e:
        # Log any exceptions and send them to CloudWatch
        logger.error(f"An error occurred: {e}")
        raise

# # For local testing
# if __name__ == "__main__":
#     # Set environment variables or provide actual values
#     os.environ["PROJECT_ID"] = "YOUR_PROJECT_ID"
#     os.environ["API_KEY"] = "YOUR_API_KEY"
#     os.environ["DYNAMODB_TABLE"] = "YOUR_DYNAMODB_TABLE"
#     os.environ["KINESIS_STREAM_NAME"] = "YOUR_KINESIS_STREAM"

#     lambda_handler({}, {})
