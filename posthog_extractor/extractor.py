import requests
import json
from datetime import datetime, timedelta
from dynamodb_manager.manager import DynamoDBManager
from kinesis_sender.sender import FirehoseSender

class PosthogEventsExtractor:
    def __init__(self, project_id, api_key, dynamodb_table, kinesis_stream_name):
        self.project_id = project_id
        self.api_key = api_key
        self.dynamodb_table = dynamodb_table
        self.kinesis_stream_name = kinesis_stream_name
        self.dynamodb_manager = DynamoDBManager(dynamodb_table)
        self.kinesis_sender = FirehoseSender(kinesis_stream_name)
        self.timestamp = self.get_timestamp_from_dynamodb()
        self.events_iterator = iter([])

    def get_timestamp_from_dynamodb(self):
        return self.dynamodb_manager.get_timestamp()

    def set_timestamp_to_dynamodb(self, timestamp):
        self.dynamodb_manager.set_timestamp(timestamp)

    def build_api_url(self, timestamp):
        url = f"https://eu.posthog.com/api/projects/{self.project_id}/query"
        params = {
            "query": {
                "kind": "HogQLQuery",
                "query": f"select * from events where timestamp >= '{timestamp}' order by timestamp"
            }
        }
        return url, params

    def fetch_events(self, url, params=None):
        headers = {"Authorization": f"Bearer {self.api_key}"}

        try:
            response = requests.post(url, headers=headers, json=params)

            if response.status_code == 200:
                return response.json()['results'], response.json().get('next')
            else:
                print(response.content)
                print(f"Error retrieving events: {response.status_code}")
                return [], None

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return [], None
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}")
            return [], None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return [], None

    def __iter__(self):
        return self

    def __next__(self):
        if not self.events_iterator:
            url, params = self.build_api_url(self.timestamp)
            events, next_url = self.fetch_events(url, params)
            self.events_iterator = iter(events)

            if events:
                # Update timestamp in DynamoDB with the timestamp of the last event in the current batch
                last_event_timestamp = events[-1].get('timestamp')
                self.set_timestamp_to_dynamodb(last_event_timestamp)

                # Send data to Kinesis after fetching events
                self.kinesis_sender.send_to_firehose(events)

        try:
            return next(self.events_iterator)
        except StopIteration:
            # No more events, fetch next batch
            if next_url:
                events, next_url = self.fetch_events(next_url)
                self.events_iterator = iter(events)

                if events:
                    # Update timestamp in DynamoDB with the timestamp of the last event in the current batch
                    last_event_timestamp = events[-1].get('timestamp')
                    self.set_timestamp_to_dynamodb(last_event_timestamp)

                    # Send data to Kinesis after fetching events
                    self.kinesis_sender.send_to_firehose(events)

                return next(self.events_iterator)

            raise StopIteration("No more events")

# if __name__ == "__main__":
#     # Replace with your actual values
#     extractor = PosthogEventsExtractor("PROJECT_ID", "API_KEY", "your_dynamodb_table", "your_kinesis_stream")
    
#     # Example: Iterate over events
#     for event in extractor:
#         print(event)
