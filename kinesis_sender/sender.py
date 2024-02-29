import json
import boto3

class FirehoseSender:
    def __init__(self, delivery_stream_name):
        self.delivery_stream_name = delivery_stream_name
        self.firehose_client = boto3.client('firehose')

    def send_to_firehose(self, data):
        try:
            records = [{'Data': json.dumps(record) + '\n'} for record in data]
            response = self.firehose_client.put_record_batch(
                DeliveryStreamName=self.delivery_stream_name,
                Records=records
            )
            print(f"Data sent to Firehose with number of successfully put records: {response['RequestResponses']}")
        except Exception as e:
            print(f"Error sending data to Firehose: {e}")

# if __name__ == "__main__":
#     # Example usage
#     firehose_sender = FirehoseSender("your_firehose_delivery_stream")
#     data = [{"event_type": "example_event", "timestamp": "2024-02-29T12:00:00"}]
#     firehose_sender.send_to_firehose(data)