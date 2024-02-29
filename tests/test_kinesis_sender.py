import unittest
from unittest.mock import patch, MagicMock
from kinesis_sender.sender import FirehoseSender

class FirehoseSenderTest(unittest.TestCase):

    @patch('boto3.client')
    def test_send_to_firehose(self, mock_client):
        # Create a mock object for the Firehose client
        mock_firehose_client = MagicMock()
        mock_client.return_value = mock_firehose_client

        # Set up expected response from Firehose
        expected_response = {'RequestResponses': [{'RecordId': '12345678-1234-1234-1234-123456789012'}]}
        mock_firehose_client.put_record_batch.return_value = expected_response

        # Create an instance of the FirehoseSender class
        firehose_sender = FirehoseSender("your_firehose_delivery_stream")

        # Sample data to send
        data = [{"event_type": "example_event", "timestamp": "2024-02-29T12:00:00"}]

        # Call the send_to_firehose method and assert the output
        firehose_sender.send_to_firehose(data)
        self.assertEqual(mock_firehose_client.put_record_batch.call_count, 1)
        print(mock_firehose_client.put_record_batch.call_args)

if __name__ == '__main__':
    unittest.main()
