import unittest
from unittest.mock import patch, MagicMock
from dynamodb_manager.manager import DynamoDBManager

class TestDynamoDBManager(unittest.TestCase):

    @patch('boto3.resource')
    def setUp(self, mock_boto3_resource):
        self.mock_dynamodb = MagicMock()
        self.mock_dynamodb_table = MagicMock()
        self.mock_dynamodb.Table.return_value = self.mock_dynamodb_table
        mock_boto3_resource.return_value = self.mock_dynamodb
        self.dynamodb_manager = DynamoDBManager('test_table_name')

    def test_set_timestamp(self):
        try:
            self.dynamodb_manager.set_timestamp('2024-02-02T00:00:00')
            # Verify that the item is added to the table
            self.mock_dynamodb_table.put_item.assert_called_once_with(
                Item={'config_key': 'timestamp', 'timestamp': '2024-02-02T00:00:00'}
            )
        except Exception as e:
            self.fail(f"Unexpected exception: {e}")

    def test_get_timestamp(self):
        # Mock DynamoDB response when the item is found
        self.mock_dynamodb_table.get_item.return_value = {'Item': {'config_key': 'timestamp', 'timestamp': '2022-01-01T00:00:00'}}

        try:
            timestamp = self.dynamodb_manager.get_timestamp()
            self.assertEqual(timestamp, '2022-01-01T00:00:00')
        except Exception as e:
            self.fail(f"Unexpected exception: {e}")

