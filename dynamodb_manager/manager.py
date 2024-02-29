import boto3
import logging

class DynamoDBManager:
    def __init__(self, table_name, logger=None):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(self.table_name)
        self.logger = logger or logging.getLogger(__name__)

    def get_timestamp(self):
        try:
            response = self.table.get_item(
                Key={'config_key': 'timestamp'}
            )
            item = response.get('Item')
            if item:
                return item.get('timestamp')
        except Exception as e:
            self.logger.error(f"Error retrieving timestamp from DynamoDB: {e}")
        return None

    def set_timestamp(self, timestamp):
        try:
            self.table.put_item(
                Item={
                    'config_key': 'timestamp',
                    'timestamp': timestamp
                }
            )
            self.logger.info(f"Timestamp updated in DynamoDB: {timestamp}")
        except Exception as e:
            self.logger.error(f"Error setting timestamp in DynamoDB: {e}")

# if __name__ == "__main__":
#     # Example usage with logger
#     logging.basicConfig(level=logging.INFO)
#     logger = logging.getLogger(__name__)

#     dynamodb_manager = DynamoDBManager("your_dynamodb_table", logger)

#     # Example: Set timestamp
#     new_timestamp = "2024-03-01T00:00:00"
#     dynamodb_manager.set_timestamp(new_timestamp)
