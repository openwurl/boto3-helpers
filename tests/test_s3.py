from unittest import TestCase
from unittest.mock import MagicMock

from boto3_helpers.s3 import query_object


class QueryObjectTests(TestCase):
    def test_basic(self):
        s3_client = MagicMock()
        s3_client.select_object_content.return_value = {
            'Payload': [
                {'Records': {'Payload': b'{"record": 1}\n{"record": 2}\n{"rec'}},
                {'Records': {'Payload': b'ord": 3}\n{"record": 4}\n{"rec'}},
                {'Records': {'Payload': b'ord": 5}\n'}},
            ],
        }

        bucket = 'TestBucket'
        key = 'TestKey'
        query = 'SELECT * FROM s3object s'
        all_records = query_object(bucket, key, query, 'jsonl.gz', s3_client=s3_client)
        expected = [{'record': n} for n in range(1, 5 + 1)]
        self.assertEqual(list(all_records), expected)

        s3_client.select_object_content.assert_called_once_with(
            Bucket=bucket,
            Key=key,
            Expression=query,
            ExpressionType='SQL',
            InputSerialization={'JSON': {'Type': 'LINES'}, 'CompressionType': 'GZIP'},
            OutputSerialization={'JSON': {}},
        )

    def test_custom(self):
        s3_client = MagicMock()
        s3_client.select_object_content.return_value = {
            'Payload': [
                {'Records': {'Payload': b'{"record": 1}\n{"record": 2}\n{"rec'}},
                {'Stats': {}},
                {'Records': {'Payload': b'ord": 3}\n'}},
                {'Stats': {}},
            ],
        }

        bucket = 'TestBucket'
        key = 'TestKey'
        query = 'SELECT * FROM s3object s'
        input_serialization = {
            'CSV': {'FileHeaderInfo': 'USE', 'FieldDelimeter': '\t'},
            'CompressionType': 'GZIP',
        }
        all_records = query_object(
            bucket,
            key,
            query,
            None,
            InputSerialization=input_serialization,
            s3_client=s3_client,
        )
        expected = [{'record': n} for n in range(1, 3 + 1)]
        self.assertEqual(list(all_records), expected)

        s3_client.select_object_content.assert_called_once_with(
            Bucket=bucket,
            Key=key,
            Expression=query,
            ExpressionType='SQL',
            InputSerialization=input_serialization,
            OutputSerialization={'JSON': {}},
        )
