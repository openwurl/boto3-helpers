from unittest import TestCase
from unittest.mock import MagicMock

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.s3 import head_bucket, query_object


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


class HeadBucketTest(TestCase):
    def test_exists(self):
        mock_s3_client = boto3_client('s3', region_name='not-a-region')
        stubber = Stubber(mock_s3_client)
        params = {'Bucket': 'example'}
        resp = {}
        stubber.add_response('head_bucket', resp, params)

        with stubber:
            actual = head_bucket('example', s3_client=mock_s3_client)

        self.assertEqual(actual, resp)

    def test_not_exists(self):
        mock_s3_client = boto3_client('s3', region_name='not-a-region')
        stubber = Stubber(mock_s3_client)
        params = {'Bucket': 'example'}
        stubber.add_client_error(
            'head_bucket',
            service_error_code='404',
            service_message='The specified bucket does not exist.',
            http_status_code=404,
            expected_params=params,
        )

        with stubber, self.assertRaises(mock_s3_client.exceptions.NoSuchBucket):
            head_bucket('example', s3_client=mock_s3_client)

    def test_other_error(self):
        mock_s3_client = boto3_client('s3', region_name='not-a-region')
        stubber = Stubber(mock_s3_client)
        params = {'Bucket': 'example'}
        stubber.add_client_error(
            'head_bucket',
            service_error_code='403',
            service_message='Some other bad thing happened.',
            http_status_code=403,
            expected_params=params,
        )

        with stubber, self.assertRaises(mock_s3_client.exceptions.ClientError):
            head_bucket('example', s3_client=mock_s3_client)
