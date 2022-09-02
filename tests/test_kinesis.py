from datetime import datetime, timezone
from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.kinesis import yield_all_shards


class KinesisTests(TestCase):
    def test_yield_all_shards(self):
        # Set up the stubber
        kinesis_client = boto3_client('kinesis', region_name='not-a-region')
        stubber = Stubber(kinesis_client)

        page_1_params = {
            'StreamName': 'example-stream',
            'ExclusiveStartShardId': 'shard-0001',
            'StreamCreationTimestamp': datetime(
                2022, 9, 2, 0, 0, 0, tzinfo=timezone.utc
            ),
            'MaxResults': 2,
        }
        page_1_resp = {
            'Shards': [
                {
                    'ShardId': 'shard-0002',
                    'HashKeyRange': {
                        'StartingHashKey': '200000000',
                        'EndingHashKey': '29999999',
                    },
                    'SequenceNumberRange': {
                        'StartingSequenceNumber': '200000000',
                        'EndingSequenceNumber': '29999999',
                    },
                },
                {
                    'ShardId': 'shard-0003',
                    'HashKeyRange': {
                        'StartingHashKey': '300000000',
                        'EndingHashKey': '39999999',
                    },
                    'SequenceNumberRange': {
                        'StartingSequenceNumber': '300000000',
                        'EndingSequenceNumber': '39999999',
                    },
                },
            ],
            'NextToken': 'example-token',
        }
        stubber.add_response('list_shards', page_1_resp, page_1_params)

        page_2_params = {
            'NextToken': 'example-token',
            'MaxResults': 2,
        }
        page_2_resp = {
            'Shards': [
                {
                    'ShardId': 'shard-0004',
                    'HashKeyRange': {
                        'StartingHashKey': '400000000',
                        'EndingHashKey': '49999999',
                    },
                    'SequenceNumberRange': {
                        'StartingSequenceNumber': '400000000',
                        'EndingSequenceNumber': '49999999',
                    },
                },
            ],
        }
        stubber.add_response('list_shards', page_2_resp, page_2_params)

        # Do the deed
        with stubber:
            actual = list(
                yield_all_shards(kinesis_client=kinesis_client, **page_1_params)
            )

        expected = page_1_resp['Shards'] + page_2_resp['Shards']
        self.assertEqual(actual, expected)
