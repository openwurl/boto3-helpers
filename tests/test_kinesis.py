from datetime import datetime, timezone
from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.kinesis import (
    yield_all_shards,
    yield_available_shard_records,
    yield_available_stream_records,
)


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

    def test_yield_available_shard_records(self):
        # Set up the stubber
        kinesis_client = boto3_client('kinesis', region_name='not-a-region')
        stubber = Stubber(kinesis_client)
        stream_name = 'example-stream'
        shard_id = 'shard-0001'
        dt = datetime(2022, 9, 7, 15, 11, 0, tzinfo=timezone.utc)
        iterator_params_1 = {
            'StreamName': stream_name,
            'ShardId': shard_id,
            'ShardIteratorType': 'AT_TIMESTAMP',
            'Timestamp': dt,
        }
        iterator_resp_1 = {'ShardIterator': 'iterator-0001'}
        stubber.add_response('get_shard_iterator', iterator_resp_1, iterator_params_1)

        # The first call to get_records doesn't catch us up to the latest data,
        # so another shard iterator is given.
        records_params_1 = {'ShardIterator': 'iterator-0001'}
        records_resp_1 = {
            'Records': [
                {
                    'PartitionKey': shard_id,
                    'SequenceNumber': '100000001',
                    'Data': b'Record 1',
                },
                {
                    'PartitionKey': shard_id,
                    'SequenceNumber': '100000002',
                    'Data': b'Record 2',
                },
            ],
            'MillisBehindLatest': 1,
            'NextShardIterator': 'iterator-0002',
        }
        stubber.add_response('get_records', records_resp_1, records_params_1)

        # The second call does catch us up, so that's the end.
        records_params_2 = {'ShardIterator': 'iterator-0002'}
        records_resp_2 = {
            'Records': [
                {
                    'PartitionKey': shard_id,
                    'SequenceNumber': '100000003',
                    'Data': b'Record 3',
                },
            ],
            'MillisBehindLatest': 0,
        }
        stubber.add_response('get_records', records_resp_2, records_params_2)

        # Do the deed
        with stubber:
            actual = list(
                yield_available_shard_records(
                    'example-stream',
                    shard_id,
                    ShardIteratorType='AT_TIMESTAMP',
                    Timestamp=dt,
                    kinesis_client=kinesis_client,
                )
            )

        expected = [
            records_resp_1['Records'][0],
            records_resp_1['Records'][1],
            records_resp_2['Records'][0],
        ]
        self.assertEqual(actual, expected)

    def test_yield_available_stream_records(self):
        # Set up the stubber
        kinesis_client = boto3_client('kinesis', region_name='not-a-region')
        stubber = Stubber(kinesis_client)
        stream_name = 'example-stream'
        stream_shards = ['shard-a', 'shard-b', 'shard-c']

        # This stream has two shards
        list_params = {'StreamName': stream_name}
        list_resp = {
            'Shards': [
                {
                    'ShardId': stream_shards[0],
                    'HashKeyRange': {
                        'StartingHashKey': '100000000',
                        'EndingHashKey': '19999999',
                    },
                    'SequenceNumberRange': {
                        'StartingSequenceNumber': '100000000',
                        'EndingSequenceNumber': '19999999',
                    },
                },
                {
                    'ShardId': stream_shards[1],
                    'HashKeyRange': {
                        'StartingHashKey': '200000000',
                        'EndingHashKey': '29999999',
                    },
                    'SequenceNumberRange': {
                        'StartingSequenceNumber': '200000000',
                        'EndingSequenceNumber': '29999999',
                    },
                },
            ],
        }
        stubber.add_response('list_shards', list_resp, list_params)

        # The function will request a shard iterator for the first shard, then
        # get records from it.
        iterator_params_1 = {
            'StreamName': stream_name,
            'ShardId': stream_shards[0],
            'ShardIteratorType': 'TRIM_HORIZON',
        }
        iterator_resp_1 = {'ShardIterator': 'iterator-a-1'}
        stubber.add_response('get_shard_iterator', iterator_resp_1, iterator_params_1)

        records_params_1 = {'ShardIterator': 'iterator-a-1'}
        records_resp_1 = {
            'Records': [
                {
                    'PartitionKey': stream_shards[0],
                    'SequenceNumber': '100000001',
                    'Data': b'Record a1',
                },
                {
                    'PartitionKey': stream_shards[0],
                    'SequenceNumber': '100000002',
                    'Data': b'Record a2',
                },
            ],
            'MillisBehindLatest': 0,
        }
        stubber.add_response('get_records', records_resp_1, records_params_1)

        # Shard iterator and records for the second shard
        iterator_params_2 = {
            'StreamName': stream_name,
            'ShardId': stream_shards[1],
            'ShardIteratorType': 'TRIM_HORIZON',
        }
        iterator_resp_2 = {'ShardIterator': 'iterator-b-1'}
        stubber.add_response('get_shard_iterator', iterator_resp_2, iterator_params_2)

        records_params_2 = {'ShardIterator': 'iterator-b-1'}
        records_resp_2 = {
            'Records': [
                {
                    'PartitionKey': stream_shards[1],
                    'SequenceNumber': '200000001',
                    'Data': b'Record b1',
                },
                {
                    'PartitionKey': stream_shards[1],
                    'SequenceNumber': '200000002',
                    'Data': b'Record b2',
                },
                {
                    'PartitionKey': stream_shards[1],
                    'SequenceNumber': '200000003',
                    'Data': b'Record b3',
                },
            ],
            'MillisBehindLatest': 0,
        }
        stubber.add_response('get_records', records_resp_2, records_params_2)

        # Do the deed. Records from the two shards are interleaved together:
        # A, B, A, B...
        with stubber:
            actual = list(
                yield_available_stream_records(
                    'example-stream', kinesis_client=kinesis_client
                )
            )

        expected = [
            records_resp_1['Records'][0],
            records_resp_2['Records'][0],
            records_resp_1['Records'][1],
            records_resp_2['Records'][1],
            records_resp_2['Records'][2],
        ]
        self.assertEqual(actual, expected)
