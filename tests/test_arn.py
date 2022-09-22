from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.arn import construct_arn


class ARNTests(TestCase):
    def test_construct_from_no_resource_type(self):
        actual = construct_arn(
            'arn:aws:sqs:not-a-region:000000000000:example-queue',
            service='medialive',
            resource_type='channel',
            resource_separator=':',
            resource_id='24601',
        )
        expected = 'arn:aws:medialive:not-a-region:000000000000:channel:24601'
        self.assertEqual(actual, expected)

    def test_construct_to_no_resource_type(self):
        actual = construct_arn(
            'arn:aws:medialive:not-a-region:000000000000:channel:24601',
            service='sqs',
            resource_type='',
            resource_id='example-queue',
        )
        expected = 'arn:aws:sqs:not-a-region:000000000000:example-queue'
        self.assertEqual(actual, expected)

    def test_bogus(self):
        with self.assertRaises(ValueError):
            construct_arn(
                'arn:aws:medialive:not-a-region:000000000000:channel:24601:bogus',
                resource_id='24602',
            )

    def test_construct_arn_from_sts(self):
        sts_client = boto3_client('sts', region_name='not-a-region')
        stubber = Stubber(sts_client)

        get_resp = {
            'UserId': 'SomeUserID',
            'Account': '000000000000',
            'Arn': (
                'arn:aws:sts::000000000000:assumed-role/'
                'SomeRole/botocore-session-1663795182'
            ),
        }
        stubber.add_response('get_caller_identity', get_resp, {})

        with stubber:
            actual = construct_arn(
                sts_client=sts_client,
                service='medialive',
                resource_type='channel',
                resource_separator=':',
                resource_id='24601',
            )
        expected = 'arn:aws:medialive:not-a-region:000000000000:channel:24601'
        self.assertEqual(actual, expected)
