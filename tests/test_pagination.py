from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.pagination import yield_all_items


class PaginationTests(TestCase):
    def test_medialive_list_input_device_transfers(self):
        # Set up the stubber
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)

        # The first page has a NextToken
        page_1_params = {'TransferType': 'INCOMING'}
        page_1_resp = {
            'InputDeviceTransfers': [
                {
                    'Id': '00000000',
                    'Message': 'Just starting...',
                    'TargetCustomerId': '000000000001',
                    'TransferType': 'INCOMING',
                },
                {
                    'Id': '00000001',
                    'Message': 'In progress...',
                    'TargetCustomerId': '000000000001',
                    'TransferType': 'INCOMING',
                },
            ],
            'NextToken': '00000002',
        }
        stubber.add_response('list_input_device_transfers', page_1_resp, page_1_params)

        # The second page is the last one
        page_2_params = {'TransferType': 'INCOMING', 'NextToken': '00000002'}
        page_2_resp = {
            'InputDeviceTransfers': [
                {
                    'Id': '00000002',
                    'Message': 'Almost done...',
                    'TargetCustomerId': '000000000001',
                    'TransferType': 'INCOMING',
                },
            ],
        }
        stubber.add_response('list_input_device_transfers', page_2_resp, page_2_params)

        # Do the deed - we expect to get everything from both pages
        with stubber:
            actual = list(
                yield_all_items(
                    eml_client,
                    'list_input_device_transfers',
                    'InputDeviceTransfers',
                    TransferType='INCOMING',
                )
            )

        expected = (
            page_1_resp['InputDeviceTransfers'] + page_2_resp['InputDeviceTransfers']
        )
        self.assertEqual(actual, expected)
