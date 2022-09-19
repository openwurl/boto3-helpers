from unittest import TestCase
from unittest.mock import patch

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.sqs import _get_size, delete_batches, send_batches


class SQSTests(TestCase):
    def test_get_size(self):
        message = {
            'MessageBody': '\U0001F574' * 10,
            'MessageAttributes': {
                'text': {'DataType': 'String', 'StringValue': '\U0001F574' * 2},
                'data': {'DataType': 'Binary', 'BinaryValue': b'\x00' * 10},
            },
        }
        self.assertEqual(_get_size(message), 78)

    def test_send_batches(self):
        # Prepare the arguments
        queue_url = 'https://sqs.test-region-1.amazonaws.com/000000000000/test-queue'
        all_messages = [
            # Batch 1 cuts off because we reach 49 bytes
            {'Id': '0000', 'MessageBody': '1234567890', 'DelaySeconds': 1},
            {'Id': '0001', 'MessageBody': '1234567890'},
            {'Id': '0002', 'MessageBody': '1234567890'},
            {'Id': '0003', 'MessageBody': '1234567890'},
            {'Id': '0004', 'MessageBody': '123456789'},
            # Batch 2 cuts off because we reach 5 messages
            {'Id': '0005', 'MessageBody': '1'},
            {'Id': '0006', 'MessageBody': '1'},
            {'Id': '0007', 'MessageBody': '1'},
            {'Id': '0008', 'MessageBody': '1'},
            {'Id': '0009', 'MessageBody': '1'},
            # Batch 3 is just this one straggler
            {'Id': '0010', 'MessageBody': '1'},
        ]

        # Set up the stubber
        sqs_client = boto3_client('sqs', region_name='not-a-region')
        stubber = Stubber(sqs_client)
        expected = {'Successful': [], 'Failed': []}

        batch_1 = all_messages[0:5]
        send_params_1 = {'QueueUrl': queue_url, 'Entries': batch_1}
        send_resp_1 = {'Successful': [], 'Failed': []}
        for i, message in enumerate(batch_1):
            item = {
                'Id': str(i),
                'MessageId': message['Id'],
                'MD5OfMessageBody': '0' * 32,
            }
            send_resp_1['Successful'].append(item)
            expected['Successful'].append(item)
        stubber.add_response('send_message_batch', send_resp_1, send_params_1)

        batch_2 = all_messages[5:10]
        send_params_2 = {'QueueUrl': queue_url, 'Entries': batch_2}
        send_resp_2 = {'Successful': [], 'Failed': []}
        for i, message in enumerate(batch_2):
            item = {
                'Id': str(i),
                'SenderFault': True,
                'Code': 'Unknown',
                'Message': '?',
            }
            send_resp_2['Failed'].append(item)
            expected['Failed'].append(item)
        stubber.add_response('send_message_batch', send_resp_2, send_params_2)

        batch_3 = all_messages[10:]
        send_params_3 = {'QueueUrl': queue_url, 'Entries': batch_3}
        send_resp_3 = {'Successful': [], 'Failed': []}
        for i, message in enumerate(batch_1):
            item = {
                'Id': str(i),
                'MessageId': message['Id'],
                'MD5OfMessageBody': '0' * 32,
            }
            send_resp_3['Successful'].append(item)
            expected['Successful'].append(item)
        stubber.add_response('send_message_batch', send_resp_3, send_params_3)

        # Do the deed
        with stubber:
            actual = send_batches(
                queue_url,
                all_messages,
                sqs_client=sqs_client,
                message_limit=5,
                size_limit=49,
            )
        self.assertEqual(actual, expected)

    @patch('boto3_helpers.sqs.token_hex', lambda x: '00' * x)
    def test_delete_batches(self):
        # Prepare the arguments
        queue_url = 'https://sqs.test-region-1.amazonaws.com/000000000000/test-queue'
        all_messages = [
            {'ReceiptHandle': 'receipt-1', 'Body': 'message 1'},
            {'ReceiptHandle': 'receipt-2', 'Body': 'message 2'},
            {'ReceiptHandle': 'receipt-3', 'Body': 'message 3'},
            {'ReceiptHandle': 'receipt-4', 'Body': 'message 4'},
            {'ReceiptHandle': 'receipt-5', 'Id': '5'},
            {'ReceiptHandle': 'receipt-6', 'Body': 'message 6'},
        ]

        # Set up the stubber
        sqs_client = boto3_client('sqs', region_name='not-a-region')
        stubber = Stubber(sqs_client)
        expected = {'Successful': [], 'Failed': []}

        batch_1 = all_messages[0:5]
        delete_params_1 = {
            'QueueUrl': queue_url,
            'Entries': [
                {'Id': '00000000-1', 'ReceiptHandle': 'receipt-1'},
                {'Id': '00000000-2', 'ReceiptHandle': 'receipt-2'},
                {'Id': '00000000-3', 'ReceiptHandle': 'receipt-3'},
                {'Id': '00000000-4', 'ReceiptHandle': 'receipt-4'},
                {'Id': '5', 'ReceiptHandle': 'receipt-5'},
            ],
        }
        delete_resp_1 = {'Successful': [], 'Failed': []}
        for i, message in enumerate(batch_1):
            item = {'Id': str(i)}
            delete_resp_1['Successful'].append(item)
            expected['Successful'].append(item)
        stubber.add_response('delete_message_batch', delete_resp_1, delete_params_1)

        batch_2 = all_messages[5:]
        delete_params_2 = {
            'QueueUrl': queue_url,
            'Entries': [{'ReceiptHandle': 'receipt-6', 'Id': '00000000-6'}],
        }
        delete_resp_2 = {'Successful': [], 'Failed': []}
        for i, message in enumerate(batch_2):
            item = {
                'Id': str(i),
                'SenderFault': True,
                'Code': 'Unknown',
                'Message': '?',
            }
            delete_resp_2['Failed'].append(item)
            expected['Failed'].append(item)
        stubber.add_response('delete_message_batch', delete_resp_2, delete_params_2)

        # Do the deed
        with stubber:
            actual = delete_batches(
                queue_url,
                all_messages,
                sqs_client=sqs_client,
                message_limit=5,
            )
        self.assertEqual(actual, expected)
