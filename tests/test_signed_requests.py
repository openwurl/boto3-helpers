from unittest import TestCase
from unittest.mock import MagicMock

from boto3_helpers.signed_requests import SigV4RequestException, sigv4_request


class SigV4RequestTests(TestCase):
    def test_call_succeeds(self):
        _client = MagicMock()
        _client.meta.region_name = 'test-region-1'
        _client._endpoint.http_session.send.return_value = MagicMock(
            status_code=200, content=b'{"NextToken": null, "Schedules": []}'
        )

        service = 'scheduler'
        method = 'POST'
        endpoint = '/schedules?MaxResults=1'
        operation_name = 'ListSchedules'
        actual = sigv4_request(
            service,
            method,
            endpoint,
            client=_client,
            operation_name=operation_name,
            data='{"test": "payload"}',
        )
        expected = {'NextToken': None, 'Schedules': []}
        self.assertEqual(actual, expected)

        sign_call = _client._request_signer.sign.call_args
        self.assertEqual(sign_call[0][0], operation_name)
        self.assertEqual(sign_call[0][1].method, method)
        self.assertEqual(sign_call[0][1].data, '{"test": "payload"}')
        self.assertEqual(
            sign_call[0][1].url,
            'https://scheduler.test-region-1.amazonaws.com/schedules?MaxResults=1',
        )
        self.assertEqual(sign_call[1], {'signing_name': service})

        _client._endpoint.http_session.send.assert_called_once_with(sign_call[0][1])

    def test_call_fails(self):
        _client = MagicMock()
        _client.meta.region_name = 'test-region-1'
        _client._endpoint.http_session.send.return_value = MagicMock(
            status_code=400, content=b'<UnknownOperationException/>\n'
        )

        with self.assertRaises(SigV4RequestException) as cm:
            sigv4_request('scheduler', 'GET', '/yolo', client=_client)

        self.assertEqual(cm.exception.status_code, 400)
        self.assertEqual(cm.exception.content, b'<UnknownOperationException/>\n')
