from decimal import Decimal
from unittest import TestCase
from unittest.mock import call as MockCall, patch

from boto3.dynamodb.conditions import Attr as ddb_attr, Key as ddb_key
from boto3 import resource as boto3_resource
from botocore.stub import Stubber

from boto3_helpers.dynamodb import (
    batch_yield_items,
    fix_numbers,
    load_dynamodb_json,
    query_table,
    scan_table,
    update_attributes,
)

SCAN_RESPONSE = """\
{
    "Items": [
        {
            "bin_set": {
                "BS": [
                    "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"
                ]
            },
            "string_set": {
                "SS": [
                    "ss_1",
                    "ss_2"
                ]
            },
            "number_int": {
                "N": "1"
            },
            "number_set": {
                "NS": [
                    "1.1",
                    "1"
                ]
            },
            "string_literal": {
                "S": "s"
            },
            "list_value": {
                "L": [
                    {
                        "S": "sl_1"
                    },
                    {
                        "N": "1"
                    }
                ]
            },
            "bin_value": {
                "B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"
            },
            "bool_value": {
                "BOOL": true
            },
            "null_value": {
                "NULL": true
            },
            "number_float": {
                "N": "1.1"
            },
            "map_value": {
                "M": {
                    "n_key": {
                        "N": "1.1"
                    },
                    "s_key": {
                        "S": "s_value"
                    }
                }
            }
        }
    ],
    "Count": 1,
    "ScannedCount": 1,
    "ConsumedCapacity": null
}
"""


class DynamoDBTests(TestCase):
    def test_query_table(self):
        # Set up the stubber
        ddb_resource = boto3_resource('dynamodb', region_name='not-a-region')
        ddb_table = ddb_resource.Table('test-table')
        stubber = Stubber(ddb_resource.meta.client)

        # This will be a query for a simple key
        query_expr = ddb_key('username').eq('ExampleUser')

        # The first query will return a single page of results
        page_1_resp = {
            'Items': [
                {'username': {'S': 'ExampleUser'}, 'index': {'S': '1'}},
                {'username': {'S': 'ExampleUser'}, 'index': {'S': '2'}},
            ],
            'LastEvaluatedKey': {'username': {'S': 'ExampleUser'}, 'index': {'S': '2'}},
        }
        page_1_query = {
            'TableName': 'test-table',
            'KeyConditionExpression': query_expr,
            'Limit': 2,
        }
        stubber.add_response('query', page_1_resp, page_1_query)

        # The second query will return another page of results; this is the last page
        page_2_resp = {
            'Items': [
                {'username': {'S': 'ExampleUser'}, 'index': {'S': '3'}},
            ]
        }
        page_2_query = {
            'TableName': 'test-table',
            'KeyConditionExpression': query_expr,
            'ExclusiveStartKey': {'username': 'ExampleUser', 'index': '2'},
            'Limit': 2,
        }
        stubber.add_response('query', page_2_resp, page_2_query)

        # Do the deed
        with stubber:
            actual = list(
                query_table(ddb_table, KeyConditionExpression=query_expr, Limit=2)
            )

        expected = [
            {'username': 'ExampleUser', 'index': '1'},
            {'username': 'ExampleUser', 'index': '2'},
            {'username': 'ExampleUser', 'index': '3'},
        ]
        self.assertEqual(actual, expected)

    def test_scan_table(self):
        # Set up the stubber
        ddb_resource = boto3_resource('dynamodb', region_name='not-a-region')
        ddb_table = ddb_resource.Table('test-table')
        stubber = Stubber(ddb_resource.meta.client)

        # This will be a query for a simple key
        filter_expr = ddb_attr('age').lt(27)

        # The first scan will return a single page of results
        page_1_resp = {
            'Items': [
                {'username': {'S': 'ExampleUser'}, 'age': {'N': '26'}},
                {'username': {'S': 'ExampleUser'}, 'age': {'N': '25'}},
            ],
            'LastEvaluatedKey': {'username': {'S': 'ExampleUser'}, 'age': {'S': '25'}},
        }
        page_1_scan = {
            'TableName': 'test-table',
            'FilterExpression': filter_expr,
            'Limit': 2,
        }
        stubber.add_response('scan', page_1_resp, page_1_scan)

        # The second scan will return another page of results; this is the last page
        page_2_resp = {
            'Items': [
                {'username': {'S': 'ExampleUser'}, 'age': {'N': '24'}},
            ]
        }
        page_2_scan = {
            'TableName': 'test-table',
            'FilterExpression': filter_expr,
            'ExclusiveStartKey': {'username': 'ExampleUser', 'age': '25'},
            'Limit': 2,
        }
        stubber.add_response('scan', page_2_resp, page_2_scan)

        # Do the deed
        with stubber:
            actual = list(scan_table(ddb_table, FilterExpression=filter_expr, Limit=2))

        expected = [
            {'username': 'ExampleUser', 'age': Decimal(26)},
            {'username': 'ExampleUser', 'age': Decimal(25)},
            {'username': 'ExampleUser', 'age': Decimal(24)},
        ]
        self.assertEqual(actual, expected)

    @patch('boto3_helpers.dynamodb.boto3_resource', autospec=True)
    def test_update_attributes(self, mock_boto3_resource):
        # Set up the stubber
        ddb_resource = boto3_resource('dynamodb', region_name='not-a-region')
        stubber = Stubber(ddb_resource.meta.client)
        mock_boto3_resource.return_value = ddb_resource

        # The function will create a string for UpdateExpression
        # and a dict for ExpressionAttributeValues
        update_resp = {
            'Attributes': {
                'username': {'S': 'janedoe'},
                'last_name': {'S': 'Doe'},
                'age': {'N': '26'},
                'weight_kg': {'N': '70'},
            }
        }
        update_params = {
            'TableName': 'test-table',
            'Key': {'last_name': 'Doe', 'username': 'janedoe'},
            'UpdateExpression': 'SET age = :val1, weight_kg = :val2',
            'ExpressionAttributeValues': {':val1': 26, ':val2': 70},
            'ReturnValues': 'ALL_NEW',
        }
        stubber.add_response('update_item', update_resp, update_params)

        # Do the deed
        with stubber:
            actual = update_attributes(
                'test-table',
                {'username': 'janedoe', 'last_name': 'Doe'},
                {'age': 26, 'weight_kg': 70},
                ReturnValues='ALL_NEW',
            )
        expected = {
            'Attributes': {
                'username': 'janedoe',
                'last_name': 'Doe',
                'age': Decimal(26),
                'weight_kg': Decimal(70),
            }
        }
        self.assertEqual(actual, expected)

    @patch('boto3_helpers.dynamodb.sleep', autospec=True)
    @patch('boto3_helpers.dynamodb.boto3_resource', autospec=True)
    def test_batch_yield_items(self, mock_boto3_resource, mock_sleep):
        table_name = 'test-table'
        all_keys = [
            {'primary_key': '1', 'sort_key': 'a'},
            {'primary_key': '1', 'sort_key': 'b'},
            {'primary_key': '2', 'sort_key': 'a'},
            {'primary_key': '2', 'sort_key': 'b'},
            {'primary_key': '3', 'sort_key': 'a'},
            {'primary_key': '3', 'sort_key': 'b'},
        ]

        mock_boto3_resource.return_value.batch_get_item.side_effect = [
            {
                'Responses': {table_name: all_keys[:3]},
                'UnprocessedKeys': {table_name: all_keys[3:]},
            },
            {
                'Responses': {table_name: all_keys[3:]},
                'UnprocessedKeys': {},
            },
        ]
        actual = list(batch_yield_items(table_name, all_keys[:], backoff_base=0.1))
        self.assertEqual(actual, all_keys)

        mock_sleep.assert_called_once_with(0.1)
        mock_boto3_resource.assert_called_once_with('dynamodb')
        self.assertEqual(mock_boto3_resource.return_value.batch_get_item.call_count, 2)

    @patch('boto3_helpers.dynamodb.sleep', autospec=True)
    @patch('boto3_helpers.dynamodb.boto3_resource', autospec=True)
    def test_batch_yield_items_batch_size(self, mock_boto3_resource, mock_sleep):
        table_name = 'test-table'
        all_keys = [
            {'primary_key': '1', 'sort_key': 'a'},
            {'primary_key': '1', 'sort_key': 'b'},
            {'primary_key': '2', 'sort_key': 'a'},
            {'primary_key': '2', 'sort_key': 'b'},
            {'primary_key': '3', 'sort_key': 'a'},
            {'primary_key': '3', 'sort_key': 'b'},
            {'primary_key': '4', 'sort_key': 'a'},
            {'primary_key': '4', 'sort_key': 'b'},
        ]

        mock_boto3_resource.return_value.batch_get_item.side_effect = [
            {
                'Responses': {table_name: all_keys[:2]},
                'UnprocessedKeys': {},
            },
            {
                'Responses': {table_name: all_keys[2:4]},
                'UnprocessedKeys': {},
            },
            {
                'Responses': {table_name: all_keys[4:6]},
                'UnprocessedKeys': {},
            },
            {
                'Responses': {table_name: all_keys[6:8]},
                'UnprocessedKeys': {},
            },
        ]
        actual = list(
            batch_yield_items(
                table_name, all_keys[:], batch_size=2, backoff_base=0.1, backoff_max=0.2
            )
        )
        self.assertEqual(actual, all_keys)

        self.assertEqual(
            mock_sleep.mock_calls, [MockCall(0.1), MockCall(0.2), MockCall(0.2)]
        )
        mock_boto3_resource.assert_called_once_with('dynamodb')
        self.assertEqual(mock_boto3_resource.return_value.batch_get_item.call_count, 4)

    def test_fix_numbers(self):
        # Set up the stubber
        ddb_resource = boto3_resource('dynamodb', region_name='not-a-region')
        ddb_table = ddb_resource.Table('test-table')
        stubber = Stubber(ddb_resource.meta.client)

        # The first query will return a single page of results
        resp = {
            'Item': {
                'string_set': {'SS': ['ss_1', 'ss_2']},
                'number_int': {'N': '1'},
                'number_set': {'NS': ['1.1', '1']},
                'TestKey': {'S': 'test-key'},
                'list_value': {'L': [{'S': 'sl_1'}, {'N': '1'}]},
                'bool_value': {'BOOL': True},
                'null_value': {'NULL': True},
                'number_float': {'N': '1.1'},
                'map_value': {'M': {'n_key': {'N': '1.1'}, 's_key': {'S': 's_value'}}},
            }
        }
        params = {'TableName': 'test-table', 'Key': {'TestKey': 'test-key'}}
        stubber.add_response('get_item', resp, params)

        # Do the deed
        with stubber:
            item = ddb_table.get_item(Key={'TestKey': 'test-key'})['Item']
            actual = fix_numbers(item)

        expected = {
            'string_set': {'ss_1', 'ss_2'},
            'number_int': 1,
            'number_set': {1, 1.1},
            'TestKey': 'test-key',
            'list_value': ['sl_1', 1],
            'bool_value': True,
            'null_value': None,
            'number_float': 1.1,
            'map_value': {'n_key': 1.1, 's_key': 's_value'},
        }
        self.assertEqual(actual, expected)

    def test_load_dynamodb_json_scan(self):
        actual = load_dynamodb_json(SCAN_RESPONSE)
        expected = {
            'Items': [
                {
                    'bin_set': {b'this text is base64-encoded'},
                    'string_set': {'ss_1', 'ss_2'},
                    'number_int': 1,
                    'number_set': {1.1, 1},
                    'string_literal': 's',
                    'list_value': ['sl_1', 1],
                    'bin_value': b'this text is base64-encoded',
                    'bool_value': True,
                    'null_value': None,
                    'number_float': 1.1,
                    'map_value': {'n_key': 1.1, 's_key': 's_value'},
                }
            ],
            'Count': 1,
            'ScannedCount': 1,
            'ConsumedCapacity': None,
        }
        self.assertEqual(actual, expected)

    def test_load_dynamodb_json_get(self):
        i = 0
        for text, use_decimal, expected in (
            (
                '{"Item": {"some_number": {"N": "100"}}}',
                False,
                {'Item': {'some_number': 100}},
            ),
            (
                '{"Item": {"some_number": {"N": "100.1"}}}',
                False,
                {'Item': {'some_number': 100.1}},
            ),
            (
                '{"Item": {"some_number": {"N": "100.1"}}}',
                True,
                {'Item': {'some_number': Decimal('100.1')}},
            ),
        ):
            i += 1
            with self.subTest(i=i):
                actual = load_dynamodb_json(text, use_decimal=use_decimal)
                self.assertEqual(actual, expected)
