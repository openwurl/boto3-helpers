from decimal import Decimal
from unittest import TestCase

from boto3.dynamodb.conditions import Attr as ddb_attr, Key as ddb_key
from boto3 import resource as boto3_resource
from botocore.stub import Stubber

from boto3_helpers.dynamodb import query_table, scan_table, update_attributes


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

    def test_update_attributes(self):
        # Set up the stubber
        ddb_resource = boto3_resource('dynamodb', region_name='not-a-region')
        ddb_table = ddb_resource.Table('test-table')
        stubber = Stubber(ddb_resource.meta.client)

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
            'UpdateExpression': 'SET age = :val1 weight_kg = :val2',
            'ExpressionAttributeValues': {':val1': 26, ':val2': 70},
            'ReturnValues': 'ALL_NEW',
        }
        stubber.add_response('update_item', update_resp, update_params)

        # Do the deed
        with stubber:
            actual = update_attributes(
                ddb_table,
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
