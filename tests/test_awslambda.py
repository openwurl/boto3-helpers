from copy import deepcopy
from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.awslambda import (
    delete_old_versions,
    update_environment_variables,
)


class AWSLambdaTests(TestCase):
    def test_update_environment_variables(self):
        # Set up the stubber
        lambda_client = boto3_client('lambda', region_name='not-a-region')
        stubber = Stubber(lambda_client)

        function_name = 'TestFunction'
        current_env = {'LOG_SERVER': '192.0.2.1', 'LOG_PORT': '24601'}
        new_env = {'LOG_SERVER': '198.51.100.1', 'LOG_LEVEL': 'INFO'}
        combined_env = {
            'LOG_SERVER': '198.51.100.1',
            'LOG_PORT': '24601',
            'LOG_LEVEL': 'INFO',
        }

        # First is the get command
        get_params = {'FunctionName': function_name}
        get_resp = {
            'FunctionName': function_name,
            'Environment': {'Variables': current_env},
        }
        stubber.add_response('get_function_configuration', get_resp, get_params)

        # Next is the put command - everything should be the same as above,
        # but the target valus should be updated and the read-only values not present.
        update_params = {
            'FunctionName': function_name,
            'Environment': {'Variables': combined_env},
        }
        update_resp = deepcopy(update_params)
        stubber.add_response(
            'update_function_configuration', update_resp, update_params
        )

        # Do the deed - we expect to get the put response back
        with stubber:
            actual = update_environment_variables(
                function_name,
                new_env,
                lambda_client=lambda_client,
            )

        self.assertEqual(actual, combined_env)

    def test_delete_old_versions(self):
        # Set up the stubber
        lambda_client = boto3_client('lambda', region_name='not-a-region')
        stubber = Stubber(lambda_client)

        function_name = 'TestFunction'

        # First is the call for the first page of list_versions_by_function
        list_params_1 = {'FunctionName': function_name}
        list_resp_1 = {
            'Versions': [{'Version': '1'}, {'Version': '2'}],
            'NextMarker': 'page-2',
        }
        stubber.add_response('list_versions_by_function', list_resp_1, list_params_1)

        # Next is the call for the second page of list_versions_by_function
        list_params_2 = {'FunctionName': function_name, 'Marker': 'page-2'}
        list_resp_2 = {
            'Versions': [{'Version': '3'}, {'Version': '4'}, {'Version': '5'}]
        }
        stubber.add_response('list_versions_by_function', list_resp_2, list_params_2)

        # Next are the calls for the delete_function command. Versions 4 and 5
        # should be kept; the rest should be deleted.
        delete_params_1 = {'FunctionName': function_name, 'Qualifier': '1'}
        delete_resp_1 = {}
        stubber.add_response('delete_function', delete_resp_1, delete_params_1)

        delete_params_2 = {'FunctionName': function_name, 'Qualifier': '2'}
        delete_resp_2 = {}
        stubber.add_response('delete_function', delete_resp_2, delete_params_2)

        delete_params_3 = {'FunctionName': function_name, 'Qualifier': '3'}
        delete_resp_3 = {}
        stubber.add_response('delete_function', delete_resp_3, delete_params_3)

        # Do the deed - we expect to get the publish response back
        with stubber:
            actual = delete_old_versions(function_name, 2, lambda_client=lambda_client)

        self.assertEqual(actual, ['1', '2', '3'])

    def test_delete_old_versions_no_deletes(self):
        # Test that no deletes are made if there are fewer than the limit
        # Set up the stubber
        lambda_client = boto3_client('lambda', region_name='not-a-region')
        stubber = Stubber(lambda_client)

        function_name = 'TestFunction'

        # First is the call for the first page of list_versions_by_function
        list_params_1 = {'FunctionName': function_name}
        list_resp_1 = {'Versions': [{'Version': '4'}, {'Version': '5'}]}
        stubber.add_response('list_versions_by_function', list_resp_1, list_params_1)

        # Do the deed - we expect to get the publish response back
        with stubber:
            actual = delete_old_versions(function_name, 2, lambda_client=lambda_client)

        self.assertEqual(actual, [])
