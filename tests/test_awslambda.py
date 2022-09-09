from copy import deepcopy
from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.awslambda import update_environment_variables


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
