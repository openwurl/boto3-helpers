from boto3 import client as boto3_client


def update_environment_variables(function_name, new_env, *, lambda_client=None):
    """Do a partial update of a Lambda function's environment variables. Return the
    resulting environment.

    * *function_name* is the Lambda function name.
    * *new_env* is a mapping with the new environment variables.
    * *lambda_client* is a ``boto3.client('lambda')`` instance. If not given, one will
      be created with ``boto3.client('lambda')``.

    Usage:

    .. code-block:: python

        from boto3_helpers.awslambda import update_environment_variables

        new_env = {'LOG_LEVEL': 'INFO', 'LOG_SERVER': '198.51.100.1'}
        result_env = update_environment_variables(
            'ExamplePlaybackConfig',
            AdDecisionServerUrl='https://198.51.100.1:24601/ads/',
        )
        assert result_env['LOG_LEVEL'] == 'INFO'
        assert result_env['LOG_SERVER'] == '198.51.100.1'
        assert result_env['LOG_PORT'] == '24601'  # Or whatever it was before

    The function's existing environment variables will be fetched, merged with the
    *new_env*, and sent to the Lambda API.

    .. note::

        It's possible for another API user to change environment variables
        in between this function's calls to  ``get_function_configuration`` and
        ``update_function_configuration``. The Lambda API doesn't allow for atomic
        updates.
    """
    lambda_client = lambda_client or boto3_client('lambda')

    resp = lambda_client.get_function_configuration(FunctionName=function_name)
    env = resp.get('Environment', {}).get('Variables', {})
    env.update(new_env)

    lambda_client.update_function_configuration(
        FunctionName=function_name, Environment={'Variables': env}
    )
    return env
