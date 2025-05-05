from boto3 import client as boto3_client

from boto3_helpers.pagination import yield_all_items


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
        result_env = update_environment_variables('test-function', new_env)
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


def publish_version_limited(function_name, keep_count, lambda_client=None, **kwargs):
    """Publish a new version of a Lambda function. Afterward, delete older versions
    such that only a limited number are left.

    * *function_name* is the Lambda function name.
    * *keep_count* is the number of versions to leave after creating the new version.
    * *lambda_client* is a ``boto3.client('lambda')`` instance. If not given, one will
      be created with ``boto3.client('lambda')``.
    * *kwargs* are passed to the ``publish_version`` call.

    Usage:

    .. code-block:: python

        from boto3_helpers.awslambda import publish_version_limited

        version_resp = publish_version_limited('test-function', 4)

    Deleting function versions is a destructive operation, so take care.

    .. note::

        It's possible for another API user to publish or remove function versions
        in between this function's calls to the Lambda API. Be aware that
        the operations aren't atomic.
    """
    lambda_client = lambda_client or boto3_client('lambda')

    # Publish the new version
    kwargs['FunctionName'] = function_name
    publish_resp = lambda_client.publish_version(**kwargs)

    # Get the current list of versions
    all_versions = sorted(
        yield_all_items(
            lambda_client,
            'list_versions_by_function',
            'Versions',
            FunctionName=function_name,
        ),
        key=lambda x: int(x['Version']),
    )

    # If there are not too many versions, bail out.
    if len(all_versions) <= keep_count:
        return publish_resp

    # Remove all but the requested number of versions.
    for version_info in all_versions[:-keep_count]:
        lambda_client.delete_function(
            FunctionName=function_name, Qualifier=version_info['Version']
        )

    return publish_resp
