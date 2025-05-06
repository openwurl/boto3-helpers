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


def delete_old_versions(function_name, keep_count, lambda_client=None, **kwargs):
    """Delete older published versions of a Lambda function.

    * *function_name* is the Lambda function name.
    * *keep_count* is the number of versions to keep active.
    * *lambda_client* is a ``boto3.client('lambda')`` instance. If not given, one will
      be created with ``boto3.client('lambda')``.

    Returns a list of the versions that were removed.

    Usage:

    .. code-block:: python

        from boto3_helpers.awslambda import delete_old_versions

        version_resp = delete_old_versions('test-function', 4)

    This is a destructive operation, so take care.

    .. note::

        It's possible for another API user to publish or remove function versions
        in between this function's calls to the Lambda API. Be aware that
        the operations aren't atomic.
    """
    lambda_client = lambda_client or boto3_client('lambda')

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
    ret = []
    if len(all_versions) <= keep_count:
        return ret

    # Remove all but the requested number of versions.
    for version_info in all_versions[:-keep_count]:
        version_id = version_info['Version']
        lambda_client.delete_function(FunctionName=function_name, Qualifier=version_id)
        ret.append(version_id)

    return ret
