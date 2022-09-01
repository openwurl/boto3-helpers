from secrets import token_hex

from boto3 import (
    client as boto3_client,
    Session as boto3_session,
)


def assumed_role_session(sts_client=None, session_kwargs=None, **assume_role_kwargs):
    """Return a ``boto3.Session`` object for an assumed role:

    * *sts_client* is a ``boto3.client('sts')`` instance. If not given, one will be
      created with ``boto3.client('sts')``.
    * *session_kwargs* are the keyword arguments you want to pass to the
      ``boto3.Session()`` constructor.
    * *assume_role_kwargs* are the arguments for the ``assume_role`` operation, which
      at least include ``RoleArn``. If ``RoleSessionName`` is not given, a
      randomly-generated one will be used.

    Usage:

    .. code-block:: python

        from boto3_helpers.sts import assumed_role_session

        role_arn = 'arn:aws:iam::000000000000:role/TargetRole'
        session = assumed_role_session(RoleArn=role_arn)

    This is equivalent to:

    .. code-block:: python

        from boto3 import (
            client as boto3_client,
            Session as boto3_session,
        )

        sts_client = boto3_client('sts')
        role_arn = 'arn:aws:iam::000000000000:role/TargetRole'
        session_name = 'AssumedRoleSession1'
        resp = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName=session_name
        )
        credentials = resp['credentials']
        session = boto3_session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
        )
    """
    sts_client = sts_client or boto3_client('sts')
    session_kwargs = session_kwargs or {}

    assume_role_kwargs.setdefault('RoleSessionName', token_hex(4))
    credentials = sts_client.assume_role(**assume_role_kwargs)['Credentials']

    return boto3_session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        **session_kwargs,
    )


def assumed_role_client(
    service_name, *, sts_client=None, client_kwargs=None, **assume_role_kwargs
):
    """Return a ``boto3.client`` object for an assumed role:

     * *service_name* is the name of a service.
     * *sts_client* is a ``boto3.client('sts')`` instance. If not given, one will be
       created with ``boto3.client('sts')``.
     * *client_kwargs* are the keyword arguments you want to pass to the
       ``boto3.client()`` constructor.
     * *assume_role_kwargs* are the arguments for the ``assume_role`` operation, which
       at least include ``RoleArn``. If ``RoleSessionName`` is not given, a
       randomly-generated one will be used.

     Usage:

    .. code-block:: python

         from boto3_helpers.sts import assumed_role_client

         client_kwargs = {'region_name': 'us-east-2'}
         role_arn = 'arn:aws:iam::000000000000:role/TargetRole'
         sqs_client = assumed_role_client(
            'sqs', client_kwargs, RoleArn=role_arn
        )
    """
    client_kwargs = client_kwargs or {}

    session = assumed_role_session(sts_client=sts_client, **assume_role_kwargs)
    return session.client(service_name, **client_kwargs)


def assumed_role_resource(
    service_name, *, sts_client=None, resource_kwargs=None, **assume_role_kwargs
):
    """Return a ``boto3.resource`` object for an assumed role:

    * *service_name* is the name of a service.
    * *sts_client* is a ``boto3.client('sts')`` instance. If not given, one will be
      created with ``boto3.client('sts')``.
    * *resource_kwargs* are the keyword arguments you want to pass to the
      ``boto3.resource()`` constructor.
    * *assume_role_kwargs* are the arguments for the ``assume_role`` operation, which at
      least include ``RoleArn``. If ``RoleSessionName`` is not given, a
      randomly-generated one will be used.

    Usage:

    .. code-block:: python

        from boto3_helpers.sts import assumed_role_resource

        resource_kwargs = {'region_name': 'us-east-2'}
        role_arn = 'arn:aws:iam::000000000000:role/TargetRole'
        dynamodb_resource = assumed_role_resource(
            'dynamodb', resource_kwargs, RoleArn=role_arn
        )
    """
    resource_kwargs = resource_kwargs or {}

    session = assumed_role_session(sts_client, **assume_role_kwargs)
    return session.resource(service_name, **resource_kwargs)
