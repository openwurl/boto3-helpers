def yield_all_items(boto_client, method_name, list_key, **kwargs):
    """A helper function that simplifies retrieving items from API endpoints that
    require paging. Yields each item from every page:

    * *boto_client* is a ``boto3.client()`` instance for the relevant service.
    * *method_name* is the name of the client method that requires paging.
    * *list_key* is the name of the top-level key in the method's response that
      corresponds to the desired list of items.
    * *kwargs* are passed through to the appropriate ``paginate`` method.

    EC2 example:

    .. code-block:: python

        from boto3 import client as boto3_client
        from boto3_helpers.pagination import yield_all_items

        ec2_client = boto3_client('ec2')
        for item in yield_all_items(
            ec2_client, 'describe_instances', 'Reservations'
        ):
            print(item['ReservationId'])

    In this example, the ``list_key`` for EC2's ``describe_instances`` is
    ``'Reservations'``.

    S3 example:

    .. code-block:: python

        from boto3 import client as boto3_client
        from boto3_helpers.pagination import yield_all_items

        s3_client = boto3_client('s3')
        for item in yield_all_items(
            s3_client,
            'list_objects_v2',
            'Contents',
            Bucket='example-bucket',
            Prefix='example-prefix/'
        ):
            print(item['Key'])

    In this example, the ``list_key`` for S3's ``list_objects_v2`` is ``'Contents'``.

    """
    paginator = boto_client.get_paginator(method_name)
    for page in paginator.paginate(**kwargs):
        yield from page.get(list_key, [])
