def yield_all_items(boto_client, method_name, list_key, **kwargs):
    """A helper function that simplifies retrieving items from API endpoints that
    require paging. Yields each item from every page.
    *boto_client* is a ``boto3.client()`` instance for the relevant service.
    *method_name* is the name of the client method that requires paging.
    *list_key* is the name of the top-level key in the method's response that
    corresponds to the desired list of items.
    *kwargs* are passed through to the appropriate ``paginate`` method.

    S3 example:
        s3_client = boto3.client('s3')
        for item in yield_all_items(s3_client, 'list_objects_v2', 'Contents'):
            print(item['key'])

    In this example, the ``list_key`` for S3's ``list_objects_v2`` is ``'Contents'``.

    EC2 example:
        ec2_client = boto3.client('ec2')
        for item in yield_all_items(ec2_client, 'describe_instances', 'Reservations'):
            print(item['InstanceId'])

    In this example, the ``list_key`` for S3's ``describe_instances`` is
    ``'Reservations'``.
    """
    paginator = boto_client.get_paginator(method_name)
    for page in paginator.paginate(**kwargs):
        yield from page.get(list_key, [])
