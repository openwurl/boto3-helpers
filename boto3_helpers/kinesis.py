from boto3 import client as boto3_client


def yield_all_shards(kinesis_client=None, **kwargs):
    """Due to a `bug <https://github.com/boto/botocore/issues/2009>`_ in ``botocore``,
    the ``list_shards`` paginator does not work correctly. This function yields
    the information from all shards in a Kinesis stream.

    * *kinesis_client* is a ``boto3.client('kinesis_client')`` instance. If not given,
      one will be created with ``boto3.client('kinesis_client')``.
    * *kwargs* are passed directly to the ``list_shards`` method. You probably want to
      supply at least ``StreamName``.

    Usage:

    .. code-block:: python

        from boto3_helpers.kinesis import yield_all_shards

        for shard in yield_all_shards(StreamName='example-stream'):
            print(shard['ShardId'])

    """
    kinesis_client = kinesis_client or boto3_client('kinesis')

    while True:
        # The API docs say:
        # "You cannot specify this parameter if you specify the NextToken parameter"
        # for the three parameters below. This is why the standard paging tool fails.
        if 'NextToken' in kwargs:
            kwargs.pop('StreamName', None)
            kwargs.pop('ExclusiveStartShardId', None)
            kwargs.pop('StreamCreationTimestamp', None)

        resp = kinesis_client.list_shards(**kwargs)
        yield from resp.get('Shards', [])

        next_token = resp.get('NextToken')
        if not next_token:
            break
        kwargs['NextToken'] = next_token
