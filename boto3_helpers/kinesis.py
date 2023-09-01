from itertools import chain, zip_longest

from boto3 import client as boto3_client


def yield_all_shards(kinesis_client=None, **kwargs):
    """Due to a `bug <https://github.com/boto/botocore/issues/2009>`_ in ``botocore``,
    the ``list_shards`` paginator does not work correctly. This function yields
    the information from all shards in a Kinesis stream.

    * *kinesis_client* is a ``boto3.client('kinesis_client')`` instance. If not given,
      one will be created with ``boto3.client('kinesis_client')``.
    * *kwargs* are passed directly to the ``list_shards`` method.
      You'll need to supply at least *StreamARN* or *StreamName*.

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


def yield_available_shard_records(kinesis_client=None, **kwargs):
    """Yield all available records from the given Kinesis stream shard.
    Records will be pulled from until ``MillisBehindLatest`` is zero.

    * *ShardId* is the ID of the shard.
    * *kinesis_client* is a ``boto3.client('kinesis_client')`` instance. If not given,
      one will be created with ``boto3.client('kinesis_client')``.
    * *kwargs* are passed directly to the ``get_shard_iterator`` method.
      You'll need to supply at least *StreamARN* (or *StreamName*) and *ShardId*.
      By default you'll get records from the stream's ``TRIM_HORIZON``.

    Reading from the earliest available record:

    .. code-block:: python

        from datetime import datetime, timedelta, timezone
        from boto3_helpers.kinesis import yield_available_shard_records

        for record in yield_available_shard_records('example-stream', 'shard-0001'):
            print(record['SequenceNumber], record['Data], sep='\t')

    """
    kinesis_client = kinesis_client or boto3_client('kinesis')

    kwargs.setdefault('ShardIteratorType', 'TRIM_HORIZON')
    shard_iterator = kinesis_client.get_shard_iterator(**kwargs)['ShardIterator']

    while True:
        resp = kinesis_client.get_records(ShardIterator=shard_iterator)
        yield from resp.get('Records', [])
        if not resp['MillisBehindLatest']:
            break
        shard_iterator = resp['NextShardIterator']


def yield_available_stream_records(kinesis_client=None, **kwargs):
    """Yield all available records from the given Kinesis stream.
    Records will be pulled from each of the stream's shards until ``MillisBehindLatest``
    is zero. The shards' records will be interleaved together (example: if a stream has
    three shards, the first record yielded will be from shard A, the second will be from
    shard B, the third will be from shard, the fourth will be from shard A, etc.).

    * *kinesis_client* is a ``boto3.client('kinesis_client')`` instance. If not given,
      one will be created with ``boto3.client('kinesis_client')``.
    * *kwargs* are passed directly to the ``get_shard_iterator`` method.
      You'll need to supply at least *StreamARN* or *StreamName*.
      By default you'll get records from the stream's ``TRIM_HORIZON``.

    Reading from the earliest available record:

    .. code-block:: python

        from datetime import datetime, timedelta, timezone
        from boto3_helpers.kinesis import yield_available_stream_records

        for record in yield_available_stream_records(StreamName='example-stream'):
            print(record['SequenceNumber], record['Data], sep='\t')

    Reading from a particular timestamp:

    .. code-block:: python

        from datetime import datetime, timedelta, timezone
        from boto3_helpers.kinesis import yield_available_stream_records

        for record in yield_available_stream_records(
            'example-stream',
            ShardIteratorType='AT_TIMESTAMP',
            Timestamp=datetime.now(timezone.utc) - timedelta(hours=1),
        ):
            print(record['SequenceNumber], record['Data], sep='\t')

    .. note::

        This is a synchronous function, and may not be fast enough for real-time
        processing of high volume streams.
    """
    list_shards_kwargs = {}
    for key in ('StreamName', 'StreamARN'):
        if key in kwargs:
            list_shards_kwargs[key] = kwargs[key]

    all_shard_records = []
    for shard in yield_all_shards(kinesis_client=kinesis_client, **list_shards_kwargs):
        shard_records = yield_available_shard_records(
            ShardId=shard['ShardId'], kinesis_client=kinesis_client, **kwargs
        )
        all_shard_records.append(shard_records)

    for item in chain.from_iterable(zip_longest(*all_shard_records)):
        if item is not None:
            yield item
