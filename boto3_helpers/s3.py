from json import loads

from boto3 import client as boto3_client


SELECT_FORMATS = {
    'json': {'JSON': {'Type': 'DOCUMENT'}},
    'json.gz': {'JSON': {'Type': 'DOCUMENT'}, 'CompressionType': 'GZIP'},
    'jsonl': {'JSON': {'Type': 'LINES'}},
    'jsonl.gz': {'JSON': {'Type': 'LINES'}, 'CompressionType': 'GZIP'},
    'csv': {'CSV': {'FileHeaderInfo': 'USE', 'FieldDelimeter': ','}},
    'csv.gz': {
        'CSV': {'FileHeaderInfo': 'USE', 'FieldDelimeter': ','},
        'CompressionType': 'GZIP',
    },
    'tsv': {'CSV': {'FileHeaderInfo': 'USE', 'FieldDelimeter': '\t'}},
    'tsv.gz': {
        'CSV': {'FileHeaderInfo': 'USE', 'FieldDelimeter': '\t'},
        'CompressionType': 'GZIP',
    },
}


def query_object(bucket, key, query, input_format, *, s3_client=None, **kwargs):
    """Runs an S3 Select query on the given object and yields each of
    the matching records.

    * *bucket* is the S3 bucket to use
    * *key* is the key to query
    * *query* is the S3 Select SQL query to use
    * *input_format* this can be ``json``, ``json.gz``,
      ``jsonl``, ``jsonl.gz``, ``csv``, ``csv.gz``, ``tsv``, ``tsv.gz``,
      or ``None``.
    * *s3_client* is a ``boto3.client('s3')`` instance. If not given,
      one will be created with ``boto3.client('s3')``.
    * *kwargs* are passed to the ``select_object_content`` method.

    The ``csv``, ``csv.gz``, ``tsv``, and ``tsv.gz`` input formats
    assume a usable header row. To customize the input format, set
    *input_format* to ``None`` and specify ``InputSerialization``
    in *kwargs*.

    Each of the output records will be decoded with ``json.loads``
    before being yielded. The function takes care of combining
    partial records from S3's event stream.

    .. code-block:: python

        from boto3_helpers.s3 import query_object

        for record in query_object(
            'ExampleBucket',
            'ExamplePath/ExampleKey.jsonl.gz',
            'SELECT * FROM s3object s',
            'jsonl.gz',
        ):
            print(record['SomeField'], record['OtherField'], sep=' ')

    """
    s3_client = s3_client or boto3_client('s3')

    request_kwargs = {
        'Bucket': bucket,
        'Key': key,
        'Expression': query,
        'ExpressionType': 'SQL',
    }
    if input_format in SELECT_FORMATS:
        request_kwargs['InputSerialization'] = SELECT_FORMATS[input_format]

    request_kwargs.update(kwargs)

    request_kwargs['OutputSerialization'] = {'JSON': {}}

    resp = s3_client.select_object_content(**request_kwargs)
    data = bytearray()
    for event in resp['Payload']:
        if 'Records' in event:
            data += event['Records']['Payload']
            for line in data.splitlines(True):
                if line.endswith(b'\n'):
                    yield loads(line)
                else:
                    data[:] = line


def head_bucket(bucket, s3_client=None, **kwargs):
    """Perform a ``HeadBucket`` API call and return the response. If the given
    *bucket* does not exist, raise ``s3_client.exceptions.NoSuchBucket``

    * *bucket* is the S3 bucket to use
    * *s3_client* is a ``boto3.client('s3')`` instance. If not given,
      one will be created with ``boto3.client('s3')``.
    * *kwargs* are passed to the ``head_bucket`` method.

    The ``boto3`` docs infamously claim that the `head_bucket` method can raise
    ``S3.Client.exceptions.NoSuchBucket``, leading reasonable people to assume that
    this exception will be raised if the requested *bucket* does not exist. Alas,
    it raises a generic ``ClientError`` instead. This function fixes the problem.

    .. code-block:: python

        from boto3 import client as boto3_client
        from boto3_helpers.s3 import head_bucket

        s3_client = boto3_client('s3')
        try:
            head_bucket('ExampleBucket', s3_client=s3_client)
        except s3_client.exceptions.NoSuchBucket:
            print('No such bucket')
        else:
            print('That bucket exists')
    """
    s3_client = s3_client or boto3_client('s3')
    kwargs['Bucket'] = bucket
    try:
        return s3_client.head_bucket(**kwargs)
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise s3_client.exceptions.NoSuchBucket(e.response, e.operation_name)
        raise


def create_bucket(bucket, region_name='us-east-1', s3_client=None, **kwargs):
    """Create a bucket in the given *region_name*.

    * *bucket* is the S3 bucket to use
    * *region_name* is the region to use.
    * *s3_client* is a ``boto3.client('s3')`` instance. If not given,
      one will be created with ``boto3.client('s3')``.
    * *kwargs* are passed to the ``create_bucket`` method.

    This helper smooths out a quirk in the S3 API. To create buckets outsied of
    the us-east-1 region, you must specify a ``LocationConstraint``.
    But to create a bucket in us-east-1, you must not use a ``LocationConstraint``.

    .. code-block:: python

        from boto3 import client as boto3_client
        from boto3_helpers.s3 import create_bucket

        s3_client = boto3_client('s3')
        create_bucket('ExampleBucket', region_name='us-west-1', s3_client=s3_client)
    """
    s3_client = s3_client or boto3_client('s3')
    kwargs['Bucket'] = bucket

    create_bucket_configuration = kwargs.pop('CreateBucketConfiguration', {})
    create_bucket_configuration.pop('LocationConstraint', None)
    if region_name != 'us-east-1':
        create_bucket_configuration['LocationConstraint'] = region_name

    if create_bucket_configuration:
        kwargs['CreateBucketConfiguration'] = create_bucket_configuration

    return s3_client.create_bucket(**kwargs)
