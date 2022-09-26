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
