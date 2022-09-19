from secrets import token_hex

from boto3 import client as boto3_client

MESSAGE_LIMIT = 10
SIZE_LIMIT = 262144


def _get_size(message):
    # The size of the message body is the size of the UTF-8 representation
    ret = len(message['MessageBody'].encode('utf-8'))

    # All parts of the message attribute, including Name, DataType, and Value are part
    # of the message size restriction
    for attr_name, attr_data in message.get('MessageAttributes', {}).items():
        ret += len(attr_name.encode('utf-8'))
        ret += len(attr_data['DataType'].encode('utf-8'))
        if 'StringValue' in attr_data:
            ret += len(attr_data['StringValue'].encode('utf-8'))
        elif 'BinaryValue' in attr_data:
            ret += len(attr_data['BinaryValue'])

    # MessageSystemAttributes don't count towards the total size of a message.
    return ret


def _get_batches(all_messages, message_limit, size_limit):
    base_id = token_hex(4)
    current_batch = []
    current_size = 0
    current_count = 0
    for i, message in enumerate(all_messages, 1):
        message.setdefault('Id', f'{base_id}-{i}')

        message_size = _get_size(message)
        reached_size = (current_size + message_size) > size_limit
        reached_count = current_count == message_limit
        if current_batch and (reached_size or reached_count):
            yield current_batch[:]
            del current_batch[:]
            current_size = 0
            current_count = 0

        current_batch.append(message)
        current_size += message_size
        current_count += 1

    if current_batch:
        yield current_batch[:]


def send_batches(
    queue_url,
    all_messages,
    sqs_client=None,
    message_limit=MESSAGE_LIMIT,
    size_limit=SIZE_LIMIT,
):
    """Call ``send_message_batch`` as many times as necessary to deliver the messages
    in *all_messages*, creating batches that fit SQS limits automatically.

    * *queue_url* is the URL of the SQS queue.
    * *all_messages* is an iterable of message entries, like what you would use for
      ``send_message`` or ``send_message_batch``.
    * *sqs_client* is a ``boto3.client('sqs')`` instance. If not given, is created
      with ``boto3.client('sqs')``.
    * *message_limit* is ``10`` by default. This is the maximum number of messages to
      be sent per batch.
    * *size_limit* is ``262_144`` (256 KiB) by default. This is the maximum batch
      payload size.

    Return value:

    .. code-block:: python

        {
            'Successful': [
                {
                    'Id': 'string',
                    'MessageId': 'string',
                    'MD5OfMessageBody': 'string',
                    'MD5OfMessageAttributes': 'string',
                    'MD5OfMessageSystemAttributes': 'string',
                    'SequenceNumber': 'string'
                },
            ],
            'Failed': [
                {
                    'Id': 'string',
                    'SenderFault': bool,
                    'Code': 'string',
                    'Message': 'string'
                },
            ]
        }


    If you don't supply an ``Id`` parameter in your messages, one will be inserted
    automatically.

    Messages from *all_messages* are collected in order. If the number of message
    reaches the *message_limit* or the combined payload size of the messages reaches
    *size_limit*, a new batch will be started. The size calculation includes message
    attributes.

    Usage:

    .. code-block:: python

        from boto3_helpers.sqs import send_batches

        queue_url = 'https://sqs.test-region-1.amazonaws.com/000000000000/test-queue'
        all_messages = [
            {'MessageBody': 'Beautiful is better than ugly'},
            {'MessageBody': 'Explicit is better than implicit', 'DelaySeconds': 120},
            {'MessageBody': 'Simple is better than complex'},
            # Fill this in with an arbitrary number of messages
        ]
        send_batches(queue_url, all_messages)

    """
    sqs_client = sqs_client or boto3_client('sqs')

    ret = {'Successful': [], 'Failed': []}
    for batch in _get_batches(all_messages, message_limit, size_limit):
        resp = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=batch)
        ret['Successful'] += resp.get('Successful', [])
        ret['Failed'] += resp.get('Failed', [])

    return ret
