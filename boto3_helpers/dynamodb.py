from boto3 import resource as boto3_resource
from time import sleep


def _table_or_name(x):
    if isinstance(x, str):
        return boto3_resource('dynamodb').Table(x)

    return x


def _page_helper(method, **kwargs):
    while True:
        resp = method(**kwargs)
        yield from resp.get('Items', [])
        start_key = resp.get('LastEvaluatedKey')
        if not start_key:
            break
        kwargs['ExclusiveStartKey'] = start_key


def query_table(ddb_table, **kwargs):
    """Yield all of the items that match the DynamoDB query:

    * *ddb_table* is a table name or a ``boto3.resource('dynamodb').Table`` instance.
    * *kwargs* are passed directly to the ``Table.query`` method.

    Usage:

    .. code-block:: python

        from boto3 import resource as boto3_resource
        from boto3.dynamodb.conditions import Key
        from boto3_helpers.dynamodb import query_table

        ddb_resource = boto3_resource('dynamodb')
        ddb_table = ddb_resource.Table('example-table')
        condition = Key('username').eq('johndoe')
        all_items = list(
            query_table(ddb_table, KeyConditionExpression=condition)
        )
    """
    t = _table_or_name(ddb_table)
    yield from _page_helper(t.query, **kwargs)


def scan_table(ddb_table, **kwargs):
    """Yield all of the items that match the DynamoDB query:

    * *ddb_table* is a table name or a ``boto3.resource('dynamodb').Table`` instance.
    * *kwargs* are passed directly to the ``Table.scan`` method.

    Usage:

    .. code-block:: python

        from boto3 import resource as boto3_resource
        from boto3.dynamodb.conditions import Attr
        from boto3_helpers.dynamodb import scan_table

        ddb_resource = boto3_resource('dynamodb')
        ddb_table = ddb_resource.Table('example-table')
        condition = Attr('username').eq('johndoe')
        all_items = list(
            scan_table(ddb_table, FilterExpression=condition)
        )
    """
    t = _table_or_name(ddb_table)
    yield from _page_helper(t.scan, **kwargs)


def update_attributes(ddb_table, key, update_map, **kwargs):
    """Update a DyanmoDB table item and return the ``update_item`` response:

    * *ddb_table* is a table name or a ``boto3.resource('dynamodb').Table`` instance.
    * *key* is a mapping that identifies the item to update.
    * *update_map* is a mapping of top-level item attributes to target values.
    * *kwargs* are passed directly to the the ``Table.update_item`` method.

    Usage:

    .. code-block:: python

        from boto3 import resource as boto3_resource
        from boto3_helpers.dynamodb import update_attributes

        ddb_resource = boto3_resource('dynamodb')
        ddb_table = ddb_resource.Table('example-table')
        key = {'username': 'janedoe', 'last_name': 'Doe'}
        update_map = {'age': 26}
        resp = update_attributes(ddb_table, key, update_map)

    In the past, the ``boto3`` DynamoDB library provided a simple means of
    updating items with ``AttributeUpdates``. However, this parameter is deprecated.
    This function constructs equivalent ``UpdateExpression`` and
    ``ExpressionAttributeValues`` parameters.

    Equivalent to:

    .. code-block:: python

        ddb_resource = boto3_resource('dynamodb')
        ddb_table = ddb_resource.Table('example-table')
        key = {'username': 'janedoe', 'last_name': 'Doe'}
        resp = ddb_table.update_item(
            Key=key,
            UpdateExpression='SET age = :val1',
            ExpressionAttributeValues={':val1': 26},
        )

    Note that nested attributes (i.e. map attributes) can be updated, but that you need
    to provide values for the entire map.

    .. code-block:: python

        # Suppose that DDB had this before:
        # {
        #   "username": "janedoe",
        #   "parameters": {
        #     "age": 25,
        #     "weight_kg": 70
        #   }
        # }
        # After this call, `parameters.age` will be 26, but there will
        # no longer be a `paramters.weight_kg`.

        ddb_resource = boto3_resource('dynamodb')
        ddb_table = ddb_resource.Table('example-table')
        key = {'username': 'janedoe', 'last_name': 'Doe'}
        update_map = {'parameters': {'age': 26}}
        resp = update_attributes(ddb_table, key, update_map)

    """
    t = _table_or_name(ddb_table)
    set_parts = []
    attrib_values = {}
    for i, (k, v) in enumerate(update_map.items(), 1):
        set_parts.append(f'{k} = :val{i}')
        attrib_values[f':val{i}'] = v
    set_stmt = ', '.join(set_parts)

    return t.update_item(
        Key=key,
        UpdateExpression=f"SET {set_stmt}",
        ExpressionAttributeValues=attrib_values,
        **kwargs,
    )


def batch_yield_items(
    table_name,
    all_keys,
    ddb_resource=None,
    batch_size=100,
    backoff_base=0.1,
    backoff_max=5,
    **kwargs,
):
    """Do a series a DyanmoDB ``batch_get_item`` queries against a single table, taking
    care of retries and paging. Yield the returned items as they are available.

    * *table_name* is the name of the table.
    * *all_keys* is an iterable of dictionaries with the keys for the
      ``batch_get_item`` operation.
    * *ddb_resource* is a ``boto3.resource('dynamodb')`` instance. If not supplied, one
      will be created.
    * *batch_size* is the number of items to request per page (default: 100).
    * *backoff_base* is the value, in seconds, of the exponential backoff base for
      retries.
    * *backoff_max* is the value, in seconds, of the maximum time to wait between
      retries.
    * *kwargs* are passed directly to the the ``batch_get_item`` method.

    Usage:

    .. code-block:: python

        from boto3_helpers.dynamodb import batch_yield_items

        all_keys = [
            {'primary_key': '1', 'sort_key', 'a'},
            {'primary_key': '1', 'sort_key', 'b'},
            {'primary_key': '2', 'sort_key', 'a'},
            {'primary_key': '2', 'sort_key', 'b'},
        ]
        all_items = list('example-table', all_keys)
    """
    ddb_resource = ddb_resource or boto3_resource('dynamodb')

    i = 0
    unprocessed_keys = list(all_keys)
    while True:
        batch_keys = unprocessed_keys[:batch_size]
        unprocessed_keys = unprocessed_keys[batch_size:]
        resp = ddb_resource.batch_get_item(
            RequestItems={table_name: {'Keys': batch_keys}}, **kwargs
        )
        yield from resp['Responses'][table_name]
        unprocessed_keys += resp.get('UnprocessedKeys', {}).get(table_name, [])
        if not unprocessed_keys:
            break
        sleep(min(backoff_base * (2**i), backoff_max))
        i += 1
