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

    * *ddb_table* is a ``boto3.resource('dynamodb').Table`` instance.
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
    yield from _page_helper(ddb_table.query, **kwargs)


def scan_table(ddb_table, **kwargs):
    """Yield all of the items that match the DynamoDB query:

    * *ddb_table* is a ``boto3.resource('dynamodb').Table`` instance.
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
    yield from _page_helper(ddb_table.scan, **kwargs)


def update_attributes(ddb_table, key, update_map, **kwargs):
    """Update a DyanmoDB table item and return the result:

    * *ddb_table* is a ``boto3.resource('dynamodb').Table`` instance.
    * *key* is a mapping that identifies the item to update.
    * *update_map* is a mapping of item attributes to target values.
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

        from boto3 import resource as boto3_resource

        ddb_resource = boto3_resource('dynamodb')
        ddb_table = ddb_resource.Table('example-table')
        key = {'username': 'janedoe', 'last_name': 'Doe'}
        resp = ddb_table.update_item(
            Key=key,
            UpdateExpression='SET age = :val1',
            ExpressionAttributeValues={':val1': 26},
        )
    """
    set_parts = []
    attrib_values = {}
    for i, (k, v) in enumerate(update_map.items(), 1):
        set_parts.append(f'{k} = :val{i}')
        attrib_values[f':val{i}'] = v
    set_stmt = ', '.join(set_parts)

    return ddb_table.update_item(
        Key=key,
        UpdateExpression=f"SET {set_stmt}",
        ExpressionAttributeValues=attrib_values,
        **kwargs,
    )
