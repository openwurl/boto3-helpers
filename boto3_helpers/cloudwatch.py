from boto3 import client as boto3_client

from boto3_helpers.pagination import yield_all_items


def yield_metric_data(
    namespace,
    metric_name,
    dimension_map,
    period,
    stat,
    start_time,
    end_time,
    cw_client=None,
    **kwargs,
):
    """Yield all the data associated with a single metric. Each item yielded is a
    ``(dt, value)`` pair.

    * *namespace* is the namespace for the metric.
    * *metric_name* is the name of the metric.
    * *dimension_map* is a ``dict`` that maps dimension names to values,
      e.g. ``{'Name': 'Value'}``. If the metric has no dimension, supply an empty
      ``dict``.
    * *period* is the granularity of the returned data points in seconds.
    * *stat* is the name of the statistic to evaluate, e.g. ``Maximum``.
    * *start_time* is a ``datetime.datetime`` object that specifies that beginning
      of the query.
    * *end_time* is a ``datetime.datetime`` object that specifies that end
      of the query.
    * *cw_client* is a ``boto3.client('cloudwatch')`` instance. If not given, is
      created with ``boto3.client('cloudwatch')``
    * *kwargs* can include ``Unit``, ``Expression``, ``Period``, ``AccountId``,
      ``ScanBy``, ``LabelOptions``, and ``PaginationConfig``. These are inserted
      at the appropriate place in the ``get_paginator('get_metric_data').paginate``
      call.

    Usage:

    .. code-block:: python

        from datetime import datetime, timezone
        from boto3_helpers.cloudwatch import yield_metric_data

        for dt, value in yield_metric_data(
            'AWS/S3',
            'NumberOfObjects',
            {'StorageType': 'AllStorageTypes', 'BucketName': 'example-bucket'},
            86400,
            'Maximum',
            datetime(2022, 9, 1, 0, 0, 0, timezone.utc),
            datetime(2022, 9, 16, 0, 0, 0, timezone.utc),
        ):
            print(dt.isoformat(), value, sep=' ')

    This function is designed to simplify the common case of pulling all data for a
    single metric, which is cumbersome with the normal CloudWatch ``get_metric_data``
    method.

    ``ScanBy`` is set to ``TimestampAscending`` by default, so data should be emitted
    in sorted order.

    """
    cw_client = cw_client or boto3_client('cloudwatch')

    metric_stat = {
        'Metric': {
            'Namespace': namespace,
            'MetricName': metric_name,
            'Dimensions': [{'Name': k, 'Value': v} for k, v in dimension_map.items()],
        },
        'Period': period,
        'Stat': stat,
    }
    unit = kwargs.pop('Unit', None)
    if unit is not None:
        metric_stat['Unit'] = unit

    query = {'Id': 'query0', 'MetricStat': metric_stat}
    for key in ('Expression', 'Period', 'AccountId'):
        value = kwargs.pop(key, None)
        if value is not None:
            query[key] = value

    get_kwargs = {
        'MetricDataQueries': [query],
        'StartTime': start_time,
        'EndTime': end_time,
        'ScanBy': 'TimestampAscending',
    }
    get_kwargs.update(kwargs)

    for item in yield_all_items(
        cw_client, 'get_metric_data', 'MetricDataResults', **get_kwargs
    ):
        yield from zip(item['Timestamps'], item['Values'])
