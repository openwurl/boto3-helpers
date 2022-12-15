from boto3 import client as boto3_client

from boto3_helpers.pagination import yield_all_items


def yield_schedules_with_details(*, sched_client=None, **kwargs):
    """Yield a ``dict`` with the information from the ``get_schedule`` call
    for all schedules.

    * *sched_client* is a ``boto3.client('scheduler')`` instance. If not given, one will
      be created with ``boto3.client('scheduler')``.
    * *kwargs* are passed directly to ``list_schedules``.

    Usage:

    .. code-block:: python

        from boto3 import client as boto3_client
        from boto3_helpers.events import yield_schedules_with_details

        sched_client = boto3_client('scheduler')
        for schedule in yield_schedules_with_details():
            print(
                rule_data['Name'],
                rule_data['GroupName'],
                rule_data['ScheduleExpression'],
                rule_data['ScheduleExpressionTimezone'],
                rule_data['Target']['Arn'],
                sep='\t'
            )

    .. note::

        This function first calls ``list_schedules`` and then makes a series of
        ``get_schedule`` calls. It's possible for another user's actions to prevent
        this from working as intended, e.g. by deleting a schedule after the list
        call but before the get call. Govern yourself accordingly.

    """
    sched_client = sched_client or boto3_client('scheduler')
    for schedule_listing in yield_all_items(
        sched_client, 'list_schedules', 'Schedules', **kwargs
    ):
        name = schedule_listing['Name']
        group_name = schedule_listing['GroupName']
        details = sched_client.get_schedule(GroupName=group_name, Name=name)
        schedule_listing.update(details)
        schedule_listing.pop('ResponseMetadata', {})
        yield schedule_listing
