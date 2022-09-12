from boto3 import client as boto3_client

from boto3_helpers.pagination import yield_all_items


def describe_rule_with_targets(*, events_client=None, **kwargs):
    """Return a ``dict`` with the information from the ``describe_rule``
    call combined with the information from the ``list_targets_by_rule`` call.

    * *events_client* is a ``boto3.client('events')`` instance. If not given, one will
      be created with ``boto3.client('events')``.
    * *Name* is the name of the rule to be passed to ``describe_rule``.
      This is required.
    * *EventBusName* is the name or ARN of the event bus associated with the rule. If
      omitted, the default event bus is used.

    Sample output:

    .. code-block:: python

        {
            'Name': 'example-rule',
            'Arn': 'arn:aws:events:us-east-2:00000000:rule/example-rule',
            'ScheduleExpression': 'rate(5 minutes)',
            'State': 'ENABLED',
            'EventBusName': 'default',
            'CreatedBy': '00000000',
            'Targets': [
                {
                    'Id': 'Id1c4e7db5-6dd2-47e9-b7bc-98561ae038f7',
                    'Arn': 'arn:aws:lambda:us-east-2:00000000:function:example-func',
                    'Input': '{"hello": "world"}',
                }
            ],
        }

    Usage:

    .. code-block:: python

        from boto3 import client as boto3_client
        from boto3_helpers.events import describe_rule_with_targets

        events_client = boto3_client('events')
        rule_data = describe_rule_with_targets(
            Name='example-rule', EventBusName='example-bus'
        )
        for target in rule['Targets']:
            print(
                rule_data['Arn'],
                rule_data['ScheduleExpression'],
                target['Arn'],
                sep='\t'
            )

    .. note::

        It's possible for another API user to make changes in between the calls to
        ``describe_rule`` and ``list_targets_by_rule``.
        Govern yourself accordingly. This function is here to save you the trouble of
        making these calls manually.
    """
    events_client = events_client or boto3_client('events')
    resp = events_client.describe_rule(**kwargs)
    kwargs['Rule'] = kwargs.pop('Name')
    resp['Targets'] = list(
        yield_all_items(events_client, 'list_targets_by_rule', 'Targets', **kwargs)
    )
    resp.pop('ResponseMetadata', {})

    return resp


def yield_rules_by_target(*, events_client=None, **kwargs):
    """Yield a ``dict`` with information about each rule in the
    ``list_rule_names_by_target`` response.

    * *events_client* is a ``boto3.client('events')`` instance. If not given, one will
      be created with ``boto3.client('events')``.
    * *TargetArn* is the ARN of the target. This is required
    * *EventBusName* is the name or ARN of the event bus to list rules for. If
      omitted, the default event bus is used.

    See :func:`describe_rule_with_targets` for the output format.

    Usage:

    .. code-block:: python

        from boto3 import client as boto3_client
        from boto3_helpers.events import yield_rules_by_target

        events_client = boto3_client('events')
        for rule_data in yield_rules_by_target(
            TargetArn='arn:aws:lambda:us-east-2:00000000:function:example-func'
        ):
            print(
                rule_data['Arn'],
                rule_data['ScheduleExpression'],
                len(rule_data['Targets']),
                sep='\t'
            )

    .. note::

        It's possible for another API user to make changes in between the calls to
        ``list_rule_names_by_target``, ``describe_rule``, and ``list_targets_by_rule``.
        Govern yourself accordingly. This function is here to save you the trouble of
        making these calls manually.

    """
    events_client = events_client or boto3_client('events')
    target_arn = kwargs['TargetArn']
    for rule_name in yield_all_items(
        events_client, 'list_rule_names_by_target', 'RuleNames', **kwargs
    ):
        describe_kwargs = {'Name': rule_name}
        if 'EventBusName' in kwargs:
            describe_kwargs['EventBusName'] = kwargs['EventBusName']
        rule_data = describe_rule_with_targets(
            events_client=events_client, **describe_kwargs
        )
        rule_data['Targets'] = [
            t for t in rule_data['Targets'] if t['Arn'] == target_arn
        ]
        yield rule_data


def yield_rules_with_targets(*, events_client=None, **kwargs):
    """Yield a ``dict`` with the information from the ``describe_rule``
    call combined with the information from the ``list_targets_by_rule`` call for
    each rule in the ``list_rules`` response.

    * *events_client* is a ``boto3.client('events')`` instance. If not given, one will
      be created with ``boto3.client('events')``.
    * *NamePrefix* is an optional filtering prefix for rule name
    * *EventBusName* is the name or ARN of the event bus to list rules for. If
      omitted, the default event bus is used.

    See :func:`describe_rule_with_targets` for the output format.

    Usage:

    .. code-block:: python

        from boto3 import client as boto3_client
        from boto3_helpers.events import yield_rules_with_targets

        events_client = boto3_client('events')
        for rule_data in yield_rules_with_targets():
            print(
                rule_data['Name'],
                rule_data['ScheduleExpression'],
                len(rule_data['Targets']),
                sep='\t'
            )

    .. note::

        It's possible for another API user to make changes in between the calls to
        ``list_rules`` and ``list_targets_by_rule``.
        Govern yourself accordingly. This function is here to save you the trouble of
        making these calls manually.

    """
    events_client = events_client or boto3_client('events')
    for rule_data in yield_all_items(events_client, 'list_rules', 'Rules', **kwargs):
        rule_data['Targets'] = list(
            yield_all_items(
                events_client,
                'list_targets_by_rule',
                'Targets',
                Rule=rule_data['Name'],
                EventBusName=rule_data['EventBusName'],
            )
        )
        yield rule_data
