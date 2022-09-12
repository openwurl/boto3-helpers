from copy import deepcopy
from unittest import TestCase
from unittest.mock import call as MockCall, patch

from botocore.stub import Stubber
from boto3 import client as boto3_client

from boto3_helpers.events import (
    describe_rule_with_targets,
    yield_rules_by_target,
    yield_rules_with_targets,
)


class EventsTests(TestCase):
    def test_describe_rule_with_targets(self):
        # Set up the stubber
        account = '00000000'
        region = 'not-a-region'
        rule_name = 'test-rule'
        bus_name = 'test-bus'

        events_client = boto3_client('events', region_name=region)
        stubber = Stubber(events_client)

        # We start with the normal describe_rule call
        describe_params = {'Name': rule_name, 'EventBusName': bus_name}
        describe_resp = {
            'Name': rule_name,
            'Arn': f'arn:aws:events:{region}:{account}:rule/{rule_name}',
            'ScheduleExpression': 'rate(5 minutes)',
            'State': 'ENABLED',
            'EventBusName': bus_name,
            'CreatedBy': account,
            'ResponseMetadata': {},
        }
        stubber.add_response('describe_rule', describe_resp, describe_params)

        # Then we page through the targets in the list_targets_by_rule call
        list_params_1 = {'Rule': rule_name, 'EventBusName': bus_name}
        list_resp_1 = {
            'Targets': [
                {
                    'Id': 'Id90e95dc9-731c-41ed-b29f-31fe1a9f2b15',
                    'Arn': f'arn:aws:lambda:{region}:{account}:function/test-func-1',
                },
                {
                    'Id': 'Id06cb632e-f41a-4028-828f-e62c7c802d82',
                    'Arn': f'arn:aws:lambda:{region}:{account}:function/test-func-2',
                },
            ],
            'NextToken': 'test-token',
        }
        stubber.add_response('list_targets_by_rule', list_resp_1, list_params_1)

        list_params_2 = {
            'Rule': rule_name,
            'EventBusName': bus_name,
            'NextToken': 'test-token',
        }
        list_resp_2 = {
            'Targets': [
                {
                    'Id': 'Ideec87129-cdfe-43f5-a80f-9ec41b03ea67',
                    'Arn': f'arn:aws:lambda:{region}:{account}:function/test-func-3',
                },
            ],
        }
        stubber.add_response('list_targets_by_rule', list_resp_2, list_params_2)

        # Do the deed
        with stubber:
            actual = describe_rule_with_targets(
                Name=rule_name, EventBusName=bus_name, events_client=events_client
            )
        expected = {
            'Name': rule_name,
            'Arn': describe_resp['Arn'],
            'ScheduleExpression': describe_resp['ScheduleExpression'],
            'State': describe_resp['State'],
            'EventBusName': bus_name,
            'CreatedBy': account,
            'Targets': list_resp_1['Targets'] + list_resp_2['Targets'],
        }

        self.assertEqual(actual, expected)

    @patch('boto3_helpers.events.describe_rule_with_targets', autospec=True)
    def test_yield_rules_by_target(self, mock_describe_rule_with_targets):
        # Simulate calls to describe_rule_with_targets
        account = '00000000'
        region = 'not-a-region'
        target_arn = f'arn:aws:lambda:{region}:{account}:function/test-func-1'
        bus_name = 'test-bus'

        describe_responses = [
            {
                'Name': 'test-rule-1',
                'Arn': f'arn:aws:events:{region}:{account}:rule/test-rule-1',
                'ScheduleExpression': 'rate(5 minutes)',
                'State': 'ENABLED',
                'EventBusName': bus_name,
                'CreatedBy': account,
                'Targets': [
                    {
                        'Id': 'Id30c80c70-b4fa-4a64-931a-19d34893c04e',
                        'Arn': target_arn,
                    },
                    {
                        'Id': 'Id1f3f73d0-5738-4c0e-af47-2f4b09eca55c',
                        'Arn': f'arn:aws:lambda:{region}:{account}:function/other-func',
                    },
                ],
            },
            {
                'Name': 'test-rule-2',
                'Arn': f'arn:aws:events:{region}:{account}:rule/test-rule-2',
                'ScheduleExpression': 'rate(10 minutes)',
                'State': 'ENABLED',
                'EventBusName': bus_name,
                'CreatedBy': account,
                'Targets': [
                    {
                        'Id': 'Id61bf250a-dc02-4857-9350-c1f3b4a5ad70',
                        'Arn': target_arn,
                    },
                ],
            },
        ]
        mock_describe_rule_with_targets.side_effect = deepcopy(describe_responses)

        # Set up the stubber with calls to the list_rule_names_by_target paginator
        events_client = boto3_client('events', region_name=region)
        stubber = Stubber(events_client)

        list_params_1 = {'TargetArn': target_arn, 'EventBusName': bus_name}
        list_resp_1 = {'RuleNames': ['test-rule-1'], 'NextToken': 'test-token'}
        stubber.add_response('list_rule_names_by_target', list_resp_1, list_params_1)

        list_params_2 = {
            'TargetArn': target_arn,
            'EventBusName': bus_name,
            'NextToken': 'test-token',
        }
        list_resp_2 = {'RuleNames': ['test-rule-2']}
        stubber.add_response('list_rule_names_by_target', list_resp_2, list_params_2)

        # Do the deed - the first rule's second target will be filtered out
        with stubber:
            actual = list(
                yield_rules_by_target(
                    TargetArn=target_arn,
                    EventBusName=bus_name,
                    events_client=events_client,
                )
            )
        describe_responses[0]['Targets'].pop()
        self.assertEqual(actual, describe_responses)

        # Check the calls
        mock_describe_rule_with_targets.assert_has_calls(
            [
                MockCall(
                    Name='test-rule-1',
                    EventBusName=bus_name,
                    events_client=events_client,
                ),
                MockCall(
                    Name='test-rule-2',
                    EventBusName=bus_name,
                    events_client=events_client,
                ),
            ]
        )

    def test_yield_rules_with_targets(self):
        # Set up the stubber
        account = '00000000'
        region = 'not-a-region'
        bus_name = 'test-bus'

        events_client = boto3_client('events', region_name=region)
        stubber = Stubber(events_client)

        # list_rules first page, followed by list_targets_by_rule
        rule_params_1 = {'EventBusName': bus_name}
        rule_resp_1 = {
            'Rules': [
                {
                    'Name': 'test-rule-1',
                    'Arn': f'arn:aws:events:{region}:{account}:rule/test-rule-1',
                    'ScheduleExpression': 'rate(5 minutes)',
                    'State': 'ENABLED',
                    'EventBusName': bus_name,
                },
            ],
            'NextToken': 'test-token',
        }
        stubber.add_response('list_rules', rule_resp_1, rule_params_1)

        target_params_1 = {'Rule': 'test-rule-1', 'EventBusName': bus_name}
        target_resp_1 = {
            'Targets': [
                {
                    'Id': 'Id90e95dc9-731c-41ed-b29f-31fe1a9f2b15',
                    'Arn': f'arn:aws:lambda:{region}:{account}:function/test-func-1',
                },
            ],
        }
        stubber.add_response('list_targets_by_rule', target_resp_1, target_params_1)

        # list_rules second page, followed by list_targets_by_rule
        rule_params_2 = {'EventBusName': bus_name, 'NextToken': 'test-token'}
        rule_resp_2 = {
            'Rules': [
                {
                    'Name': 'test-rule-2',
                    'Arn': f'arn:aws:events:{region}:{account}:rule/test-rule-2',
                    'ScheduleExpression': 'rate(5 minutes)',
                    'State': 'ENABLED',
                    'EventBusName': bus_name,
                },
            ],
        }
        stubber.add_response('list_rules', rule_resp_2, rule_params_2)

        target_params_2 = {'Rule': 'test-rule-2', 'EventBusName': bus_name}
        target_resp_2 = {
            'Targets': [
                {
                    'Id': 'Id90e95dc9-731c-41ed-b29f-31fe1a9f2b15',
                    'Arn': f'arn:aws:lambda:{region}:{account}:function/test-func-2',
                },
            ],
            'NextToken': 'test-token',
        }
        stubber.add_response('list_targets_by_rule', target_resp_2, target_params_2)

        target_params_3 = {
            'Rule': 'test-rule-2',
            'EventBusName': bus_name,
            'NextToken': 'test-token',
        }
        target_resp_3 = {
            'Targets': [
                {
                    'Id': 'Id90e95dc9-731c-41ed-b29f-31fe1a9f2b15',
                    'Arn': f'arn:aws:lambda:{region}:{account}:function/test-func-3',
                },
            ],
        }
        stubber.add_response('list_targets_by_rule', target_resp_3, target_params_3)

        # Do the deed
        with stubber:
            actual = list(
                yield_rules_with_targets(
                    EventBusName=bus_name, events_client=events_client
                )
            )

        rule_1 = deepcopy(rule_resp_1['Rules'][0])
        rule_1['Targets'] = target_resp_1['Targets']

        rule_2 = deepcopy(rule_resp_2['Rules'][0])
        rule_2['Targets'] = target_resp_2['Targets'] + target_resp_3['Targets']

        expected = [rule_1, rule_2]

        self.assertEqual(actual, expected)
