from copy import deepcopy
from datetime import datetime, timezone
from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.medialive import delete_schedule_action_chain, delete_schedule_after

CHANNEL_ID = '24601'

TEST_SCHEDULE_ACTIONS = [
    # One level down from the first chain
    {
        'ActionName': 'chain_1_2',
        'ScheduleActionStartSettings': {
            'FollowModeScheduleActionStartSettings': {
                'ReferenceActionName': 'chain_1',
                'FollowPoint': 'END',
            },
        },
        'ScheduleActionSettings': {},
    },
    # Beginning of the first chain
    {
        'ActionName': 'chain_1',
        'ScheduleActionStartSettings': {
            'FixedModeScheduleActionStartSettings': {'Time': '2018-11-05T16:10:30.000Z'}
        },
        'ScheduleActionSettings': {},
    },
    # One level down
    {
        'ActionName': 'chain_1_1',
        'ScheduleActionStartSettings': {
            'FollowModeScheduleActionStartSettings': {
                'ReferenceActionName': 'chain_1',
                'FollowPoint': 'END',
            },
        },
        'ScheduleActionSettings': {},
    },
    # Two levels down
    {
        'ActionName': 'chain_1_1_1',
        'ScheduleActionStartSettings': {
            'FollowModeScheduleActionStartSettings': {
                'ReferenceActionName': 'chain_1_1',
                'FollowPoint': 'END',
            },
        },
        'ScheduleActionSettings': {},
    },
    # Standalone action
    {
        'ActionName': 'chain_2',
        'ScheduleActionStartSettings': {
            'FixedModeScheduleActionStartSettings': {'Time': '2018-11-05T16:20:30.000Z'}
        },
        'ScheduleActionSettings': {},
    },
]


class MediaLiveTests(TestCase):
    def _stub_describe_schedule(self, stubber, actions=None, *, paginate=False):
        actions = TEST_SCHEDULE_ACTIONS if actions is None else actions
        if paginate:
            stubber.add_response(
                'describe_schedule',
                {'ScheduleActions': actions[:2], 'NextToken': 'page-2'},
                {'ChannelId': CHANNEL_ID},
            )
            stubber.add_response(
                'describe_schedule',
                {'ScheduleActions': actions[2:]},
                {'ChannelId': CHANNEL_ID, 'NextToken': 'page-2'},
            )
            return

        stubber.add_response(
            'describe_schedule',
            {'ScheduleActions': actions},
            {'ChannelId': CHANNEL_ID},
        )

    def _stub_delete(self, stubber, action_names):
        stubber.add_response(
            'batch_update_schedule',
            {'Deletes': {'ScheduleActions': []}},
            {'ChannelId': CHANNEL_ID, 'Deletes': {'ActionNames': action_names}},
        )

    def test_delete_schedule_action_chain(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber, paginate=True)
        expected = ['chain_1', 'chain_1_1', 'chain_1_1_1', 'chain_1_2']
        self._stub_delete(stubber, expected)

        with stubber:
            actual = delete_schedule_action_chain(
                CHANNEL_ID, 'chain_1', eml_client=eml_client
            )

        self.assertEqual(actual, expected)

    def test_delete_schedule_action_chain_mid(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber)
        expected = ['chain_1_1', 'chain_1_1_1']
        self._stub_delete(stubber, expected)

        with stubber:
            actual = delete_schedule_action_chain(
                CHANNEL_ID, 'chain_1_1', eml_client=eml_client
            )

        self.assertEqual(actual, expected)

    def test_delete_schedule_action_chain_leaf(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber)
        expected = ['chain_2']
        self._stub_delete(stubber, expected)

        with stubber:
            actual = delete_schedule_action_chain(
                CHANNEL_ID, 'chain_2', eml_client=eml_client
            )

        self.assertEqual(actual, expected)

    def test_delete_schedule_action_chain_dry_run(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber)

        with stubber:
            actual = delete_schedule_action_chain(
                CHANNEL_ID, 'chain_1', dry_run=True, eml_client=eml_client
            )

        self.assertEqual(actual, ['chain_1', 'chain_1_1', 'chain_1_1_1', 'chain_1_2'])

    def test_delete_not_found(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber)

        with stubber, self.assertRaises(KeyError):
            delete_schedule_action_chain(
                CHANNEL_ID, 'missing-action', eml_client=eml_client
            )

    def test_delete_schedule_after(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber)
        expected = ['chain_2']
        self._stub_delete(stubber, expected)

        dt = datetime(2018, 11, 5, 16, 15, 0, tzinfo=timezone.utc)
        with stubber:
            actual = delete_schedule_after(CHANNEL_ID, dt, eml_client=eml_client)

        self.assertEqual(actual, expected)

    def test_delete_schedule_after_all(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber)
        expected = ['chain_1', 'chain_1_1', 'chain_1_1_1', 'chain_1_2', 'chain_2']
        self._stub_delete(stubber, expected)

        dt = datetime(2018, 11, 5, 16, 0, 0, tzinfo=timezone.utc)
        with stubber:
            actual = delete_schedule_after(CHANNEL_ID, dt, eml_client=eml_client)

        self.assertEqual(actual, expected)

    def test_delete_schedule_after_none(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber)

        dt = datetime(2018, 11, 5, 16, 30, 0, tzinfo=timezone.utc)
        with stubber:
            actual = delete_schedule_after(CHANNEL_ID, dt, eml_client=eml_client)

        self.assertEqual(actual, [])

    def test_delete_schedule_after_dry_run(self):
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber)

        dt = datetime(2018, 11, 5, 16, 15, 0)
        with stubber:
            actual = delete_schedule_after(
                CHANNEL_ID, dt, dry_run=True, eml_client=eml_client
            )

        self.assertEqual(actual, ['chain_2'])

    def test_delete_schedule_after_keeps_immediate(self):
        # Immediate actions have no start time in the schedule; treat them as
        # current/imminent and keep them (plus anything that follows them).
        actions = deepcopy(TEST_SCHEDULE_ACTIONS)
        actions.append(
            {
                'ActionName': 'chain_3',
                'ScheduleActionStartSettings': {
                    'ImmediateModeScheduleActionStartSettings': {}
                },
                'ScheduleActionSettings': {},
            }
        )
        actions.append(
            {
                'ActionName': 'chain_3_1',
                'ScheduleActionStartSettings': {
                    'FollowModeScheduleActionStartSettings': {
                        'ReferenceActionName': 'chain_3',
                        'FollowPoint': 'START',
                    },
                },
                'ScheduleActionSettings': {},
            }
        )

        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)
        self._stub_describe_schedule(stubber, actions)
        expected = ['chain_2']
        self._stub_delete(stubber, expected)

        dt = datetime(2018, 11, 5, 16, 15, 0, tzinfo=timezone.utc)
        with stubber:
            actual = delete_schedule_after(CHANNEL_ID, dt, eml_client=eml_client)

        self.assertEqual(actual, expected)
