from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.medialive import delete_schedule_action_chain

TEST_SCHEDULE_ACTIONS = [
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
    # One level down again
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
    def test_delete_schedule_action_chain(self):
        # Try deleting each action in a chain; the result should be the same each time.
        channel_id = '24601'
        deletion_chain = ['chain_1', 'chain_1_1', 'chain_1_1_1', 'chain_1_2']

        for delete_action_name in deletion_chain:
            with self.subTest(delete_action_name=delete_action_name):
                # Set up the stubber
                eml_client = boto3_client('medialive', region_name='not-a-region')
                stubber = Stubber(eml_client)

                # First command is describe_schedule
                describe_resp = {'ScheduleActions': TEST_SCHEDULE_ACTIONS}
                describe_params = {'ChannelId': channel_id}
                stubber.add_response(
                    'describe_schedule', describe_resp, describe_params
                )

                # Second command is batch_update_schedule
                update_resp = {}
                update_params = {
                    'ChannelId': channel_id,
                    'Deletes': {'ActionNames': deletion_chain},
                }
                stubber.add_response(
                    'batch_update_schedule', update_resp, update_params
                )

                # Do the deed
                with stubber:
                    actual = delete_schedule_action_chain(
                        channel_id, delete_action_name, eml_client=eml_client
                    )
                self.assertEqual(actual, deletion_chain)

    def test_delete_single(self):
        # Delete a single action that's not part of a chain.
        channel_id = '24601'
        delete_action_name = 'chain_2'

        # Set up the stubber
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)

        # First command is describe_schedule
        describe_resp = {'ScheduleActions': TEST_SCHEDULE_ACTIONS}
        describe_params = {'ChannelId': channel_id}
        stubber.add_response('describe_schedule', describe_resp, describe_params)

        # Second command is batch_update_schedule
        update_resp = {}
        update_params = {
            'ChannelId': channel_id,
            'Deletes': {'ActionNames': [delete_action_name]},
        }
        stubber.add_response('batch_update_schedule', update_resp, update_params)

        # Do the deed
        with stubber:
            actual = delete_schedule_action_chain(
                channel_id, delete_action_name, eml_client=eml_client
            )
        self.assertEqual(actual, [delete_action_name])

    def test_delete_not_found(self):
        # Try deleting a non-existent action.
        channel_id = '24601'
        delete_action_name = 'chain_3'

        # Set up the stubber
        eml_client = boto3_client('medialive', region_name='not-a-region')
        stubber = Stubber(eml_client)

        # First command is describe_schedule
        describe_resp = {'ScheduleActions': TEST_SCHEDULE_ACTIONS}
        describe_params = {'ChannelId': channel_id}
        stubber.add_response('describe_schedule', describe_resp, describe_params)

        # There should be an error because the requested action is not present.
        with self.assertRaises(ValueError):
            with stubber:
                delete_schedule_action_chain(
                    channel_id, delete_action_name, eml_client=eml_client
                )
