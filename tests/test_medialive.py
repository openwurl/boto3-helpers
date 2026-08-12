from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.medialive import delete_schedule_action_chain, delete_schedule_after

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
    # TODO: add more cases

    def test_delete_schedule_action_chain(self):
        self.fail()  # TODO

    def test_delete_not_found(self):
        self.fail()  # TODO

    def test_delete_schedule_after(self):
        self.fail()  # TODO
