from collections import defaultdict

from boto3 import client as boto3_client
from jmespath import search as json_search

from boto3_helpers.pagination import yield_all_items

PARENT_ACTION_PATH = (
    'ScheduleActionStartSettings'
    '.FollowModeScheduleActionStartSettings'
    '.ReferenceActionName'
)


def delete_schedule_action_chain(
    channel_id, delete_action_name, eml_client=None, dry_run=False
):
    eml_client = eml_client or boto3_client('medialive')

    # Associate each item on the schedule with its immediate ancestor.
    all_action_names = set()
    immediate_parent_map = {}
    for schedule_action in yield_all_items(
        eml_client, 'describe_schedule', 'ScheduleActions', ChannelId=channel_id
    ):
        action_name = schedule_action['ActionName']
        all_action_names.add(action_name)

        parent_name = json_search(PARENT_ACTION_PATH, schedule_action)
        if parent_name:
            immediate_parent_map[action_name] = parent_name

    # If the target action was not in the schedule, bail out early.
    if delete_action_name not in all_action_names:
        raise ValueError(
            f'Action name {delete_action_name} was not present in the schedule'
        )

    # Associate each item on the schedule with its ultimate ancestor, and each
    # ultimate ancestor with all of its descendents.
    ultimate_parent_map = {}
    children_map = defaultdict(set)
    for action_name in all_action_names:
        parent_action_name = action_name
        while parent_action_name in immediate_parent_map:
            parent_action_name = immediate_parent_map[parent_action_name]

        parent_action_name = parent_action_name or action_name
        ultimate_parent_map[action_name] = parent_action_name
        children_map[parent_action_name].add(action_name)

    # Find the chain of actions associated with our target action.
    delete_action_parent_name = ultimate_parent_map[delete_action_name]
    all_deletes = sorted(children_map[delete_action_parent_name])

    # Exceute the deletion.
    if not dry_run:
        eml_client.batch_update_schedule(
            ChannelId=channel_id, Deletes={'ActionNames': all_deletes}
        )

    return all_deletes
