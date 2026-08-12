from collections import defaultdict
from datetime import datetime, timezone

from boto3 import client as boto3_client
from jmespath import search as json_search

from boto3_helpers.pagination import yield_all_items

PARENT_ACTION_PATH = (
    'ScheduleActionStartSettings'
    '.FollowModeScheduleActionStartSettings'
    '.ReferenceActionName'
)


def _parse_action_chains(eml_actions):
    parent_map = {}
    for schedule_action in eml_actions:
        action_name = schedule_action['ActionName']
        parent_name = json_search(PARENT_ACTION_PATH, schedule_action) or action_name
        parent_map[action_name] = parent_name

    children_map = defaultdict(set)
    for action_name, parent_name in parent_map.items():
        children_map[action_name].add(action_name)

        while True:
            children_map[parent_name].add(action_name)
            if parent_name == parent_map[parent_name]:
                break
            parent_name = parent_map[parent_name]

    return parent_map, children_map


def delete_schedule_action_chain(
    channel_id, delete_action_name, dry_run=False, eml_client=None
):
    """Delete a MediaLive scheduled action, plus any actions that depend on it.
    Return the names of the actions that were deleted.

    * *channel_id* is the MediaLive channel ID.
    * *delete_action_name* is the name of the scheduled action to delete.
    * *dry_run* determines whether the delete actions are actually executed. Set to
      ``False`` to return the names of the actions that _would_ have been deleted.
    * *eml_client* (optional) is a ``boto3.client('medialive')`` instance.

    Usage:

    .. code-block:: python

        from boto3_helpers.medialive import delete_schedule_action_chain

        deleted_actions = delete_schedule_action_chain(
            '24601', 'switch-immediate'
        )

    MediaLive's deletion rules still apply: you can't delete an action chain associated
    with the most recent input switch.

    """
    raise NotImplementedError


def delete_schedule_after(channel_id, dt=None, dry_run=False, eml_client=None):
    """Delete scheduled MediaLive scheduled actions that take effect after
    *dt*.

    * *channel_id* is the MediaLive channel ID.
    * *dt* is a ``datetime.datetime`` instance (UTC timezone)
    * *dry_run* determines whether the delete actions are actually executed. Set to
      ``False`` to return the names of the actions that _would_ have been deleted.
    * *eml_client* (optional) is a ``boto3.client('medialive')`` instance.

    Usage:

    .. code-block:: python

        from boto3_helpers.medialive import delete_schedule_after

        deleted_actions = delete_schedule_after('24601)

    MediaLive's schedule action deletion rules are followed, so after deletion, a
    schedule might contain the most recent fixed input switch and some of its follow
    actions.
    """
    # Notes: Delete any fixed mode schedule actions after dt.
    # Also delete any follow mode schedule actions after dt, if they're not about
    # to be played imminently.
    dt = dt or datetime.now(timezone.utc)
    raise NotImplementedError
