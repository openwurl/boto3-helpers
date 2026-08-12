from collections import defaultdict, deque
from datetime import datetime, timezone

from boto3 import client as boto3_client

from boto3_helpers.pagination import yield_all_items


def _schedule_actions(channel_id, eml_client):
    return list(
        yield_all_items(
            eml_client, 'describe_schedule', 'ScheduleActions', ChannelId=channel_id
        )
    )


def _follow_graph(actions):
    """Map each action name to the names of actions that follow it."""
    followers = defaultdict(list)
    for action in actions:
        follow = action['ScheduleActionStartSettings'].get(
            'FollowModeScheduleActionStartSettings'
        )
        if follow is not None:
            followers[follow['ReferenceActionName']].append(action['ActionName'])
    return followers


def _action_chain(root_name, followers):
    """Return *root_name* plus every action that follows it, directly or indirectly."""
    chain = []
    seen = set()
    pending = deque([root_name])
    while pending:
        name = pending.popleft()
        if name in seen:
            continue
        seen.add(name)
        chain.append(name)
        pending.extend(followers[name])
    return chain


def _start_time(action):
    start = action['ScheduleActionStartSettings']
    settings = (
        start.get('FixedModeScheduleActionStartSettings')
        or start.get('ImmediateModeScheduleActionStartSettings')
        or {}
    )
    time_str = settings.get('Time')
    if time_str is None:
        return None
    return datetime.fromisoformat(time_str.replace('Z', '+00:00'))


def _delete_names(channel_id, action_names, dry_run, eml_client):
    action_names = sorted(action_names)
    if action_names and not dry_run:
        eml_client.batch_update_schedule(
            ChannelId=channel_id, Deletes={'ActionNames': action_names}
        )
    return action_names


def delete_schedule_action_chain(
    channel_id, delete_action_name, dry_run=False, eml_client=None
):
    """Delete a MediaLive scheduled action, plus any actions that depend on it.
    Return the names of the actions that were deleted.

    * *channel_id* is the MediaLive channel ID.
    * *delete_action_name* is the name of the scheduled action to delete.
    * *dry_run* if ``True``, do not actually delete; just return the names of the
      actions that would have been deleted.
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
    eml_client = eml_client or boto3_client('medialive')
    actions = _schedule_actions(channel_id, eml_client)
    names = {action['ActionName'] for action in actions}
    if delete_action_name not in names:
        raise KeyError(delete_action_name)

    followers = _follow_graph(actions)
    return _delete_names(
        channel_id, _action_chain(delete_action_name, followers), dry_run, eml_client
    )


def delete_schedule_after(channel_id, dt=None, dry_run=False, eml_client=None):
    """Delete scheduled MediaLive scheduled actions that take effect after
    *dt*.

    * *channel_id* is the MediaLive channel ID.
    * *dt* is a ``datetime.datetime`` instance (UTC timezone)
    * *dry_run* if ``True``, do not actually delete; just return the names of the
      actions that would have been deleted.
    * *eml_client* (optional) is a ``boto3.client('medialive')`` instance.

    Usage:

    .. code-block:: python

        from boto3_helpers.medialive import delete_schedule_after

        deleted_actions = delete_schedule_after('24601')

    MediaLive's schedule action deletion rules are followed, so after deletion, a
    schedule might contain the most recent fixed input switch and some of its follow
    actions.
    """
    # Notes: Delete any fixed mode schedule actions after dt.
    # Also delete any follow mode schedule actions after dt, if they're not about
    # to be played imminently.
    dt = dt or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    eml_client = eml_client or boto3_client('medialive')

    actions = _schedule_actions(channel_id, eml_client)
    followers = _follow_graph(actions)

    to_delete = set()
    for action in actions:
        start_time = _start_time(action)
        if start_time is None or start_time <= dt:
            continue
        to_delete.update(_action_chain(action['ActionName'], followers))

    return _delete_names(channel_id, to_delete, dry_run, eml_client)
