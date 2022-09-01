from boto3 import client as boto3_client


def update_playback_configuration(config_name, emt_client=None, **config_kwargs):
    """Do a partial update of a MediaTailor configuration and return the result:

    * *emt_client* is a ``boto3.client('mediatailor')`` instance. If not given, one will
      be created with ``boto3.client('mediatailor')``.
    * *config_name* is the name of the playback configuration that will be updated.
    * *config_kwargs* are passed directly to the ``put_playback_configuration`` method.

    Usage:

    .. code-block:: python

        from boto3_helpers.mediatailor import update_playback_configuration

        new_config = update_playback_configuration(
            'ExamplePlaybackConfig',
            AdDecisionServerUrl='https://198.51.100.1:24601/ads/',
        )

    The existing playback configuration will be fetched, merged with the new values,
    and then sent to the MediaTailor API.

    .. note::

        It's possible for another API user to change the playback configuration
        in between this function's calls to  ``get_playback_configuration`` and
        ``put_playback_configuration``. The MediaTailor API doesn't allow for atomic
        updates.
    """
    emt_client = emt_client or boto3_client('mediatailor')
    playback_config = emt_client.get_playback_configuration(Name=config_name)
    for key in (
        'HlsConfiguration',
        'LogConfiguration',
        'PlaybackConfigurationArn',
        'PlaybackEndpointPrefix',
        'ResponseMetadata',
        'SessionInitializationEndpointPrefix',
    ):
        playback_config.pop(key, None)

    playback_config.get('DashConfiguration', {}).pop('ManifestEndpointPrefix', None)
    playback_config.update(config_kwargs)

    return emt_client.put_playback_configuration(**playback_config)
