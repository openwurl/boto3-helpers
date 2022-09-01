from copy import deepcopy
from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.mediatailor import update_playback_configuration


class MediaTailorTests(TestCase):
    def test_update_playback_configuration(self):
        # Set up the stubber
        emt_client = boto3_client('mediatailor', region_name='not-a-region')
        stubber = Stubber(emt_client)

        # First is the get command
        get_resp = {
            'AdDecisionServerUrl': 'https://localhost/ads/test-configuration',
            'AvailSuppression': {'Mode': 'BEHIND_LIVE_EDGE', 'Value': '00:00:00'},
            'Bumper': {},
            'CdnConfiguration': {
                'AdSegmentUrlPrefix': 'https://localhost/cache/ad-segments/'
            },
            'DashConfiguration': {
                'ManifestEndpointPrefix': 'https://localhost/playback/dash/',
                'MpdLocation': 'EMT_DEFAULT',
                'OriginManifestType': 'MULTI_PERIOD',
            },
            'HlsConfiguration': {
                'ManifestEndpointPrefix': 'https://localhost/playback/hls/'
            },
            'LivePreRollConfiguration': {},
            'ManifestProcessingRules': {'AdMarkerPassthrough': {'Enabled': False}},
            'Name': 'TestConfiguration',
            'PlaybackConfigurationArn': (
                'arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/'
                'TestConfiguration'
            ),
            'PlaybackEndpointPrefix': 'https://localhost/playback/',
            'SessionInitializationEndpointPrefix': 'https://localhost/playback/init/',
            'Tags': {},
            'TranscodeProfileName': '',
            'VideoContentSourceUrl': 'https://localhost/origin/hls/',
            'LogConfiguration': {'PercentEnabled': 1},
        }
        get_params = {'Name': 'TestConfiguration'}
        stubber.add_response('get_playback_configuration', get_resp, get_params)

        # Next is the put command - everything should be the same as above,
        # but the target valus should be updated and the read-only values not present.
        put_params = {
            'AdDecisionServerUrl': 'https://198.51.100.1:24601/ads/',  # Updated
            'AvailSuppression': {'Mode': 'off'},  # Updated
            'Bumper': {},
            'CdnConfiguration': {
                'AdSegmentUrlPrefix': 'https://localhost/cache/ad-segments/'
            },
            'DashConfiguration': {
                'MpdLocation': 'EMT_DEFAULT',
                'OriginManifestType': 'MULTI_PERIOD',
            },
            'LivePreRollConfiguration': {},
            'ManifestProcessingRules': {'AdMarkerPassthrough': {'Enabled': False}},
            'Name': 'TestConfiguration',
            'Tags': {},
            'TranscodeProfileName': '',
            'VideoContentSourceUrl': 'https://localhost/origin/hls/',
        }
        put_resp = deepcopy(get_resp)
        put_resp.update(put_params)
        stubber.add_response('put_playback_configuration', put_resp, put_params)

        # Do the deed - we expect to get the put response back
        with stubber:
            actual = update_playback_configuration(
                'TestConfiguration',
                emt_client,
                AdDecisionServerUrl='https://198.51.100.1:24601/ads/',
                AvailSuppression={'Mode': 'off'},
            )

        self.assertEqual(actual, put_resp)
