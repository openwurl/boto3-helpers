from datetime import datetime, timezone, timedelta
from copy import deepcopy
from unittest import TestCase

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.cloudwatch import yield_metric_data


class CloudWatchTests(TestCase):
    def test_yield_metric_data(self):
        # Sample metric to fetch
        namespace = 'MediaLive'
        metric_name = 'FillMsec'
        dimension_map = {'ChannelId': '24601', 'Pipeline': '0'}
        period = 60
        stat = 'Maximum'
        start_time = datetime(2022, 9, 18, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2022, 9, 18, 0, 5, 0, tzinfo=timezone.utc)
        label_options = {'Timezone': '+0000'}

        # Set up the stubber
        cw_client = boto3_client('cloudwatch', region_name='not-a-region')
        stubber = Stubber(cw_client)

        # Page 1
        page_1_params = {
            'MetricDataQueries': [
                {
                    'Id': 'query0',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': namespace,
                            'MetricName': metric_name,
                            'Dimensions': [
                                {'Name': 'ChannelId', 'Value': '24601'},
                                {'Name': 'Pipeline', 'Value': '0'},
                            ],
                        },
                        'Period': period,
                        'Stat': stat,
                    },
                },
            ],
            'StartTime': start_time,
            'EndTime': end_time,
            'ScanBy': 'TimestampAscending',
            'LabelOptions': label_options,
        }
        page_1_resp = {
            'MetricDataResults': [
                {
                    'Id': 'query0',
                    'Label': metric_name,
                    'Timestamps': [
                        start_time + timedelta(seconds=x) for x in range(0, 180, 60)
                    ],
                    'Values': [0.0, 0.0, 20000.0],
                    'StatusCode': 'PartialData',
                },
            ],
            'NextToken': 'test-token',
        }
        stubber.add_response('get_metric_data', page_1_resp, page_1_params)

        # Page 2
        page_2_params = deepcopy(page_1_params)
        page_2_params['NextToken'] = 'test-token'

        page_2_resp = {
            'MetricDataResults': [
                {
                    'Id': 'query0',
                    'Label': metric_name,
                    'Timestamps': [
                        start_time + timedelta(seconds=x) for x in range(180, 300, 60)
                    ],
                    'Values': [60000.0, 30000.0],
                    'StatusCode': 'Complete',
                },
            ],
        }
        stubber.add_response('get_metric_data', page_2_resp, page_2_params)

        # Do the deed - we expect to get the put response back
        with stubber:
            actual = list(
                yield_metric_data(
                    namespace,
                    metric_name,
                    dimension_map,
                    period,
                    stat,
                    start_time,
                    end_time,
                    cw_client=cw_client,
                    LabelOptions=label_options,
                )
            )
        expected = [
            (start_time + timedelta(seconds=0), 0.0),
            (start_time + timedelta(seconds=60), 0.0),
            (start_time + timedelta(seconds=120), 20000.0),
            (start_time + timedelta(seconds=180), 60000.0),
            (start_time + timedelta(seconds=240), 30000.0),
        ]
        self.assertEqual(actual, expected)
