from unittest import TestCase
from unittest.mock import patch

from boto3 import client as boto3_client
from botocore.stub import Stubber

from boto3_helpers.sts import (
    assumed_role_session,
    assumed_role_client,
    assumed_role_resource,
)


@patch('boto3_helpers.sts.token_hex', lambda x: '00' * x)
class SecurityTokenServiceTests(TestCase):
    def setUp(self):
        self.access_key = 'not-an-access-key-id'
        self.secret_key = 'not-a-secret-access-key'
        self.token = 'not-a-session-token'
        self.external_id = 'not-an-external-id'
        self.target_role = 'arn:aws:iam::000000000000:role/test-role'
        self.target_region = 'test-region-1'

    def _get_stubber(self):
        # Set up the stubber
        sts_client = boto3_client('sts', region_name='not-a-region')
        stubber = Stubber(sts_client)

        assume_resp = {
            'Credentials': {
                'AccessKeyId': self.access_key,
                'SecretAccessKey': self.secret_key,
                'SessionToken': self.token,
                'Expiration': '2022-08-25T15:13:00Z',
            },
            'AssumedRoleUser': {
                'AssumedRoleId': 'not-an-assumed-role-id',
                'Arn': self.target_role,
            },
            'PackedPolicySize': 100,
        }
        assume_params = {
            'RoleArn': self.target_role,
            'RoleSessionName': '00000000',
            'ExternalId': self.external_id,
        }
        stubber.add_response('assume_role', assume_resp, assume_params)

        return sts_client, stubber

    def test_assumed_role_session(self):
        # Do the deed
        sts_client, stubber = self._get_stubber()
        with stubber:
            session = assumed_role_session(
                sts_client=sts_client,
                session_kwargs={'region_name': 'test-region-1'},
                RoleArn=self.target_role,
                ExternalId=self.external_id,
            )

        # The session should be created with the target region
        self.assertEqual(session.region_name, 'test-region-1')

        # And use the assumed role's temporary credentials
        creds = session.get_credentials()
        self.assertEqual(creds.access_key, self.access_key)
        self.assertEqual(creds.secret_key, self.secret_key)
        self.assertEqual(creds.token, self.token)

    def test_assumed_role_client(self):
        # Set up the stubber
        sts_client, stubber = self._get_stubber()
        with stubber:
            ssm_client = assumed_role_client(
                'ssm',
                sts_client=sts_client,
                client_kwargs={'region_name': self.target_region},
                RoleArn=self.target_role,
                ExternalId=self.external_id,
            )

        # The client should be created with the target region
        self.assertEqual(ssm_client.meta.region_name, self.target_region)

        # And use the assumed role's temporary credentials
        creds = ssm_client._request_signer._credentials
        self.assertEqual(creds.access_key, self.access_key)
        self.assertEqual(creds.secret_key, self.secret_key)
        self.assertEqual(creds.token, self.token)

    def test_assumed_role_resource(self):
        # Set up the stubber
        sts_client, stubber = self._get_stubber()
        with stubber:
            ddb_resource = assumed_role_resource(
                'dynamodb',
                sts_client=sts_client,
                resource_kwargs={'region_name': self.target_region},
                RoleArn=self.target_role,
                ExternalId=self.external_id,
            )

        # The client should be created with the target region
        ddb_client = ddb_resource.meta.client
        self.assertEqual(ddb_client.meta.region_name, self.target_region)

        # And use the assumed role's temporary credentials
        creds = ddb_client._request_signer._credentials
        self.assertEqual(creds.access_key, self.access_key)
        self.assertEqual(creds.secret_key, self.secret_key)
        self.assertEqual(creds.token, self.token)
