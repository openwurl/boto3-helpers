from json import loads

from boto3 import client as boto3_client
from botocore.awsrequest import AWSRequest


class SigV4RequestException(Exception):
    """Exception raised by :func:`sigv4_request` when an HTTP response indicates an
    error.
    """

    def __init__(self, status_code, content):
        self.status_code = status_code
        self.content = content


def sigv4_request(service, method, endpoint, client=None, operation_name=None):
    """Make a signed request to the AWS API and return the JSON payload.

    * *service* is the AWS API service code
    * *method* is an HTTP method like ``'GET'`` or ``'POST'``
    * *endpoint* is the target API endpoint. If you need to supply parameters, put
      supply them as a query string here (e.g., ``?MaxResults=1``)
    * *client* is a ``boto3.client`` instance for the same account and region as your
      target. If not given, is created with ``boto3.client('sts')``
    * *operation_name* is the name of the API operation to use when signing the request

    If the API response indicates an error,
    ``boto3_helpers.signed_requests.SigV4RequestException`` will be raised.

    This function is useful for accessing endpoints that aren't supported by ``boto3``.
    For example, ``botocore`` introduced support for the ``'scheduler'`` service in
    version 1.29.7. You could have used this function to interact with that API
    before this new version was available:

    .. code-block:: python

        from boto3_helpers.signed_requests import sigv4_request

        schedule_data = sigv4_request('scheduler', 'GET', 'schedules')

    Explanation:

    * The EventBridge Scheduler API Reference describes the ``ListSchedules`` API
      action.
    * The service code for EventBridge Scheduler is ``'scheduler'``.
    * The method for ``ListSchedules`` is ``'GET'``.
    * The endpoint is ``'/schedules'``. This function will strip off leading slashes.

    We could haved optionally supplied the ``operation_name`` as ``'ListSchedules'``.

    """
    client = client or boto3_client('sts')

    endpoint = endpoint.lstrip('/')
    url = f'https://{service}.{client.meta.region_name}.amazonaws.com/{endpoint}'

    request = AWSRequest(method=method, url=url)
    client._request_signer.sign(operation_name, request, signing_name=service)
    request.prepare()
    request.headers = dict(request.headers)

    resp = client._endpoint.http_session.send(request)
    if not (200 <= resp.status_code <= 299):
        raise SigV4RequestException(resp.status_code, resp.content)

    return loads(resp.content)
