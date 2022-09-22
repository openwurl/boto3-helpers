from boto3 import client as boto3_client


class ARN:
    __slots__ = [
        'partition',
        'service',
        'region',
        'account_id',
        'resource_type',
        'resource_id',
        'resource_separator',
    ]

    def __init__(
        self, partition, service, region, account_id, *args, resource_separator='/'
    ):
        self.partition = partition
        self.service = service
        self.region = region
        self.account_id = account_id
        if len(args) == 1:
            self.resource_type = ''
            self.resource_id = args[0]
        elif len(args) == 2:
            self.resource_type = args[0]
            self.resource_id = args[1]
        else:
            raise ValueError('Invalid resource')
        self.resource_separator = resource_separator

    def __str__(self):
        if not self.resource_type:
            return (
                f'arn:{self.partition}:{self.service}:{self.region}:{self.account_id}:'
                f'{self.resource_id}'
            )
        else:
            return (
                f'arn:{self.partition}:{self.service}:{self.region}:{self.account_id}:'
                f'{self.resource_type}{self.resource_separator}{self.resource_id}'
            )

    @classmethod
    def from_existing(cls, existing):
        parts = existing.split(':')
        resource_separator = '/'
        if len(parts) == 6:
            if '/' in parts[5]:
                parts[5:] = parts[5].split('/', 1)
        elif len(parts) == 7:
            resource_separator = ':'

        return cls(*parts[1:], resource_separator=resource_separator)


def construct_arn(existing=None, *, sts_client=None, **kwargs):
    """Construct an ARN from an existing one.

    * *existing* is used as a template. If not provided, one will be
      derived from your IAM user or role.
    * *sts_client* is a ``boto3.client('sts')`` instance. If not given,
      is created with ``boto3.client('sts')``. This will only be used
      if *existing* is not supplied.
    * *kwargs* can include any of the following: ``partition``,
      ``service``, ``region``, ``account_id``,
      ``resource_type``, ``resource_separator``, ``resource_id``.

    Some ARNs end with ``resource_id``. You can construct these by
    supplying ``resource_type=''`` and ``resource_id='DesiredID'``.

    Other ARNs end with ``resource_type:resource_id``. You can
    construct these by supplying ``resource_type='DesiredType'``,
    ``resource_separator=':'``, and ``resource_id='DesiredID'``

    Still other ARNs end with ``resource_type/resource_id``. You can
    construct these by supplying ``resource_type='DesiredType'``,
    ``resource_separator='/'``, and ``resource_id='DesiredID'``

    Converting one format to another:

    .. code-block:: python

        from boto3_helpers.arn import construct_arn

        existing = 'arn:aws:dynamodb:us-east-2:00000000:table/demo'
        new = construct_arn(
            existing,
            service='lambda',
            resource_type='function',
            resource_separator=':',
            resource_id='example'
        )
        print(new)  # arn:aws:lambda:us-east-2:00000000:function:example

    Starting from your IAM user or role:

    .. code-block:: python

        from boto3_helpers.arn import construct_arn

        new = construct_arn(
            service='dynamodb',
            resource_type='table',
            resource_id='demo'
        )
        print(new)  # arn:aws:dynamodb:us-east-2:00000000:table/demo
    """
    if existing is None:
        sts_client = sts_client or boto3_client('sts')
        existing = sts_client.get_caller_identity()['Arn']
        kwargs.setdefault('region', sts_client.meta.region_name)

    obj = ARN.from_existing(existing)
    for k, v in kwargs.items():
        setattr(obj, k, v)

    return str(obj)
