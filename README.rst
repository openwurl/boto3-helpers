boto3-helpers
=============

This is the documentation for the ``boto3_helpers`` package, a Python library that
aims to provide a smoother interface for some of the functions in the AWS
`boto3 <https://github.com/boto/boto3>`_ package.
 
You know how to install it:

.. code-block:: sh

    pip install boto3-helpers

Have you ever seen anybody make this mistake?

.. code-block:: python

    from boto3 import resource as boto3_resource
    from boto3.dynamodb.conditions import Key, Attr
    
    # Don't do this; you'll miss out if there is more than one page
    ddb_table = boto3.resource('dynamodb').Table('example-table')
    resp = ddb_table.query(
        KeyConditionExpression=Key('username').eq('johndoe')
    )
    for item in resp.get('Items', []):
        print(item)

What they should have done is this:

.. code-block:: python

    from boto3 import resource as boto3_resource
    from boto3.dynamodb.conditions import Key, Attr

    # Loop through all the pages
    ddb_table = boto3.resource('dynamodb').Table('example-table')
    kwargs = {'KeyConditionExpression': Key('username').eq('johndoe')}
    while True:
        resp = ddb_table.query(**kwargs)
        for item in resp.get('Items', []):
            print(item)
        if 'LastEvaluatedKey' not in resp:
            break
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']

With ``boto3_helpers``, you can do the right thing more easily:

.. code-block:: python

        from boto3 import resource as boto3_resource
        from boto3.dynamodb.conditions import Key, Attr
        from boto3_helpers.dynamodb import query_table

        ddb_table = boto3.resource('dynamodb').Table('example-table')
        for item in query_table(
            ddb_table, KeyConditionExpression=Key('username').eq('johndoe')
        ):
            print(item)

This package provides helper functions for several similar actions in AWS, such as:

* Paging through S3 listings
* Updating items in DynamoDB
* Assuming roles with STS

See the `full documentation <git branch --set-upstream-to=origin/>`_.
