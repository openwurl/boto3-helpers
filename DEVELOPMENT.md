## Development

Use a Python virtual environment to run code code.
* If the `venv/` directory exists, activate it with this command: `source venv/bin/activate`
* If it doesn't, create it first: `python3.13 -m venv venv`
* To install dependencies, run this command after activating the virtual environment: `make requirements`
* Before committing code changes, run the formatter with this command: `make format`
* Also run the linter with this command: `make check`
* Finally, run the unit test suite with this command: `make test`

When writing Python code, channel the spirit of Python luminaries like Raymond Hettinger.
If doubts arise, consult [Google's style guide](https://google.github.io/styleguide/pyguide.html).

## Testing

It's important that tests stay offline. They should not attempt to make connections outside the local machine or access remote APIs.

To prevent tests from accessing the Internet, `unittest.patch`:
```
@patch('boto3_helpers.example_module.api_request', autospec=True)
def test_example_module(self, mock_api_request):
    mock_api_request.status_code = 200
    ...
```

For `boto3` actions, use the [Stubber](https://docs.aws.amazon.com/botocore/latest/reference/stubber.html) where possible.
