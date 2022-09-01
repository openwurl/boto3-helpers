export AWS_DEFAULT_REGION = test-region-2
export AWS_ACCESS_KEY_ID = test-access-key-id
export AWS_SECRET_ACCESS_KEY = test-secret-access-key

.PHONY: requirements
requirements:
	python3 -m pip install -r requirements/development.txt

.PHONY: check
check:
	black --check .
	flake8 .

.PHONY: format
format:
	black .

.PHONY: coverage
coverage:
	coverage run -m unittest
	coverage report --show-missing --fail-under 99

.PHONY: test
test:
	python -m unittest -v ${tests}

.PHONY: docs
docs:
	sphinx-build -W -b html docs docs/_build/html

.PHONY: package
package:
	python setup.py sdist bdist_wheel
	twine check dist/*
