.PHONY: install test lint format clean build publish

install:
	pip install -e ".[dev]"
	pip install -r requirements-dev.txt

test:
	pytest tests/ --cov=salesforce_connector

lint:
	flake8 salesforce_connector tests
	black --check salesforce_connector tests
	mypy salesforce_connector

format:
	black salesforce_connector tests

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

publish: build
	twine upload dist/* 