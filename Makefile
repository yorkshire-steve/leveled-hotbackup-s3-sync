SHELL:=/bin/bash -O globstar
.SHELLFLAGS = -ec
.PHONY: localstack localstack-down

install:
	poetry install --sync

install-poetry:
	python3.8 -m pip install --user --upgrade pip && \
	python3.8 -m pip install --user poetry==1.4.2 && \
	poetry self add "poetry-dynamic-versioning[plugin]"

update:
	poetry update

mypy:
	poetry run mypy .

pylint:
	poetry run pylint .

pylint-ci:
	poetry run pylint --output-format=parseable --score=no .

black-check:
	poetry run black . --check

isort-check:
	poetry run isort . -c

black:
	poetry run isort .
	poetry run black .

coverage-cleanup:
	rm -f .coverage* || true

coverage-ci-test:
	poetry run coverage run -m pytest --color=yes -v --junit-xml=./reports/junit/mesh.xml

coverage-report:
	@poetry run coverage report; \
	poetry run coverage xml;

coverage: coverage-cleanup coverage-test coverage-report

coverage-test:
	poetry run coverage run -m pytest

coverage-ci: coverage-cleanup coverage-ci-test coverage-report

clean:
	rm -rf ./dist || true
	rm -rf ./reports || true
	find . -type d -name '.mypy_cache' | xargs rm -rf || true
	find . -type d -name '.pytest_cache' | xargs rm -rf || true
	find . -type d -name '__pycache__' | xargs rm -rf || true
	find . -type f -name '.coverage' | xargs rm -rf || true

purge: clean
	rm -rf .venv || true

localstack:
	cd localstack && docker compose up -d

localstack-down:
	cd localstack && docker compose down --remove-orphans || true
