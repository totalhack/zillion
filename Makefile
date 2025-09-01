PY := $(shell which python)
ENV := $(abspath $(dir $(PY))/..)
PIP := $(ENV)/bin/pip
UV := $(ENV)/bin/uv

PACKAGE_NAME := zillion
TEST_ENV := /tmp/zillion_pip_test

VERSION = $(shell $(PY) -c "import re,sys;print(re.search(r'__version__\\s*=\\s*[\\\"\\']([^\\\"\\']+)[\\\"\\']', open('zillion/version.py').read()).group(1))")

all: develop

# bootstrap venv and tooling
bootstrap:
	python -m venv $(ENV)
	$(PIP) install -U pip setuptools wheel build twine uv

clean:
	rm -rf build dist *.egg-info pinned-requirements.txt

docs:
	cd docs && $(PY) build_markdown.py

deploy_docs:
	$(PIP) install -U mkdocs mkdocs-material mkdocs-material-extensions
	cd docs && mkdocs gh-deploy

lock:
	$(UV) lock

sync-dev:
	$(UV) sync --dev --active --extra dev --extra mysql --extra postgres --extra duckdb

sync-ci:
	$(UV) sync --locked --active

sync-runtime:
	$(UV) sync --locked --active

# build wheel/sdist via pyproject
build:
	$(PY) -m build

# export pinned requirements (optional, for pip-only CI/Docker)
requirements:
	$(UV) export --format requirements-txt -o pinned-requirements.txt

# install built wheel into env
install-wheel: build
	$(PIP) install --force-reinstall dist/$(PACKAGE_NAME)-$(VERSION)-py3-none-any.whl

uninstall:
	if $(PIP) freeze 2>&1 | grep -q "^$(PACKAGE_NAME)=="; then \
	  $(PIP) uninstall -y $(PACKAGE_NAME); \
	else \
	  echo "No installed package found!"; \
	fi

dist: clean build

upload:
	$(PY) -m twine upload dist/*

test_upload:
	$(PY) -m twine upload --repository-url "https://test.pypi.org/legacy/" dist/*

# create a clean test venv; if uv.lock exists, optionally use uv inside the test venv to sync
test_env:
	rm -rf $(TEST_ENV)
	$(PY) -m venv $(TEST_ENV)
	$(TEST_ENV)/bin/pip install -U pip
	if [ -f uv.lock ]; then \
	  $(TEST_ENV)/bin/pip install uv; \
	  $(TEST_ENV)/bin/uv sync --locked; \
	fi

# publish to PyPI then smoke-test install in a clean venv
pip: dist upload test_env
	sleep 30
	$(TEST_ENV)/bin/pip install -U $(PACKAGE_NAME)==$(VERSION)
	$(TEST_ENV)/bin/python -c "import $(PACKAGE_NAME)"

# publish to TestPyPI then smoke-test install from TestPyPI in a clean venv
test_pip: dist test_upload test_env
	sleep 30
	$(TEST_ENV)/bin/pip install -i "https://test.pypi.org/simple/" --extra-index-url "https://pypi.org/simple/" $(PACKAGE_NAME)==$(VERSION)
	$(TEST_ENV)/bin/python -c "import $(PACKAGE_NAME)"

.PHONY: all bootstrap clean docs deploy_docs develop build export-pinned install uninstall dist upload test_upload test_env pip test_pip
