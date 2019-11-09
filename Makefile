ENV := $(HOME)/env/zillion
TEST_ENV := /tmp/zillion_pip_test/
PACKAGE_NAME := 'zillion'
VERSION := $(shell python setup.py --version)
EGG_OPTIONS := egg_info
PIP_CMD := $(ENV)/bin/pip
SETUP_CMD := $(ENV)/bin/python setup.py

all: install

clean:
	rm -rf build dist *.egg-info

develop:
	$(PIP_CMD) install -U -e ./ --no-binary ":all:"

install:
	$(SETUP_CMD) bdist_wheel $(EGG_OPTIONS)
	$(PIP_CMD) install -U dist/$(PACKAGE_NAME)-$(VERSION)-py3-none-any.whl

uninstall:
	if ($(PIP_CMD) freeze 2>&1 | grep $(PACKAGE_NAME)); \
		then $(PIP_CMD) uninstall $(PACKAGE_NAME) --yes; \
	else \
		echo 'No installed package found!'; \
	fi

dist:
	$(MAKE) clean
	$(SETUP_CMD) sdist bdist_wheel

upload:
	$(ENV)/bin/python -m twine upload dist/*

test_upload:
	$(ENV)/bin/python -m twine upload --repository-url "https://test.pypi.org/legacy/" dist/*

test_env:
	rm -rf $(TEST_ENV)
	mkdir $(TEST_ENV)
	$(ENV)/bin/python	-m venv $(TEST_ENV)
	$(TEST_ENV)/bin/pip install -U pip

pip:
	$(MAKE) dist
	$(MAKE) upload
	$(MAKE) test_env
	sleep 30
	$(TEST_ENV)/bin/pip install -U zillion==$(VERSION)
	$(TEST_ENV)/bin/python -c "import zillion"

test_pip:
	$(MAKE) dist
	$(MAKE) test_upload
	$(MAKE) test_env
	sleep 30
	$(TEST_ENV)/bin/pip install -i "https://test.pypi.org/simple/" --extra-index-url "https://pypi.org/simple/" zillion==$(VERSION)
	$(TEST_ENV)/bin/python -c "import zillion"

.PHONY: dist clean test_env
