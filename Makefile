PACKAGE := zillion
ENV := /usr
PIP := $(ENV)/bin/pip
SETUP := $(ENV)/bin/python setup.py
VERSION := $(shell echo `date +%Y%m%d%H%M%S`)

clean:
	rm -rf build dist *.egg-info

develop:
	$(PIP) install -U -e ./ --no-binary ":all:"

install:
	$(SETUP) bdist_wheel egg_info --tag-build '.$(VERSION)'
	$(PIP) install dist/$(PACKAGE)-0.0.$(VERSION)-py3-none-any.whl

uninstall:
	if ($(PIP) freeze 2>&1 | grep $(PACKAGE)); \
		then $(PIP) uninstall $(PACKAGE) --yes; \
	else \
		echo 'Package not installed'; \
	fi
