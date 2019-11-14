import pytest

from .test_utils import *


def pytest_addoption(parser):
    parser.addoption(
        "--longrun",
        action="store_true",
        dest="longrun",
        default=False,
        help="enable longrun decorated tests",
    )


def pytest_configure(config):
    if not config.option.longrun:
        setattr(config.option, "markexpr", "not longrun")


@pytest.fixture(scope="function")
def config():
    return copy.deepcopy(TEST_CONFIG)


@pytest.fixture(scope="function")
def datasources():
    return init_datasources()


@pytest.fixture(scope="function")
def wh(datasources, config):
    ds_priority = [ds.name for ds in datasources]
    return Warehouse(datasources, config=config, ds_priority=ds_priority)
