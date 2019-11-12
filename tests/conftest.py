import pytest

from .test_utils import *


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
