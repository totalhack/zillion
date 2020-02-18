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
    return copy.deepcopy(TEST_WH_CONFIG)


@pytest.fixture(scope="function")
def adhoc_config():
    return copy.deepcopy(TEST_ADHOC_CONFIG)


@pytest.fixture(scope="function")
def wh(config):
    return Warehouse(config=config)


@pytest.fixture(scope="function")
def adhoc_ds(config):
    ds = get_adhoc_datasource()
    yield ds
    ds.clean_up()


@pytest.fixture
def pymysql_conn():
    conn = get_pymysql_conn()
    yield conn
    try:
        conn.close()
    except:
        pass


@pytest.fixture
def sqlalchemy_conn():
    conn = get_sqlalchemy_conn()
    yield conn
    try:
        conn.close()
    except:
        pass
