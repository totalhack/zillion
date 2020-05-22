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
def ds_config():
    config = copy.deepcopy(TEST_WH_CONFIG)
    return config["datasources"]["testdb1"]


@pytest.fixture(scope="function")
def adhoc_config():
    return copy.deepcopy(TEST_ADHOC_CONFIG)


@pytest.fixture(scope="function")
def wh(config):
    return Warehouse(config=config)


@pytest.fixture(scope="function")
def adhoc_ds(config):
    return get_adhoc_datasource()


@pytest.fixture(scope="function")
def mysql_ds_config():
    return load_datasource_config("test_mysql_ds_config.json")


@pytest.fixture(scope="function")
def mysql_ds(mysql_ds_config):
    return DataSource("mysql", config=mysql_ds_config)


@pytest.fixture(scope="function")
def mysql_wh(mysql_ds):
    return Warehouse(datasources=[mysql_ds])


@pytest.fixture(scope="function")
def postgres_ds_config():
    return load_datasource_config("test_postgres_ds_config.json")


@pytest.fixture(scope="function")
def postgres_ds(postgres_ds_config):
    return DataSource("postgres", config=postgres_ds_config)


@pytest.fixture(scope="function")
def postgres_wh(postgres_ds):
    return Warehouse(datasources=[postgres_ds])


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
