import pytest

from tlbx import info, st

from .test_utils import *
from zillion.configs import zillion_config, load_datasource_config
from zillion.core import DataSourceQueryTimeoutException, DataSourceQueryModes
from zillion.datasource import *
from zillion.warehouse import Warehouse


def test_mysql_datasource():
    mysql_config = load_datasource_config("test_mysql_ds_config.json")
    ds = DataSource.from_config("mysql", mysql_config)
    wh = Warehouse(datasources=[ds])
    metrics = ["cost", "clicks", "transactions"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_mysql_sequential_timeout():
    mysql_config = load_datasource_config("test_mysql_ds_config.json")
    ds = DataSource.from_config("mysql", mysql_config)
    wh = Warehouse(datasources=[ds])
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        metrics = ["benchmark"]
        dimensions = ["partner_name"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = wh.execute(metrics, dimensions=dimensions)


def test_mysql_multithreaded_timeout():
    mysql_config = load_datasource_config("test_mysql_ds_config.json")
    ds = DataSource.from_config("mysql", mysql_config)
    wh = Warehouse(datasources=[ds])
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
            DATASOURCE_QUERY_TIMEOUT=1e-1,
        )
    ):
        metrics = ["benchmark", "transactions"]
        dimensions = ["partner_name"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = wh.execute(metrics, dimensions=dimensions)
