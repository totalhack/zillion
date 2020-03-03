import pytest

from tlbx import info, st

from .test_utils import *
from zillion.configs import zillion_config, load_datasource_config
from zillion.core import DataSourceQueryTimeoutException, DataSourceQueryModes
from zillion.datasource import *
from zillion.warehouse import Warehouse


def test_postgres_datasource():
    ds_config = load_datasource_config("test_postgres_ds_config.json")
    ds = DataSource.from_config("postgres", ds_config)
    wh = Warehouse(datasources=[ds])
    wh.print_info()
    metrics = ["cost", "clicks", "transactions"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_postgres_sequential_timeout():
    ds_config = load_datasource_config("test_postgres_ds_config.json")
    ds = DataSource.from_config("postgres", ds_config)
    wh = Warehouse(datasources=[ds])
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        metrics = ["cost"]
        dimensions = ["benchmark"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = wh.execute(metrics, dimensions=dimensions)


def test_postgres_multithreaded_timeout():
    ds_config = load_datasource_config("test_postgres_ds_config.json")
    ds = DataSource.from_config("postgres", ds_config)
    wh = Warehouse(datasources=[ds])
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        metrics = ["cost"]
        dimensions = ["benchmark"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = wh.execute(metrics, dimensions=dimensions)
