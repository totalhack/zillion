import pytest

from .test_utils import *
from zillion.configs import zillion_config, load_datasource_config
from zillion.core import *
from zillion.datasource import *
from zillion.warehouse import Warehouse


def test_postgres_datasource(postgres_wh):
    metrics = ["cost", "clicks", "transactions"]
    dimensions = ["partner_name"]
    result = postgres_wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_postgres_sequential_timeout(postgres_wh):
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        metrics = ["cost"]
        dimensions = ["benchmark"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = postgres_wh.execute(metrics, dimensions=dimensions)
            info(result.df)


def test_postgres_multithreaded_timeout(postgres_wh):
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        metrics = ["cost"]
        dimensions = ["benchmark"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = postgres_wh.execute(metrics, dimensions=dimensions)


def test_postgres_date_conversions(postgres_wh):
    params = get_date_conversion_test_params()
    result = postgres_wh.execute(**params)
    assert result
    df = result.df.reset_index()
    row = df.iloc[0]
    info(df)
    for field, value in EXPECTED_DATE_CONVERSION_VALUES:
        assert row[field] == value
