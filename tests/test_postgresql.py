import pytest

from .test_utils import *
from zillion.configs import zillion_config, load_datasource_config
from zillion.core import *
from zillion.datasource import *
from zillion.warehouse import Warehouse


def test_postgresql_datasource(postgresql_wh):
    metrics = ["cost", "clicks", "transactions"]
    dimensions = ["partner_name"]
    result = postgresql_wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_postgresql_sequential_timeout(postgresql_wh):
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        metrics = ["cost"]
        dimensions = ["benchmark"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = postgresql_wh.execute(metrics, dimensions=dimensions)
            info(result.df)


def test_postgresql_multithreaded_timeout(postgresql_wh):
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        metrics = ["cost"]
        dimensions = ["benchmark"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = postgresql_wh.execute(metrics, dimensions=dimensions)


def test_postgresql_date_dimension_conversions(postgresql_wh):
    params = get_date_conversion_test_params()
    result = postgresql_wh.execute(**params)
    assert result
    df = result.df.reset_index()
    row = df.iloc[0]
    info(df)
    for field, value in EXPECTED_DATE_CONVERSION_VALUES:
        assert row[field] == value


def test_postgresql_where_criteria_conversions(postgresql_wh):
    metrics = ["clicks"]
    dimensions = ["campaign_created_at"]
    for field, op, val in CRITERIA_CONVERSION_TESTS:
        print("criteria:", field, op, val)
        criteria = [("campaign_name", "=", "Campaign 2B"), (field, op, val)]
        result = wh_execute(postgresql_wh, locals())
        assert result.df.index.any()
        assert len(result.df) == 1
        assert result.df["clicks"][0] == 85
