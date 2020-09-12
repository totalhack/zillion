import pytest

from .test_utils import *
from zillion.configs import zillion_config
from zillion.core import *
from zillion.datasource import *
from zillion.warehouse import Warehouse


def test_mysql_datasource(mysql_wh):
    metrics = ["cost", "clicks", "transactions"]
    dimensions = ["partner_name"]
    result = mysql_wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_mysql_table_data_url(mysql_ds_config, adhoc_config):
    adhoc_table_config = adhoc_config["datasources"]["test_adhoc_db"]["tables"][
        "main.dma_zip"
    ]
    mysql_ds_config["tables"]["zillion_test.dma_zip"] = adhoc_table_config
    ds = DataSource("mysql", config=mysql_ds_config)
    assert ds.has_table("zillion_test.dma_zip")


def test_mysql_ignore_table_data_url(mysql_ds_config, adhoc_config):
    adhoc_table_config = adhoc_config["datasources"]["test_adhoc_db"]["tables"][
        "main.dma_zip"
    ]
    adhoc_table_config["if_exists"] = IfExistsModes.IGNORE
    mysql_ds_config["tables"]["zillion_test.dma_zip"] = adhoc_table_config
    ds = DataSource("mysql", config=mysql_ds_config)
    assert ds.has_table("zillion_test.dma_zip")


def test_mysql_sequential_timeout(mysql_wh):
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        metrics = ["benchmark"]
        dimensions = ["partner_name"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = mysql_wh.execute(metrics, dimensions=dimensions)


def test_mysql_multithreaded_timeout(mysql_wh):
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
            DATASOURCE_QUERY_TIMEOUT=1e-1,
        )
    ):
        metrics = ["benchmark", "transactions"]
        dimensions = ["partner_name"]
        with pytest.raises(DataSourceQueryTimeoutException):
            result = mysql_wh.execute(metrics, dimensions=dimensions)


def test_mysql_date_dimension_conversions(mysql_wh):
    params = get_date_conversion_test_params()
    result = mysql_wh.execute(**params)
    assert result
    df = result.df.reset_index()
    row = df.iloc[0]
    info(df)
    for field, value in EXPECTED_DATE_CONVERSION_VALUES:
        assert row[field] == value


def test_mysql_where_criteria_conversions(mysql_wh):
    metrics = ["clicks"]
    dimensions = ["campaign_created_at"]
    for field, op, val in CRITERIA_CONVERSION_TESTS:
        print("criteria:", field, op, val)
        criteria = [("campaign_name", "=", "Campaign 2B"), (field, op, val)]
        result = wh_execute(mysql_wh, locals())
        assert result.df.index.any()
        assert len(result.df) == 1
        assert result.df["clicks"][0] == 85
