import pytest

from .test_utils import *
from zillion.core import *
from zillion.datasource import *


@pytest.mark.skipif(not pytest.importorskip("duckdb"), reason="duckdb not installed")
def test_duckdb_datasource(duckdb_wh):
    metrics = ["revenue", "leads"]
    dimensions = ["partner_name"]
    result = duckdb_wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


# TODO no support to kill queries in duckdb yet!

# def test_duckdb_sequential_timeout(duckdb_wh):
#     with update_zillion_config(
#         dict(
#             DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
#             DATASOURCE_QUERY_TIMEOUT=1e-2,
#         )
#     ):
#         metrics = ["leads"]
#         dimensions = ["benchmark"]
#         with pytest.raises(DataSourceQueryTimeoutException):
#             duckdb_wh.execute(metrics, dimensions=dimensions)


# def test_duckdb_multithreaded_timeout(duckdb_wh):
#     with update_zillion_config(
#         dict(
#             DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
#             DATASOURCE_QUERY_TIMEOUT=1e-2,
#         )
#     ):
#         metrics = ["cost"]
#         dimensions = ["benchmark"]
#         with pytest.raises(DataSourceQueryTimeoutException):
#             duckdb_wh.execute(metrics, dimensions=dimensions)


@pytest.mark.skipif(not pytest.importorskip("duckdb"), reason="duckdb not installed")
def test_duckdb_date_dimension_conversions(duckdb_wh):
    params = get_date_conversion_test_params()
    result = duckdb_wh.execute(**params)
    assert result
    df = result.df.reset_index()
    row = df.iloc[0]
    info(df)
    for field, value in EXPECTED_DATE_CONVERSION_VALUES:
        print(f"Checking {field} = {value}")
        assert row[field] == value


@pytest.mark.skipif(not pytest.importorskip("duckdb"), reason="duckdb not installed")
def test_duckdb_where_criteria_conversions(duckdb_wh):
    metrics = ["leads"]
    dimensions = ["campaign_created_at"]
    for field, op, val in CRITERIA_CONVERSION_TESTS:
        print("criteria:", field, op, val)
        criteria = [("campaign_name", "=", "Campaign 2B"), (field, op, val)]
        result = wh_execute(duckdb_wh, locals())
        assert result.df.index.any()
        assert len(result.df) == 1
        assert result.df["leads"][0] == 1
