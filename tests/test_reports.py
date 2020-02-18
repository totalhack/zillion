import pytest

from tlbx import dbg, info, st

from .test_utils import *
from zillion.configs import zillion_config
from zillion.core import (
    UnsupportedGrainException,
    InvalidFieldException,
    ReportException,
    WarehouseException,
    DataSourceQueryTimeoutException,
    DisallowedSQLException,
    DataSourceQueryModes,
)
from zillion.field import Metric
from zillion.report import ROLLUP_INDEX_LABEL, ROLLUP_TOTALS


def test_basic_report(wh):
    metrics = ["revenue", "sales_quantity"]
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 11)]
    rollup = ROLLUP_TOTALS
    result = wh.execute(
        metrics,
        dimensions=dimensions,
        criteria=criteria,
        row_filters=row_filters,
        rollup=rollup,
    )
    assert result
    info(result.df)


def test_report_sequential_timeout(wh):
    metrics = ["adhoc_metric", "revenue"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource(size=1e5)
    try:
        with update_zillion_config(
            dict(
                DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
                DATASOURCE_QUERY_TIMEOUT=1e-3,
            )
        ):
            with pytest.raises(DataSourceQueryTimeoutException):
                result = wh.execute(
                    metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
                )
    finally:
        adhoc_ds.clean_up()


def test_report_multithreaded_timeout(wh):
    metrics = ["adhoc_metric", "revenue"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource(size=1e5)
    try:
        with update_zillion_config(
            dict(
                DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
                DATASOURCE_QUERY_TIMEOUT=1e-3,
            )
        ):
            with pytest.raises(DataSourceQueryTimeoutException):
                result = wh.execute(
                    metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
                )
    finally:
        adhoc_ds.clean_up()


def test_impossible_report(wh):
    metrics = ["leads"]
    dimensions = ["sale_id"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    with pytest.raises(UnsupportedGrainException):
        result = wh.execute(metrics, dimensions=dimensions, criteria=criteria)


def test_report_pivot(wh):
    metrics = ["revenue", "sales_quantity"]
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 11)]
    rollup = ROLLUP_TOTALS
    pivot = ["partner_name"]
    result = wh.execute(
        metrics,
        dimensions=dimensions,
        criteria=criteria,
        row_filters=row_filters,
        rollup=rollup,
        pivot=pivot,
    )
    assert result
    info(result.df)


def test_report_moving_average_metric(wh):
    metrics = ["revenue", "revenue_ma_5"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 8)]
    rollup = ROLLUP_TOTALS
    result = wh.execute(
        metrics,
        dimensions=dimensions,
        criteria=criteria,
        row_filters=row_filters,
        rollup=rollup,
    )
    assert result
    info(result.df)


def test_report_moving_average_adhoc_metric(wh):
    metrics = [
        "revenue",
        {"formula": "{revenue}", "technical": "MA-5", "name": "my_revenue_ma_5"},
    ]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 8)]
    rollup = ROLLUP_TOTALS
    result = wh.execute(
        metrics,
        dimensions=dimensions,
        criteria=criteria,
        row_filters=row_filters,
        rollup=rollup,
    )
    assert result
    info(result.df)


def test_report_moving_average_formula_metric(wh):
    metrics = ["revenue", "rpl", "rpl_ma_5"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 8)]
    rollup = ROLLUP_TOTALS
    result = wh.execute(
        metrics,
        dimensions=dimensions,
        criteria=criteria,
        row_filters=row_filters,
        rollup=rollup,
    )
    assert result
    info(result.df)


def test_report_cum_sum_metric(wh):
    metrics = ["revenue", "revenue_sum_5"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = None
    rollup = ROLLUP_TOTALS
    result = wh.execute(
        metrics,
        dimensions=dimensions,
        criteria=criteria,
        row_filters=row_filters,
        rollup=rollup,
    )
    assert result
    info(result.df)


def test_report_bollinger_metric(wh):
    metrics = ["revenue", "revenue_boll_5"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = None
    rollup = ROLLUP_TOTALS
    result = wh.execute(
        metrics,
        dimensions=dimensions,
        criteria=criteria,
        row_filters=row_filters,
        rollup=rollup,
    )
    assert result
    info(result.df)


def test_report_no_dimensions(wh):
    metrics = ["revenue", "sales_quantity"]
    criteria = [("campaign_name", "=", "Campaign 2B")]
    result = wh.execute(metrics, criteria=criteria)
    assert result
    info(result.df)


def test_report_no_metrics(wh):
    metrics = []
    dimensions = ["partner_name", "campaign_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_null_criteria(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name"]
    criteria = [("campaign_name", "!=", None)]
    result = wh.execute(metrics, dimensions=dimensions, criteria=criteria)
    assert result
    info(result.df)


def test_report_count_metric(wh):
    metrics = ["leads"]
    dimensions = ["campaign_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_alias_metric(wh):
    metrics = ["revenue_avg"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_alias_dimension(wh):
    metrics = ["revenue"]
    dimensions = ["lead_id"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_multiple_queries(wh):
    metrics = ["revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_formula_metric(wh):
    metrics = ["rpl", "revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_nested_formula_metric(wh):
    metrics = ["rpl_squared", "rpl_unsquared", "rpl", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_ds_dimension_formula(wh):
    metrics = ["sales"]
    dimensions = ["revenue_decile"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_ds_metric_formula(wh):
    metrics = ["revenue", "revenue_ds"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_non_existent_metric(wh):
    metrics = ["sales1234"]
    dimensions = ["campaign_id"]
    result = False
    with pytest.raises(InvalidFieldException):
        wh.execute(metrics, dimensions=dimensions)


def test_report_weighted_formula_metric(wh):
    metrics = ["rpl_weighted", "rpl", "sales_quantity", "revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_weighted_ds_metric_formula(wh):
    metrics = ["revenue_avg", "revenue_avg_ds_weighted"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_weighted_metric(wh):
    metrics = ["sales_quantity", "revenue_avg", "revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_weighted_metric_with_rollup(wh):
    metrics = ["sales_quantity", "revenue_avg", "leads"]
    dimensions = ["partner_name"]
    rollup = ROLLUP_TOTALS
    result = wh.execute(metrics, dimensions=dimensions, rollup=rollup)
    assert result
    info(result.df)


def test_report_weighted_metric_with_multi_rollup(wh):
    metrics = ["sales_quantity", "revenue_avg", "leads"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]
    rollup = 2
    result = wh.execute(metrics, dimensions=dimensions, rollup=rollup)
    assert result
    info(result.df)


def test_report_multi_dimension(wh):
    metrics = ["leads", "sales"]
    dimensions = ["partner_name", "lead_id"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_rollup(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = ROLLUP_TOTALS
    result = wh.execute(
        metrics, dimensions=dimensions, criteria=criteria, rollup=rollup
    )
    info(result.df)
    revenue = result.rollup_rows().iloc[-1]["revenue"]
    revenue_sum = result.non_rollup_rows().sum()["revenue"]
    assert revenue == revenue_sum


def test_report_multi_rollup(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = 3
    result = wh.execute(
        metrics, dimensions=dimensions, criteria=criteria, rollup=rollup
    )
    info(result.df)
    revenue = result.rollup_rows().iloc[-1]["revenue"]
    revenue_sum = result.non_rollup_rows().sum()["revenue"]
    assert revenue == revenue_sum


def test_report_multi_rollup_pivot(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = 3
    pivot = ["campaign_name"]
    result = wh.execute(
        metrics, dimensions=dimensions, criteria=criteria, rollup=rollup, pivot=pivot
    )
    assert result
    info(result.df)


def test_report_adhoc_dimension(wh):
    metrics = ["leads", "sales"]
    dimensions = [
        "partner_name",
        "lead_id",
        {"formula": "{lead_id} > 3", "name": "testdim"},
    ]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_adhoc_metric(wh):
    metrics = ["revenue", {"formula": "{revenue} > 3*{lead_id}", "name": "testmetric"}]
    dimensions = ["partner_name", "lead_id"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_report_date_conversion(wh):
    metrics = ["revenue"]
    dimensions = ["datetime", "hour_of_day"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    result = wh.execute(metrics, dimensions=dimensions, criteria=criteria)
    assert result
    info(result.df)


def test_report_datasource_priority(wh):
    metrics = ["revenue", "leads", "sales"]
    dimensions = ["partner_name"]
    report = Report(wh, metrics=metrics, dimensions=dimensions)
    assert report.queries[0].get_datasource_name() == "testdb1"


def test_report_multi_datasource(wh):
    metrics = ["revenue", "leads", "sales", "revenue_avg"]
    dimensions = ["partner_name"]
    report = Report(wh, metrics=metrics, dimensions=dimensions)
    assert len(report.queries) == 2
    result = report.execute()
    assert result
    info(result.df)


def test_report_save_and_load(wh):
    metrics = ["revenue", "leads", "sales"]
    dimensions = ["partner_name"]
    report = Report(wh, metrics=metrics, dimensions=dimensions)
    report_id = report.save()
    try:
        result = wh.execute_id(report_id)
        assert result
        info(result.df)
    finally:
        wh.delete_report(report_id)


def test_report_save_and_load_adhoc_metric(wh):
    metrics = ["revenue", {"formula": "{revenue} > 3*{lead_id}", "name": "testmetric"}]
    dimensions = ["partner_name", "lead_id"]
    report = Report(wh, metrics=metrics, dimensions=dimensions)
    report_id = report.save()
    try:
        result = wh.execute_id(report_id)
        assert result
        info(result.df)
    finally:
        wh.delete_report(report_id)


def test_report_adhoc_datasource(wh, adhoc_ds):
    metrics = ["revenue", "adhoc_metric"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds])
    assert result
    info(result.df)


def test_report_save_and_load_adhoc_datasource(wh, adhoc_ds):
    metrics = ["revenue", "leads", "adhoc_metric"]
    dimensions = ["partner_name"]
    report = wh.save_report(
        metrics=metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
    )
    report_id = report.save()
    try:
        result = wh.execute_id(report_id, adhoc_datasources=[adhoc_ds])
        assert result
        info(result.df)
    finally:
        wh.delete_report(report_id)


def test_report_missing_adhoc_datasource_save_and_load(wh, adhoc_ds):
    metrics = ["revenue", "leads", "adhoc_metric"]
    dimensions = ["partner_name"]
    report = wh.save_report(
        metrics=metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
    )
    report_id = report.save()
    try:
        with pytest.raises(ReportException):
            result = wh.execute_id(report_id)
    finally:
        wh.delete_report(report_id)


def test_report_invalid_adhoc_datasource(wh, adhoc_ds):
    metrics = ["revenue", "adhoc_metric"]
    dimensions = ["partner_name"]
    metric = Metric("campaign_name", "String(32)")  # This is a dimension in other DSes
    adhoc_ds.add_metric(metric)
    with pytest.raises(WarehouseException):
        result = wh.execute(
            metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
        )


def test_regular_datasource_adhoc(config):
    ds1 = DataSource.from_config("testdb1", config["datasources"]["testdb1"])
    ds2 = DataSource.from_config("testdb2", config["datasources"]["testdb2"])
    wh = Warehouse(datasources=[ds1])
    metrics = ["leads", "sales", "aggr_sales"]
    dimensions = ["campaign_name"]
    result = wh.execute(metrics, dimensions=dimensions, adhoc_datasources=[ds2])
    assert result
    info(result.df)


def test_only_adhoc_datasource(adhoc_ds):
    wh = Warehouse(datasources=[adhoc_ds])
    metrics = ["adhoc_metric"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result
    info(result.df)


def test_ds_metric_formula_sql_injection(config):
    # example = r"""I don't like "special" ch;ars ¯\_(ツ)_/¯"""
    table_config = config["datasources"]["testdb1"]["tables"]["sales"]
    column_config = table_config["columns"]["revenue"]
    column_config["fields"].append(
        {
            "name": "ds_injection",
            "ds_formula": "IFNULL(sales.revenue, 0);select * from sales",
        }
    )
    metrics = ["revenue", "ds_injection"]
    dimensions = ["partner_name"]
    wh = Warehouse(config=config)
    with pytest.raises(DisallowedSQLException):
        result = wh.execute(metrics, dimensions=dimensions)


def test_ds_dim_formula_sql_injection(config):
    # example = r"""I don't like "special" ch;ars ¯\_(ツ)_/¯"""
    table_config = config["datasources"]["testdb1"]["tables"]["sales"]
    column_config = table_config["columns"]["lead_id"]
    column_config["fields"].append(
        {
            "name": "ds_injection",
            "ds_formula": "IF(sales.lead_id > 0, 1, 0);select * from sales",
        }
    )
    metrics = ["revenue"]
    dimensions = ["partner_name", "ds_injection"]
    wh = Warehouse(config=config)
    with pytest.raises(DisallowedSQLException):
        result = wh.execute(metrics, dimensions=dimensions)


def test_metric_formula_sql_injection(config):
    config["metrics"].append(
        dict(
            name="rpl_injection",
            aggregation="avg",
            rounding=2,
            formula="{revenue}/{leads};select * from leads",
        )
    )

    metrics = ["revenue", "rpl_injection"]
    dimensions = ["partner_name"]
    wh = Warehouse(config=config)
    with pytest.raises(DisallowedSQLException):
        result = wh.execute(metrics, dimensions=dimensions)


def test_weighting_metric_sql_injection(config):
    config["metrics"].append(
        dict(
            name="rpl_injection",
            aggregation="avg",
            rounding=2,
            formula="{revenue}/{leads}",
            weighting_metric="sales_quantity;select * from leads",
        )
    )

    metrics = ["revenue", "rpl_injection"]
    dimensions = ["partner_name"]
    wh = Warehouse(config=config)
    with pytest.raises(InvalidFieldException):
        result = wh.execute(metrics, dimensions=dimensions)


def test_adhoc_metric_sql_injection(wh):
    metrics = [
        "revenue",
        {"formula": "{revenue};select * from leads", "name": "rev_injection"},
    ]
    dimensions = ["partner_name"]
    with pytest.raises(DisallowedSQLException):
        result = wh.execute(metrics, dimensions=dimensions)


def test_adhoc_dimension_sql_injection(wh):
    metrics = ["leads", "sales"]
    dimensions = [
        "partner_name",
        {"formula": "{lead_id} > 3;drop table leads", "name": "testdim"},
    ]
    with pytest.raises(DisallowedSQLException):
        result = wh.execute(metrics, dimensions=dimensions)


def test_criteria_sql_injection(wh):
    metrics = ["leads", "sales"]
    dimensions = ["campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B';select * from leads --")]
    result = wh.execute(metrics, dimensions=dimensions, criteria=criteria)

    criteria = [("select * from leads", "!=", "Campaign 2B")]
    with pytest.raises(UnsupportedGrainException):
        result = wh.execute(metrics, dimensions=dimensions, criteria=criteria)

    criteria = [("campaign_name", "select * from leads", "Campaign 2B")]
    with pytest.raises(AssertionError):
        result = wh.execute(metrics, dimensions=dimensions, criteria=criteria)


def test_row_filter_sql_injection(wh):
    metrics = ["leads", "revenue"]
    dimensions = ["partner_name"]
    row_filters = [("revenue", ">", "11;select * from leads")]
    with pytest.raises(SyntaxError):
        result = wh.execute(metrics, dimensions=dimensions, row_filters=row_filters)


def test_pivot_sql_injection(wh):
    metrics = ["leads", "revenue"]
    dimensions = ["partner_name", "campaign_name"]
    pivot = ["partner_name;select * from leads"]
    with pytest.raises(AssertionError):
        result = wh.execute(metrics, dimensions=dimensions, pivot=pivot)


def test_metric_name_sql_injection(config):
    config["metrics"].append(
        dict(
            name="select * from leads",
            aggregation="avg",
            rounding=2,
            formula="{revenue}/{leads}",
        )
    )
    from marshmallow.exceptions import ValidationError

    with pytest.raises(ValidationError):
        wh = Warehouse(config=config)


def test_dimension_name_sql_injection(config):
    config["dimensions"].append(dict(name="select * from leads", type="String(64)"))
    from marshmallow.exceptions import ValidationError

    with pytest.raises(ValidationError):
        wh = Warehouse(config=config)


def test_type_conversion_prefix_sql_injection(config):
    table_config = config["datasources"]["testdb1"]["tables"]["sales"]
    table_config["columns"]["created_at"][
        "type_conversion_prefix"
    ] = "select * from sales;--"
    metrics = ["sales"]
    dimensions = ["partner_name"]
    from marshmallow.exceptions import ValidationError

    with pytest.raises(ValidationError):
        wh = Warehouse(config=config)


def test_table_name_sql_injection(config):
    tables = config["datasources"]["testdb1"]["tables"]
    del tables["sales"]
    # Since this table doesn't actually match a table name it shouldn't ever
    # become a part of the warehouse.
    tables["select * from leads;--"] = {
        "type": "metric",
        "create_fields": True,
        "columns": {
            "id": {
                "fields": [{"name": "sales", "ds_formula": "COUNT(DISTINCT sales.id)"}]
            },
            "lead_id": {"fields": ["lead_id"]},
        },
    }
    metrics = ["sales"]
    dimensions = ["lead_id"]
    wh = Warehouse(config=config)
    with pytest.raises(UnsupportedGrainException):
        result = wh.execute(metrics, dimensions=dimensions)
