import pytest

from tlbx import dbg, st

from .test_utils import *
from zillion.core import UnsupportedGrainException, InvalidFieldException
from zillion.report import ROLLUP_INDEX_LABEL, ROLLUP_TOTALS


def test_report(wh):
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
    dbg(result)
    assert result


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


def test_reporting_moving_average_formula_metric(wh):
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


def test_report_no_dimensions(wh):
    metrics = ["revenue", "sales_quantity"]
    criteria = [("campaign_name", "=", "Campaign 2B")]
    result = wh.execute(metrics, criteria=criteria)
    assert result


def test_report_no_metrics(wh):
    metrics = []
    dimensions = ["partner_name", "campaign_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_null_criteria(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name"]
    criteria = [("campaign_name", "!=", None)]
    result = wh.execute(metrics, dimensions=dimensions, criteria=criteria)
    assert result


def test_report_count_metric(wh):
    metrics = ["leads"]
    dimensions = ["campaign_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_alias_metric(wh):
    metrics = ["revenue_avg"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_alias_dimension(wh):
    metrics = ["revenue"]
    dimensions = ["lead_id"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_multiple_queries(wh):
    metrics = ["revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_formula_metric(wh):
    metrics = ["rpl", "revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_nested_formnula_metric(wh):
    metrics = ["rpl_squared", "rpl_unsquared", "rpl", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_ds_dimension_formula(wh):
    metrics = ["sales"]
    dimensions = ["revenue_decile"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_ds_metric_formula(wh):
    metrics = ["revenue", "revenue_ds"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


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


def test_report_weighted_ds_metric_formula(wh):
    metrics = ["revenue_avg", "revenue_avg_ds_weighted"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_weighted_metric(wh):
    metrics = ["sales_quantity", "revenue_avg", "revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_weighted_metric_with_rollup(wh):
    metrics = ["sales_quantity", "revenue_avg", "leads"]
    dimensions = ["partner_name"]
    rollup = ROLLUP_TOTALS
    result = wh.execute(metrics, dimensions=dimensions, rollup=rollup)
    assert result


def test_report_weighted_metric_with_multi_rollup(wh):
    metrics = ["sales_quantity", "revenue_avg", "leads"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]
    rollup = 2
    result = wh.execute(metrics, dimensions=dimensions, rollup=rollup)
    assert result


def test_report_multi_dimension(wh):
    metrics = ["leads", "sales"]
    dimensions = ["partner_name", "lead_id"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_rollup(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = ROLLUP_TOTALS
    result = wh.execute(
        metrics, dimensions=dimensions, criteria=criteria, rollup=rollup
    )
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


def test_report_adhoc_dimension(wh):
    metrics = ["leads", "sales"]
    dimensions = [
        "partner_name",
        "lead_id",
        {"formula": "{lead_id} > 3", "name": "testdim"},
    ]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_adhoc_metric(wh):
    metrics = ["revenue", {"formula": "{revenue} > 3*{lead_id}", "name": "testmetric"}]
    dimensions = ["partner_name", "lead_id"]
    result = wh.execute(metrics, dimensions=dimensions)
    assert result


def test_report_adhoc_datasource(wh):
    metrics = ["revenue", "adhoc_metric"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource()
    result = wh.execute(metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds])
    assert result


def test_report_date_conversion(wh):
    metrics = ["revenue"]
    dimensions = ["datetime", "hour_of_day"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    result = wh.execute(metrics, dimensions=dimensions, criteria=criteria)
    dbg(result)
    assert result


def test_report_datasource_priority(wh):
    metrics = ["revenue", "leads", "sales"]
    dimensions = ["partner_name"]
    report = wh.build_report(metrics=metrics, dimensions=dimensions)
    assert report.queries[0].get_datasource_name() == "testdb2"


def test_report_multi_datasource(wh):
    metrics = ["revenue", "leads", "sales", "revenue_avg"]
    dimensions = ["partner_name"]
    report = wh.build_report(metrics=metrics, dimensions=dimensions)
    assert len(report.queries) == 2
    result = report.execute()
    dbg(result)
    assert result


def test_report_save_and_load(wh):
    metrics = ["revenue", "leads", "sales"]
    dimensions = ["partner_name"]
    report = wh.build_report(metrics=metrics, dimensions=dimensions)
    try:
        report_id = report.save()
        result = wh.execute_id(report_id)
        assert result
    finally:
        wh.delete_report(report_id)


def test_report_adhoc_datasource_save_and_load(wh):
    metrics = ["revenue", "leads", "adhoc_metric"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource()
    wh.add_adhoc_datasources([adhoc_ds])
    report = wh.build_report(metrics=metrics, dimensions=dimensions)
    try:
        report_id = report.save()
        result = wh.execute_id(report_id)
        assert result
    finally:
        wh.delete_report(report_id)


# TODO: this is failing, doesnt seem to be loading adhoc DS correctly
# def test_report_missing_adhoc_datasource_save_and_load(wh):
#     metrics = ['revenue', 'leads', 'adhoc_metric']
#     dimensions = ['partner_name']
#     adhoc_ds = get_adhoc_datasource()
#     wh.add_adhoc_datasources([adhoc_ds])
#     report = wh.build_report(metrics=metrics, dimensions=dimensions)
#     report_id = report.save()
#     wh.remove_adhoc_datasources([adhoc_ds])
#     result = wh.execute_id(report_id)
#     # TODO: should clean up report IDs when done
#     assert result
