import pytest
import threading

from .test_utils import *
from zillion.configs import zillion_config
from zillion.core import *
from zillion.field import Metric
from zillion.report import ROLLUP_INDEX_LABEL


def test_basic_report(wh):
    metrics = ["revenue", "main_sales_quantity"]
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 11)]
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_sequential_timeout(wh):
    metrics = ["adhoc_metric", "revenue"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource(size=5e5, name="adhoc_large_db", reuse=True)
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        with pytest.raises(DataSourceQueryTimeoutException):
            result = wh.execute(
                metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
            )


def test_report_multithreaded_timeout(wh):
    metrics = ["adhoc_metric", "revenue"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource(size=5e5, name="adhoc_large_db", reuse=True)
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        with pytest.raises(DataSourceQueryTimeoutException):
            result = wh.execute(
                metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
            )


def test_report_one_worker(wh):
    metrics = ["sales", "revenue", "leads"]
    dimensions = ["partner_name"]
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.MULTITHREAD,
            DATASOURCE_QUERY_WORKERS=1,
        )
    ):
        result = wh_execute(wh, locals())


@pytest.mark.longrun
def test_report_reuse_after_timeout(wh):
    metrics = ["adhoc_metric", "revenue"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource(size=5e5, name="adhoc_large_db", reuse=True)
    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=1e-2,
        )
    ):
        report = Report(
            wh, metrics=metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
        )
        with pytest.raises(DataSourceQueryTimeoutException):
            result = report.execute()

    result = report.execute()
    assert result
    info(result.df)


def test_report_kill(wh):
    metrics = ["adhoc_metric", "revenue"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource(size=5e5, name="adhoc_large_db", reuse=True)
    t = None

    try:
        report = Report(
            wh, metrics=metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
        )

        t = threading.Timer(0.1, report.kill, kwargs=dict(raise_if_failed=True))
        t.start()
        with pytest.raises(ExecutionKilledException):
            result = report.execute()
    finally:
        if t:
            t.cancel()


def test_report_reuse_after_kill(wh):
    metrics = ["adhoc_metric", "revenue"]
    dimensions = ["partner_name"]
    criteria = [("partner_name", "like", "%zz%")]
    adhoc_ds = get_adhoc_datasource(size=5e5, name="adhoc_large_db", reuse=True)
    t = None

    try:
        report = Report(
            wh,
            metrics=metrics,
            dimensions=dimensions,
            criteria=criteria,
            adhoc_datasources=[adhoc_ds],
        )

        t = threading.Timer(0.1, report.kill)
        t.start()
        with pytest.raises(ExecutionKilledException):
            result = report.execute()
    finally:
        if t:
            t.cancel()

    result = report.execute()
    assert result
    info(result.df)


def test_report_timeout_then_kill(wh):
    metrics = ["adhoc_metric", "revenue"]
    dimensions = ["partner_name"]
    adhoc_ds = get_adhoc_datasource(size=5e5, name="adhoc_large_db", reuse=True)
    t = None

    with update_zillion_config(
        dict(
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=1e-3,
        )
    ):
        report = Report(
            wh, metrics=metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
        )
        with pytest.raises(DataSourceQueryTimeoutException):
            result = report.execute()

    try:
        t = threading.Timer(0.1, report.kill)
        t.start()
        with pytest.raises(ExecutionKilledException):
            result = report.execute()
    finally:
        if t:
            t.cancel()


def test_impossible_report(wh):
    metrics = ["leads"]
    dimensions = ["sale_id"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    with pytest.raises(UnsupportedGrainException):
        result = wh_execute(wh, locals())


def test_report_count_aggr(wh):
    metrics = ["leads", "lead_count", "lead_count_distinct"]
    dimensions = ["campaign_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_criteria_between(wh):
    metrics = ["leads"]
    dimensions = ["partner_name"]
    criteria = [("date", "between", ["2020-01-01", "2020-05-01"])]
    result = wh_execute(wh, locals())
    assert result and result.rowcount > 0
    criteria = [("date", "not between", ["2020-01-01", "2020-05-01"])]
    result = wh_execute(wh, locals())
    assert result.rowcount == 0


def test_report_criteria_in(wh):
    metrics = ["leads"]
    dimensions = ["date"]
    criteria = [("date", "in", ["2020-04-29", "2020-04-30"])]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)
    criteria = [("date", "not in", ["2020-04-29", "2020-04-30"])]
    result = wh_execute(wh, locals())
    assert result.rowcount == 0


def test_report_pivot(wh):
    metrics = ["revenue", "main_sales_quantity"]
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 11)]
    rollup = RollupTypes.TOTALS
    pivot = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_df_display(wh):
    metrics = ["revenue", "main_sales_quantity"]
    dimensions = ["partner_name", "campaign_name"]
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df_display)


def test_report_technical_ma(wh):
    metrics = ["revenue", "revenue_ma_5"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 8)]
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_adhoc_technicals(wh):
    dim_groups = [["partner_name", "campaign_name", "sale_id"], ["sale_id"]]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = RollupTypes.TOTALS

    technical_strings = [
        "mean(5, 1)",
        "sum(2)",
        "median(3)",
        "std(2)",
        "var(2)",
        "min(2)",
        "max(2)",
        "boll(2)",
        "diff(1)",
        "pct_change",
        "cumsum",
        "cummin",
        "cummax",
        "rank",
        "pct_rank",
    ]

    for tech in technical_strings:
        for mode in (TechnicalModes.GROUP, TechnicalModes.ALL):
            for dimensions in dim_groups:
                tech_str = tech + ":" + mode
                metrics = [
                    "revenue",
                    {
                        "formula": "{revenue}",
                        "technical": tech_str,
                        "name": "my_revenue_tech",
                    },
                ]
                result = wh_execute(wh, locals())
                assert result
                info("Technical: %s Mode: %s Dims: %s" % (tech_str, mode, dimensions))
                info(result.df)


def test_report_no_dimension_technical(wh):
    dimensions = None
    criteria = [("campaign_name", "!=", "Campaign 2B")]

    technical_strings = [
        "mean(5, 1)",
        "diff(1)",
        "pct_change",
        "boll(2)",
        "cumsum",
        "rank",
        "pct_rank",
    ]

    for tech in technical_strings:
        for mode in (TechnicalModes.GROUP, TechnicalModes.ALL):
            tech_str = tech + ":" + mode
            metrics = [
                "revenue",
                {
                    "formula": "{revenue}",
                    "technical": tech_str,
                    "name": "my_revenue_tech",
                },
            ]
            result = wh_execute(wh, locals())
            assert result
            info("Technical: %s Mode: %s" % (tech_str, mode))
            info(result.df)


def test_report_technical_ma_formula(wh):
    metrics = ["revenue", "rpl", "rpl_ma_5"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = [("revenue", ">", 8)]
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_technical_rolling_sum(wh):
    metrics = ["revenue", "revenue_sum_5"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = None
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_technical_cumsum(wh):
    metrics = ["revenue", "revenue_cumsum"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = None
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_technical_diff(wh):
    metrics = ["revenue", "revenue_diff"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = None
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_technical_pct_diff(wh):
    metrics = ["revenue", "revenue_pct_diff"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = None
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_technical_bollinger(wh):
    metrics = ["revenue", "revenue_boll_5"]
    # TODO: it doesnt make sense to use these dimensions, but no date/time
    # dims have been added as of the time of creating this test.
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    row_filters = None
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_no_dimensions(wh):
    metrics = ["revenue", "main_sales_quantity"]
    criteria = [("campaign_name", "=", "Campaign 2B")]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_no_metrics(wh):
    metrics = []
    dimensions = ["partner_name", "campaign_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_null_criteria(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name"]
    criteria = [("campaign_name", "!=", None)]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_incomplete_dimensions(config):
    del config["datasources"]["testdb2"]
    metrics = ["sales"]
    dimensions = ["campaign_name"]
    table_config = config["datasources"]["testdb1"]["tables"]["main.sales"]

    # This should prevent it from joining back through the lead table,
    # thus preventing the report from running.
    table_config["incomplete_dimensions"] = ["lead_id"]
    wh = Warehouse(config=config)
    with pytest.raises(UnsupportedGrainException):
        result = wh_execute(wh, locals())

    table_config["incomplete_dimensions"] = ["xyz"]
    with pytest.raises(WarehouseException):
        wh = Warehouse(config=config)


def test_report_inactive_table(config):
    table_config = config["datasources"]["testdb1"]["tables"]["main.sales"]
    table_config["active"] = False
    del config["datasources"]["testdb2"]
    wh = Warehouse(config=config)
    metrics = ["revenue"]
    dimensions = ["partner_name"]
    with pytest.raises(UnsupportedGrainException):
        result = wh_execute(wh, locals())

    metrics = ["leads"]
    result = wh_execute(wh, locals())


def test_report_count_metric(wh):
    metrics = ["leads"]
    dimensions = ["campaign_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_alias_metric(wh):
    metrics = ["revenue_mean"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_alias_dimension(wh):
    metrics = ["revenue"]
    dimensions = ["lead_id"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_multiple_queries(wh):
    metrics = ["revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_formula_metric(wh):
    metrics = ["rpl", "revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_nested_formula_metric(wh):
    metrics = ["rpl_squared", "rpl_unsquared", "rpl", "leads"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


# TODO: add support for FormulaDimensions
#    {
#        "name": "partner_name_query_dim",
#        "type": "String(32)",
#        "formula": "CASE WHEN ({partner_name} LIKE '%B%' OR {partner_name} LIKE '%C%') THEN 'Match' ELSE {partner_name} END"
#    }
#
# def test_report_formula_dimension(wh):
#     metrics = ["leads"]
#     dimensions = ["campaign_name", "partner_name_query_dim"]
#     result = wh_execute(wh, locals())
#     assert result
#     info(result.df)


def test_report_ds_dimension_formula(wh):
    metrics = ["sales"]
    dimensions = ["revenue_decile"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_ds_metric_formula(wh):
    metrics = ["revenue", "revenue_ds"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_where_ds_formula(wh):
    metrics = ["sales"]
    criteria = [("revenue_decile", ">=", 0)]
    dimensions = ["campaign_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_metric_formula_with_dim(config):
    config["metrics"].append(
        {
            "name": "revenue_formula_with_dim",
            "aggregation": AggregationTypes.MEAN,
            "formula": "1.0*{revenue}*IFNULL({campaign_name}, 0)",
        }
    )
    wh = Warehouse(config=config)
    metrics = ["revenue", "revenue_formula_with_dim"]
    dimensions = ["partner_name"]
    with pytest.raises(ReportException):
        result = wh_execute(wh, locals())


# def test_report_where_formula_dim(wh):
#     metrics = ["sales"]
#     criteria = [
#         ("partner_name_query_dim", "=", "Match")
#     ]
#     dimensions = ["partner_name"]
#     with pytest.raises(ReportException):
#         result = wh_execute(wh, locals())


def test_report_only_dimensions_ds_formula(wh):
    criteria = [("campaign_name_length", ">", 5)]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result.df.index.any()
    info(result.df)


def test_report_non_existent_metric(wh):
    metrics = ["sales1234"]
    dimensions = ["campaign_id"]
    with pytest.raises(InvalidFieldException):
        result = wh_execute(wh, locals())


def test_report_metric_required_grain(wh):
    metrics = ["revenue", "revenue_required_grain"]
    dimensions = ["campaign_id"]
    with pytest.raises(UnsupportedGrainException):
        result = wh_execute(wh, locals())


def test_report_metric_formula_required_grain(wh):
    metrics = ["revenue", "revenue_per_lead_required_grain"]
    dimensions = ["campaign_id"]
    with pytest.raises(UnsupportedGrainException):
        result = wh_execute(wh, locals())


def test_report_metric_formula_field_required_grain(wh):
    metrics = ["revenue", "revenue_formula_required_grain"]
    dimensions = ["campaign_id"]
    with pytest.raises(UnsupportedGrainException):
        result = wh_execute(wh, locals())


def test_report_weighted_formula_metric(wh):
    metrics = ["rpl_weighted", "rpl", "main_sales_quantity", "revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_weighted_ds_metric_formula(wh):
    metrics = ["revenue_mean", "revenue_mean_ds_weighted"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_weighted_metric(wh):
    metrics = ["main_sales_quantity", "revenue_mean", "revenue", "leads"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)
    assert result.df.loc["Partner A"]["revenue_mean"] == 14.67


def test_report_weighted_rollup(wh):
    metrics = ["main_sales_quantity", "revenue_mean", "leads"]
    dimensions = ["partner_name"]
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    assert result
    info(result.df)
    assert result.rollup_rows["revenue_mean"][0] == 17.08


def test_report_weighted_multi_rollup(wh):
    metrics = ["main_sales_quantity", "revenue_mean", "leads"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]
    rollup = 2
    result = wh_execute(wh, locals())
    assert result
    info(result.df)
    test_row = result.df.loc["Partner A", ROLLUP_INDEX_LABEL, ROLLUP_INDEX_LABEL]
    assert test_row["revenue_mean"] == 14.67


def test_report_multi_dimension(wh):
    metrics = ["leads", "sales"]
    dimensions = ["partner_name", "lead_id"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_rollup(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name", "campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = RollupTypes.TOTALS
    result = wh_execute(wh, locals())
    info(result.df)
    revenue = result.rollup_rows.iloc[-1]["revenue"]
    revenue_sum = result.non_rollup_rows.sum()["revenue"]
    assert revenue == revenue_sum


def test_report_multi_rollup(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = 3
    result = wh_execute(wh, locals())
    info(result.df)
    revenue = result.rollup_rows.iloc[-1]["revenue"]
    revenue_sum = result.non_rollup_rows.sum()["revenue"]
    assert revenue == revenue_sum


def test_report_all_rollup(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = RollupTypes.ALL
    result = wh_execute(wh, locals())
    info(result.df)
    revenue = result.rollup_rows.iloc[-1]["revenue"]
    revenue_sum = result.non_rollup_rows.sum()["revenue"]
    assert revenue == revenue_sum


def test_report_multi_rollup_pivot(wh):
    metrics = ["revenue"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]
    criteria = [("campaign_name", "!=", "Campaign 2B")]
    rollup = 3
    pivot = ["campaign_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


# TODO: add AdHocDimension support
# def test_report_adhoc_dimension(wh):
#     metrics = ["leads", "sales"]
#     dimensions = [
#         "partner_name",
#         "lead_id",
#         {"formula": "{lead_id} > 3", "name": "testdim"},
#     ]
#     result = wh_execute(wh, locals())
#     assert result
#     info(result.df)


# def test_report_adhoc_dimension_rollup(wh):
#     metrics = ["leads", "sales"]
#     dimensions = [
#         {"name": "testdim", "formula": "CASE WHEN ({partner_name} LIKE '%B%' OR {partner_name} LIKE '%C%') THEN 'Match' ELSE {partner_name} END"}
#     ]
#     rollup = RollupTypes.TOTALS
#     result = wh_execute(wh, locals())
#     assert result
#     info(result.df)


def test_report_adhoc_metric(wh):
    metrics = ["revenue", {"formula": "{revenue} > 3*{lead_id}", "name": "testmetric"}]
    dimensions = ["partner_name", "lead_id"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_adhoc_nested_metric(wh):
    metrics = [
        "revenue",
        "rpl_squared",
        {"formula": "{rpl_unsquared} > 10", "name": "testmetric"},
    ]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_where_date_conversion(wh):
    criteria = [
        ("campaign_name", "=", "Campaign 2B"),
        ("campaign_date", "=", "2019-03-26"),
    ]
    dimensions = ["campaign_created_at"]
    result = wh_execute(wh, locals())
    assert result.df.index.any()
    info(result.df)


def test_report_sqlite_date_conversions(wh):
    params = get_date_conversion_test_params()
    result = wh.execute(**params)
    assert result
    df = result.df.reset_index()
    row = df.iloc[0]
    info(df)
    for field, value in EXPECTED_DATE_CONVERSION_VALUES:
        assert row[field] == value


def test_report_datasource_priority(wh):
    metrics = ["revenue", "leads", "sales"]
    dimensions = ["partner_name"]
    report = Report(wh, metrics=metrics, dimensions=dimensions)
    assert report.queries[0].get_datasource_name() == "testdb1"


def test_report_multi_datasource(wh):
    metrics = ["revenue", "leads", "sales", "revenue_mean"]
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
    spec_id = report.save()
    try:
        result = wh.execute_id(spec_id)
        assert result
        info(result.df)
    finally:
        wh.delete_report(spec_id)


def test_report_save_and_load_adhoc_metric(wh):
    metrics = ["revenue", {"formula": "{revenue} > 3*{lead_id}", "name": "testmetric"}]
    dimensions = ["partner_name", "lead_id"]
    report = Report(wh, metrics=metrics, dimensions=dimensions)
    spec_id = report.save()
    try:
        result = wh.execute_id(spec_id)
        assert result
        info(result.df)
    finally:
        wh.delete_report(spec_id)


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
    spec_id = report.save()
    try:
        result = wh.execute_id(spec_id, adhoc_datasources=[adhoc_ds])
        assert result
        info(result.df)
    finally:
        wh.delete_report(spec_id)


def test_report_missing_adhoc_datasource_save_and_load(wh, adhoc_ds):
    metrics = ["revenue", "leads", "adhoc_metric"]
    dimensions = ["partner_name"]
    report = wh.save_report(
        metrics=metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
    )
    spec_id = report.save()
    try:
        with pytest.raises(ReportException):
            result = wh.execute_id(spec_id)
    finally:
        wh.delete_report(spec_id)


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
    ds1 = DataSource("testdb1", config=config["datasources"]["testdb1"])
    ds2 = DataSource("testdb2", config=config["datasources"]["testdb2"])
    wh = Warehouse(datasources=[ds1])
    metrics = ["leads", "sales", "aggr_sales"]
    dimensions = ["partner_name", "campaign_name"]
    result = wh.execute(metrics, dimensions=dimensions, adhoc_datasources=[ds2])
    assert result
    info(result.df)


def test_only_adhoc_datasource(adhoc_ds):
    wh = Warehouse(datasources=[adhoc_ds])
    metrics = ["adhoc_metric"]
    dimensions = ["partner_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_no_use_full_column_names(config):
    ds = DataSource("testdb2", config=config["datasources"]["testdb2"])
    wh = Warehouse(datasources=[ds])
    metrics = ["leads", "sales", "aggr_sales"]
    dimensions = ["partner_name", "campaign_name"]
    result = wh_execute(wh, locals())
    assert result
    info(result.df)


def test_report_column_required_grain(config):
    ds = DataSource("testdb2", config=config["datasources"]["testdb2"])
    wh = Warehouse(datasources=[ds])
    metrics = ["revenue", "sales"]
    dimensions = ["campaign_name"]
    with pytest.raises(UnsupportedGrainException):
        result = wh_execute(wh, locals())


def test_ds_metric_formula_sql_injection(config):
    # example = r"""I don't like "special" ch;ars ¯\_(ツ)_/¯"""
    table_config = config["datasources"]["testdb1"]["tables"]["main.sales"]
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
        result = wh_execute(wh, locals())


def test_ds_dim_formula_sql_injection(config):
    # example = r"""I don't like "special" ch;ars ¯\_(ツ)_/¯"""
    table_config = config["datasources"]["testdb1"]["tables"]["main.sales"]
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
        result = wh_execute(wh, locals())


def test_metric_formula_sql_injection(config):
    config["metrics"].append(
        dict(
            name="rpl_injection",
            aggregation=AggregationTypes.MEAN,
            rounding=2,
            formula="{revenue}/{leads};select * from leads",
        )
    )

    metrics = ["revenue", "rpl_injection"]
    dimensions = ["partner_name"]
    wh = Warehouse(config=config)
    with pytest.raises(DisallowedSQLException):
        result = wh_execute(wh, locals())


def test_weighting_metric_sql_injection(config):
    config["metrics"].append(
        dict(
            name="rpl_injection",
            aggregation=AggregationTypes.MEAN,
            rounding=2,
            formula="{revenue}/{leads}",
            weighting_metric="main_sales_quantity;select * from leads",
        )
    )

    metrics = ["revenue", "rpl_injection"]
    dimensions = ["partner_name"]
    wh = Warehouse(config=config)
    with pytest.raises(InvalidFieldException):
        result = wh_execute(wh, locals())


def test_adhoc_metric_sql_injection(wh):
    metrics = [
        "revenue",
        {"formula": "{revenue};select * from leads", "name": "rev_injection"},
    ]
    dimensions = ["partner_name"]
    with pytest.raises(DisallowedSQLException):
        result = wh_execute(wh, locals())


# def test_adhoc_dimension_sql_injection(wh):
#     metrics = ["leads", "sales"]
#     dimensions = [
#         "partner_name",
#         {"formula": "{lead_id} > 3;drop table leads", "name": "testdim"},
#     ]
#     with pytest.raises(DisallowedSQLException):
#         result = wh_execute(wh, locals())


def test_criteria_sql_injection(wh):
    metrics = ["leads", "sales"]
    dimensions = ["campaign_name"]
    criteria = [("campaign_name", "!=", "Campaign 2B';select * from leads --")]
    result = wh_execute(wh, locals())

    criteria = [("select * from leads", "!=", "Campaign 2B")]
    with pytest.raises(InvalidFieldException):
        result = wh_execute(wh, locals())

    criteria = [("campaign_name", "select * from leads", "Campaign 2B")]
    with pytest.raises(ZillionException):
        result = wh_execute(wh, locals())


def test_row_filter_sql_injection(wh):
    metrics = ["leads", "revenue"]
    dimensions = ["partner_name"]
    row_filters = [("revenue", ">", "11;select * from leads")]
    with pytest.raises(SyntaxError):
        result = wh_execute(wh, locals())


def test_pivot_sql_injection(wh):
    metrics = ["leads", "revenue"]
    dimensions = ["partner_name", "campaign_name"]
    pivot = ["partner_name;select * from leads"]
    with pytest.raises(ZillionException):
        result = wh_execute(wh, locals())


def test_metric_name_sql_injection(config):
    config["metrics"].append(
        dict(
            name="select * from leads",
            aggregation=AggregationTypes.MEAN,
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
    table_config = config["datasources"]["testdb1"]["tables"]["main.sales"]
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
    del tables["main.sales"]
    # Since this table doesn't actually match a table name it shouldn't ever
    # become a part of the warehouse.
    tables["select * from leads;--"] = {
        "type": TableTypes.METRIC,
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
        result = wh_execute(wh, locals())
