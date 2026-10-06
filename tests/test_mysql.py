import pytest

from .test_utils import *
from zillion.core import *
from zillion.datasource import *
import zillion.report as report_module
from zillion.sql_utils import sqla_compile


class RecordingConn:
    def __init__(self, conn):
        self._conn = conn
        self.explain_calls = []
        self.query_sql = []

    def execute(self, statement, *args, **kwargs):
        if isinstance(statement, report_module.ExplainJSON):
            self.explain_calls.append(statement)
        else:
            self.query_sql.append(sqla_compile(statement))
        return self._conn.execute(statement, *args, **kwargs)

    def invalidate(self, *args, **kwargs):
        return self._conn.invalidate(*args, **kwargs)

    def close(self):
        return self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _execute_mysql_with_prefix_analysis(
    mysql_wh, monkeypatch, config_updates, metrics=None, dimensions=None
):
    wrapped_connections = []
    recorded_costs = []
    original_get_conn = report_module.DataSourceQuery.get_conn
    original_get_query_cost = report_module.DataSourceQuery._get_query_cost

    def wrapped_get_conn(query):
        conn = RecordingConn(original_get_conn(query))
        wrapped_connections.append(conn)
        return conn

    def wrapped_get_query_cost(query, select):
        compiled_select = sqla_compile(select)
        cost = original_get_query_cost(query, select)
        recorded_costs.append(
            dict(has_prefix="STRAIGHT_JOIN" in compiled_select, cost=float(cost))
        )
        return cost

    monkeypatch.setattr(report_module.DataSourceQuery, "get_conn", wrapped_get_conn)
    monkeypatch.setattr(
        report_module.DataSourceQuery, "_get_query_cost", wrapped_get_query_cost
    )

    metrics = metrics or ["cost", "clicks"]
    dimensions = dimensions or ["partner_name"]
    with update_zillion_config(config_updates):
        result = mysql_wh.execute(metrics, dimensions=dimensions)
    return result, wrapped_connections, recorded_costs


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


def test_mysql_report_repeat_criteria(wh):
    metrics = ["rpl", "sales"]
    dimensions = ["date"]
    criteria = [("date", ">=", "2020-04-29"), ("date", "<", "2020-05-01")]
    result = wh_execute(wh, locals())
    assert result and result.rowcount > 0
    info(result.df)


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
        print(f"Checking {field} = {value}")
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


def test_mysql_query_prefix_analysis_drops_prefix_when_cost_threshold_met(
    mysql_wh, monkeypatch
):
    result, wrapped_connections, recorded_costs = _execute_mysql_with_prefix_analysis(
        mysql_wh,
        monkeypatch,
        dict(
            DATASOURCE_QUERY_ANALYZE_PREFIX=True,
            DATASOURCE_QUERY_PREFIX_COST_RATIO=2,
            DATASOURCE_QUERY_PREFIX_MIN_COST_DIFFERENCE=1,
        ),
    )

    assert result
    assert [len(conn.explain_calls) for conn in wrapped_connections] == [2]
    assert [item["has_prefix"] for item in recorded_costs] == [True, False]
    prefixed_cost = recorded_costs[0]["cost"]
    unprefixed_cost = recorded_costs[1]["cost"]
    assert prefixed_cost > unprefixed_cost
    assert prefixed_cost >= 2 * unprefixed_cost
    assert prefixed_cost - unprefixed_cost >= 1
    assert "STRAIGHT_JOIN" not in wrapped_connections[0].query_sql[-1]


def test_mysql_query_prefix_analysis_keeps_prefix_when_cost_threshold_not_met(
    mysql_wh, monkeypatch
):
    result, wrapped_connections, recorded_costs = _execute_mysql_with_prefix_analysis(
        mysql_wh,
        monkeypatch,
        dict(
            DATASOURCE_QUERY_ANALYZE_PREFIX=True,
            DATASOURCE_QUERY_PREFIX_COST_RATIO=5,
            DATASOURCE_QUERY_PREFIX_MIN_COST_DIFFERENCE=10000,
        ),
    )

    assert result
    assert [len(conn.explain_calls) for conn in wrapped_connections] == [2]
    assert [item["has_prefix"] for item in recorded_costs] == [True, False]
    prefixed_cost = recorded_costs[0]["cost"]
    unprefixed_cost = recorded_costs[1]["cost"]
    assert prefixed_cost > unprefixed_cost
    assert not (
        prefixed_cost >= 5 * unprefixed_cost
        and prefixed_cost - unprefixed_cost >= 10000
    )
    assert "STRAIGHT_JOIN" in wrapped_connections[0].query_sql[-1]


def test_mysql_query_prefix_analysis_disabled_skips_explains(mysql_wh, monkeypatch):
    wrapped_connections = []
    original_get_conn = report_module.DataSourceQuery.get_conn

    def wrapped_get_conn(query):
        conn = RecordingConn(original_get_conn(query))
        wrapped_connections.append(conn)
        return conn

    monkeypatch.setattr(report_module.DataSourceQuery, "get_conn", wrapped_get_conn)

    with update_zillion_config(dict(DATASOURCE_QUERY_ANALYZE_PREFIX=False)):
        result = mysql_wh.execute(
            ["cost", "clicks", "transactions"], dimensions=["partner_name"]
        )

    assert result
    assert wrapped_connections
    assert sum(len(conn.explain_calls) for conn in wrapped_connections) == 0
