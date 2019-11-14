import copy
import contextlib
import cProfile
import pstats
import pytest
import time

from tlbx import dbg, st

from zillion.configs import load_warehouse_config
from zillion.core import TableTypes
from zillion.warehouse import DataSource, AdHocDataSource, Warehouse
from .test_utils import create_adhoc_datatable, get_testdb_url

TEST_CONFIG = load_warehouse_config("test_config.json")


@contextlib.contextmanager
def profiled(pattern=None):
    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()
    stats = pstats.Stats(pr)
    stats.sort_stats("cumulative")
    dbg("Top 10 calls by cumulative time:")
    stats.print_stats(10)
    if pattern:
        stats.sort_stats("time")
        dbg("Top 10 %s calls by function time:" % pattern)
        stats.print_stats(pattern, 10)


def get_adhoc_ds(size):
    metrics = ["adhoc_metric1", "adhoc_metric2", "adhoc_metric3", "adhoc_metric4"]
    dimensions = ["partner_name", "campaign_name", "lead_id"]

    column_defs = {
        "partner_name": {"fields": ["partner_name"], "type": str},
        "campaign_name": {"fields": ["campaign_name"], "type": str},
        "lead_id": {"fields": ["lead_id"], "type": int},
        "adhoc_metric1": {"fields": ["adhoc_metric1"], "type": float},
        "adhoc_metric2": {"fields": ["adhoc_metric2"], "type": float},
        "adhoc_metric3": {"fields": ["adhoc_metric3"], "type": float},
        "adhoc_metric4": {"fields": ["adhoc_metric4"], "type": float},
    }

    start = time.time()
    dt = create_adhoc_datatable(
        "adhoc_table1", TableTypes.METRIC, column_defs, ["partner_name"], size
    )
    adhoc_ds = AdHocDataSource([dt])
    dbg("Created AdHocDataSource in %.3fs" % (time.time() - start))
    return metrics, dimensions, adhoc_ds


@pytest.mark.longrun
def test_performance(config, datasources):
    wh = Warehouse(datasources, config=config)
    metrics, dimensions, adhoc_ds = get_adhoc_ds(1e5)
    with profiled("zillion"):
        result = wh.execute(
            metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
        )
    assert result


@pytest.mark.longrun
def test_performance_multi_rollup(config, datasources):
    wh = Warehouse(datasources, config=config)
    metrics, dimensions, adhoc_ds = get_adhoc_ds(1e5)
    rollup = 2
    with profiled("zillion"):
        result = wh.execute(
            metrics, dimensions=dimensions, rollup=rollup, adhoc_datasources=[adhoc_ds]
        )
    assert result
