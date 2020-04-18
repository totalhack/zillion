import copy
import contextlib
import cProfile
import pstats
import pytest
import time

from .test_utils import *
from zillion.core import *
from zillion.datasource import *


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
    primary_key = ["partner_name"]

    column_types = dict(
        partner_name=str,
        campaign_name=str,
        lead_id=int,
        adhoc_metric1=float,
        adhoc_metric2=float,
        adhoc_metric3=float,
        adhoc_metric4=float,
    )

    table_config = {
        "type": TableTypes.METRIC,
        "create_fields": True,
        "columns": OrderedDict(
            partner_name={"fields": ["partner_name"]},
            campaign_name={"fields": ["campaign_name"]},
            lead_id={"fields": ["lead_id"]},
            adhoc_metric1={"fields": ["adhoc_metric1"]},
            adhoc_metric2={"fields": ["adhoc_metric2"]},
            adhoc_metric3={"fields": ["adhoc_metric3"]},
            adhoc_metric4={"fields": ["adhoc_metric4"]},
        ),
    }

    start = time.time()
    dt = create_adhoc_datatable(
        "adhoc_table1", table_config, primary_key, column_types, size
    )
    adhoc_ds = AdHocDataSource([dt])
    dbg("Created AdHocDataSource in %.3fs" % (time.time() - start))
    return metrics, dimensions, adhoc_ds


@pytest.mark.longrun
def test_performance_adhoc_ds(wh):
    size = 1e5
    metrics, dimensions, adhoc_ds = get_adhoc_ds(size)
    try:
        with profiled("zillion"):
            result = wh.execute(
                metrics, dimensions=dimensions, adhoc_datasources=[adhoc_ds]
            )
        assert result
    finally:
        adhoc_ds.clean_up()


@pytest.mark.longrun
def test_performance_multi_rollup(wh):
    size = 1e5
    metrics, dimensions, adhoc_ds = get_adhoc_ds(size)
    try:
        rollup = 2
        with profiled("zillion"):
            result = wh.execute(
                metrics,
                dimensions=dimensions,
                rollup=rollup,
                adhoc_datasources=[adhoc_ds],
            )
        assert result
    finally:
        adhoc_ds.clean_up()
