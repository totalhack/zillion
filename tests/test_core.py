import pytest

from tlbx import dbg, st

from .test_utils import *
from zillion.core import UnsupportedGrainException
from zillion.sql_utils import contains_aggregation
from zillion.warehouse import Warehouse


def test_warehouse_init(wh):
    assert wh.dimension_tables
    assert wh.dimensions


def test_warehouse_no_config(datasources):
    wh = Warehouse(datasources)
    assert not wh.dimensions


def test_warehouse_no_config_has_zillion_info(wh, datasources):
    for table in datasources[0].metadata.tables.values():
        table.info["zillion"] = {"type": "fact", "active": True, "autocolumns": True}
    wh = Warehouse(datasources)
    assert wh.dimensions


def test_table_config_override(datasources, config):
    config["datasources"]["testdb1"]["tables"]["campaigns"]["type"] = "fact"
    ds_priority = [ds.name for ds in datasources]
    wh = Warehouse(datasources, config=config, ds_priority=ds_priority)
    assert "campaigns" in wh.fact_tables["testdb1"]


def test_column_config_override(datasources, config):
    table_config = config["datasources"]["testdb1"]["tables"]["sales"]
    table_config["columns"]["lead_id"]["active"] = False
    ds_priority = [ds.name for ds in datasources]
    wh = Warehouse(datasources, config=config, ds_priority=ds_priority)
    assert not "sales.lead_id" in wh.dimensions["lead_id"].get_column_names("testdb1")


def test_get_dimension_table_set(wh):
    possible = [{"partner_id", "partner_name"}, {"campaign_name", "partner_name"}]

    impossible = [
        {"lead_id", "partner_id"},
        {"sale_id", "lead_id", "campaign_name", "partner_name"},
    ]

    for grain in possible:
        wh.get_dimension_table_set(grain)

    for grain in impossible:
        with pytest.raises(UnsupportedGrainException):
            wh.get_dimension_table_set(grain)


def test_get_fact_table_set(wh):
    possible = [
        ("leads", {"partner_id", "partner_name"}),
        ("leads", {"campaign_name", "partner_name"}),
        ("revenue", {"campaign_name", "partner_name", "lead_id"}),
    ]

    impossible = [("leads", {"sale_id"})]

    for fact, grain in possible:
        wh.get_fact_table_set(fact, grain)

    for fact, grain in impossible:
        with pytest.raises(UnsupportedGrainException):
            wh.get_fact_table_set(fact, grain)


def test_get_supported_dimensions(wh):
    facts = ["leads", "sales_quantity"]
    dims = wh.get_supported_dimensions(facts)
    assert dims & {"campaign_name", "partner_name"}
    assert not (dims & {"sale_id"})


def test_contains_aggregation():
    sql_with_aggr = [
        "select sum(column) from table",
        "select avg(column) from table",
        "sum(column)",
        "avg(column)",
    ]

    sql_without_aggr = [
        "select column from table",
        "column",
        "a + b",
        "(a) + (b)",
        "(a + b)",
        "sum",
        "sum + avg",
    ]

    for sql in sql_with_aggr:
        assert contains_aggregation(sql)

    for sql in sql_without_aggr:
        assert not contains_aggregation(sql)
