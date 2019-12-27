import pytest

import sqlalchemy as sa
from tlbx import dbg, st

from .test_utils import *
from zillion.configs import TableInfo, ColumnInfo
from zillion.core import UnsupportedGrainException, TableTypes
from zillion.datasource import DataSource
from zillion.sql_utils import contains_aggregation
from zillion.warehouse import Warehouse


def test_config_init(config):
    pass


def test_datasource_config_init(config):
    """This inits a DataSource from a connection URL, reflects the metadata,
    and applies a table config"""
    ds = DataSource("testdb1", config=config["datasources"]["testdb1"], reflect=True)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_metadata_init(config):
    ds_config = config["datasources"]["testdb1"]
    metadata = sa.MetaData()
    metadata.bind = sa.create_engine(ds_config["url"])
    metadata.reflect()

    # Create zillion info directly on metadata
    partners_info = TableInfo.create(dict(type="dimension", autocolumns=True))
    campaigns_info = TableInfo.create(dict(type="dimension"))
    metadata.tables["partners"].info["zillion"] = partners_info
    metadata.tables["campaigns"].info["zillion"] = campaigns_info

    ds = DataSource("testdb1", metadata=metadata)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_metadata_and_config_init(config):
    ds_config = config["datasources"]["testdb1"]
    metadata = sa.MetaData()
    metadata.bind = sa.create_engine(ds_config["url"])
    metadata.reflect()

    # Create zillion info directly on metadata
    partners_info = TableInfo.create(dict(type="dimension", autocolumns=True))
    campaigns_info = TableInfo.create(dict(type="dimension"))
    metadata.tables["partners"].info["zillion"] = partners_info
    metadata.tables["campaigns"].info["zillion"] = campaigns_info

    del ds_config["url"]

    # Pass metadata with existing zillion info as well as table config
    ds = DataSource("testdb1", metadata=metadata, config=ds_config)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_from_config(config):
    ds = DataSource.from_config("testdb1", config["datasources"]["testdb1"])
    print()  # Format test output
    ds.print_info()
    assert ds


def test_warehouse_init(config):
    ds = DataSource.from_config("testdb1", config["datasources"]["testdb1"])
    wh = Warehouse(config=config, datasources=[ds])
    assert len(wh.datasources) == 2


def test_warehouse_no_config(config):
    ds = DataSource.from_config("testdb1", config["datasources"]["testdb1"])
    wh = Warehouse(datasources=[ds])
    assert wh.get_dimension_names()
    assert len(wh.get_datasource_names()) == 1


def test_warehouse_has_zillion_info_no_config(config):
    ds = DataSource.from_config("testdb1", config["datasources"]["testdb1"])
    for table in ds.metadata.tables.values():
        table.info["zillion"].type = TableTypes.METRIC
    wh = Warehouse(datasources=[ds])
    assert not wh.datasources["testdb1"].dimension_tables


def test_column_config_override(config):
    table_config = config["datasources"]["testdb1"]["tables"]["sales"]
    table_config["columns"]["revenue"]["active"] = False
    wh = Warehouse(config=config)
    assert not "sales" in wh.datasources["testdb1"].get_tables_with_field("revenue")


def test_get_dimension_table_set(wh):
    possible = [{"partner_id", "partner_name"}, {"campaign_name", "partner_name"}]

    impossible = [
        {"lead_id", "partner_id"},
        {"sale_id", "lead_id", "campaign_name", "partner_name"},
    ]

    for grain in possible:
        ts = wh.get_dimension_table_set(grain)

    for grain in impossible:
        with pytest.raises(UnsupportedGrainException):
            ts = wh.get_dimension_table_set(grain)


def test_get_metric_table_set(wh):
    possible = [
        ("leads", {"partner_id", "partner_name"}),
        ("leads", {"campaign_name", "partner_name"}),
        ("revenue", {"campaign_name", "partner_name", "lead_id"}),
    ]

    impossible = [("leads", {"sale_id"})]

    for metric, grain in possible:
        wh.get_metric_table_set(metric, grain)

    for metric, grain in impossible:
        with pytest.raises(UnsupportedGrainException):
            wh.get_metric_table_set(metric, grain)


def test_get_supported_dimensions(wh):
    metrics = ["leads", "sales_quantity"]
    dims = wh.get_supported_dimensions(metrics)
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
