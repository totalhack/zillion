from collections import OrderedDict
import pytest
import time

import sqlalchemy as sa

from .test_utils import *
from zillion.configs import TableInfo, ColumnInfo
from zillion.core import *
from zillion.datasource import *
from zillion.sql_utils import contains_aggregation, contains_sql_keywords
from zillion.warehouse import Warehouse


def test_wh_config_init(config):
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
    metadata.reflect(schema="main")

    # Create zillion info directly on metadata
    partners_info = TableInfo.create(
        dict(type=TableTypes.DIMENSION, create_fields=True, primary_key=["partner_id"])
    )
    campaigns_info = TableInfo.create(
        dict(type=TableTypes.DIMENSION, create_fields=True, primary_key=["campaign_id"])
    )

    metadata.tables["main.partners"].info["zillion"] = partners_info
    metadata.tables["main.campaigns"].info["zillion"] = campaigns_info

    ds = DataSource("testdb1", metadata=metadata)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_metadata_and_config_init(config):
    ds_config = config["datasources"]["testdb1"]
    metadata = sa.MetaData()
    metadata.bind = sa.create_engine(ds_config["url"])
    metadata.reflect(schema="main")

    # Create zillion info directly on metadata
    partners_info = TableInfo.create(
        dict(type=TableTypes.DIMENSION, create_fields=True, primary_key=["partner_id"])
    )
    campaigns_info = TableInfo.create(
        dict(type=TableTypes.DIMENSION, primary_key=["campaign_id"])
    )
    metadata.tables["main.partners"].info["zillion"] = partners_info
    metadata.tables["main.campaigns"].info["zillion"] = campaigns_info

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
    assert len(wh.datasource_names) == 1


def test_warehouse_has_zillion_info_no_config(config):
    ds = DataSource.from_config("testdb1", config["datasources"]["testdb1"])
    for table in ds.metadata.tables.values():
        table.info["zillion"].type = TableTypes.METRIC
    wh = Warehouse(datasources=[ds])
    assert not wh.get_datasource("testdb1").dimension_tables


def test_reserved_field_name(config):
    config["datasources"]["testdb1"]["metrics"].append(
        {"name": "row_hash", "type": "Integer", "aggregation": AggregationTypes.SUM}
    )
    ds = DataSource.from_config("testdb1", config["datasources"]["testdb1"])
    with pytest.raises(WarehouseException):
        wh = Warehouse(config=config, datasources=[ds])


def test_warehouse_technical_within_formula(config):
    config["metrics"].append(
        {
            "name": "revenue_ma_5_sum_5",
            "type": "Numeric(10,2)",
            "aggregation": AggregationTypes.SUM,
            "rounding": 2,
            "formula": "{revenue_ma_5}/{revenue_sum_5}",
        }
    )
    with pytest.raises(InvalidFieldException):
        wh = Warehouse(config=config)


def test_warehouse_remote_datasource_config(config):
    config["datasources"][
        "testdb2"
    ] = "https://raw.githubusercontent.com/totalhack/zillion/master/tests/test_sqlite_ds_config.json"
    wh = Warehouse(config=config)
    assert wh.has_metric("aggr_sales")


def test_warehouse_remote_csv_table(adhoc_config):
    table_config = adhoc_config["datasources"]["test_adhoc_db"]["tables"][
        "main.dma_zip"
    ]
    table_config["adhoc_table_options"] = {"nrows": 30}
    wh = Warehouse(config=adhoc_config)
    try:
        assert wh.has_dimension("Zip_Code")
    finally:
        wh.clean_up()


def test_warehouse_remote_google_sheet(adhoc_config):
    url = "https://docs.google.com/spreadsheets/d/1iCzY4av_tinUpG2Q0mQhbxd77XOREwfbPAVZPObzFeE/edit?usp=sharing"
    adhoc_config["datasources"]["test_adhoc_db"]["tables"]["main.dma_zip"]["url"] = url
    wh = Warehouse(config=adhoc_config, if_exists="ignore")
    try:
        assert wh.has_dimension("Zip_Code")
    finally:
        wh.clean_up()


def test_warehouse_remote_xlsx_table(adhoc_config):
    url = (
        "https://raw.githubusercontent.com/totalhack/zillion/master/tests/dma_zip.xlsx"
    )
    adhoc_config["datasources"]["test_adhoc_db"]["tables"]["main.dma_zip"]["url"] = url
    wh = Warehouse(config=adhoc_config)
    try:
        assert wh.has_dimension("Zip_Code")
    finally:
        wh.clean_up()


def test_warehouse_remote_json_table(adhoc_config):
    url = (
        "https://raw.githubusercontent.com/totalhack/zillion/master/tests/dma_zip.json"
    )
    adhoc_config["datasources"]["test_adhoc_db"]["tables"]["main.dma_zip"]["url"] = url
    wh = Warehouse(config=adhoc_config)
    try:
        assert wh.has_dimension("Zip_Code")
    finally:
        wh.clean_up()


def test_warehouse_remote_html_table(adhoc_config):
    url = (
        "https://raw.githubusercontent.com/totalhack/zillion/master/tests/dma_zip.html"
    )
    adhoc_config["datasources"]["test_adhoc_db"]["tables"]["main.dma_zip"]["url"] = url
    wh = Warehouse(config=adhoc_config)
    try:
        wh.print_info()
        assert wh.has_dimension("Zip_Code")
    finally:
        wh.clean_up()


def test_reuse_existing_remote_table(adhoc_config):
    ds_name = "test_adhoc_db"
    ds_configs = adhoc_config["datasources"]
    try:
        ds = datasource_from_config(ds_name, ds_configs[ds_name], if_exists="ignore")
        ds = datasource_from_config(ds_name, ds_configs[ds_name], if_exists="ignore")
        with pytest.raises(ValueError):
            dsf = datasource_from_config(ds_name, ds_configs[ds_name], if_exists="fail")
    finally:
        ds.clean_up()


def test_adhoc_config_to_ds_init(adhoc_config):
    ds_config = adhoc_config["datasources"]["test_adhoc_db"]
    with pytest.raises(ZillionException):
        ds = DataSource("test", config=ds_config)


def test_adhoc_table_url_to_ds_init(config):
    ds_config = config["datasources"]["testdb1"]
    ds_config["tables"]["main.sales"]["url"] = "test"
    with pytest.raises(ZillionException):
        ds = DataSource("test", config=ds_config, reflect=True)


def test_column_config_override(config):
    table_config = config["datasources"]["testdb1"]["tables"]["main.sales"]
    table_config["columns"]["revenue"]["active"] = False
    wh = Warehouse(config=config)
    assert not "sales" in wh.get_datasource("testdb1").get_tables_with_field("revenue")


def test_table_config_override(config):
    table_config = config["datasources"]["testdb1"]["tables"]["main.sales"]
    table_config["active"] = False
    wh = Warehouse(config=config)
    assert not wh.get_datasource("testdb1").has_table("main.sales")


def test_no_create_fields_no_columns(config):
    table_config = config["datasources"]["testdb1"]["tables"]["main.partners"]
    del table_config["columns"]
    table_config["create_fields"] = False
    with pytest.raises(ZillionException):
        wh = Warehouse(config=config)


def test_no_create_fields_has_columns(config):
    del config["datasources"]["testdb2"]
    campaigns_config = config["datasources"]["testdb1"]["tables"]["main.campaigns"]
    campaigns_config["create_fields"] = False
    # This will raise an error because the fields referenced in the columns'
    # field lists don't exist
    with pytest.raises(WarehouseException):
        wh = Warehouse(config=config)


def test_no_create_fields_field_exists_has_columns(config):
    del config["datasources"]["testdb2"]
    table_config = config["datasources"]["testdb1"]["tables"]["main.partners"]
    table_config["create_fields"] = False
    wh = Warehouse(config=config)
    # ds_partner_name was already defined, make sure it doesnt get overwritten
    dim = wh.get_dimension("ds_partner_name")
    assert dim.type.length == 50


def test_create_fields_no_columns(config):
    del config["datasources"]["testdb2"]
    table_config = config["datasources"]["testdb1"]["tables"]["main.partners"]
    table_config["create_fields"] = True
    del table_config["columns"]
    table_config["primary_key"] = ["fake_field"]
    # Primary key mismatch in parent/child relationship with partners/campaigns
    with pytest.raises(ZillionException):
        wh = Warehouse(config=config)


def test_create_fields_has_columns(config):
    del config["datasources"]["testdb2"]
    table_config = config["datasources"]["testdb1"]["tables"]["main.partners"]
    table_config["create_fields"] = True
    wh = Warehouse(config=config)
    assert wh.has_dimension("partner_id")
    assert wh.has_dimension("partner_name")
    assert not wh.has_dimension("partners_name")
    # This one is auto-generated with create_fields, so it has a default naming
    # style:
    assert wh.has_dimension("main_partners_created_at")


def test_get_dimension_table_set(wh):
    possible = [{"partner_id", "partner_name"}, {"campaign_name", "partner_name"}]

    impossible = [
        {"lead_id", "partner_id"},
        {"sale_id", "lead_id", "campaign_name", "partner_name"},
    ]

    for grain in possible:
        ts = wh.get_dimension_table_set(grain, grain)

    for grain in impossible:
        with pytest.raises(UnsupportedGrainException):
            ts = wh.get_dimension_table_set(grain, grain)


def test_get_metric_table_set(wh):
    possible = [
        ("leads", {"partner_id", "partner_name"}),
        ("leads", {"campaign_name", "partner_name"}),
        ("revenue", {"campaign_name", "partner_name", "lead_id"}),
    ]

    impossible = [("leads", {"sale_id"})]

    for metric, grain in possible:
        wh.get_metric_table_set(metric, grain, grain)

    for metric, grain in impossible:
        with pytest.raises(UnsupportedGrainException):
            wh.get_metric_table_set(metric, grain, grain)


def test_get_supported_dimensions(wh):
    metrics = ["leads", "main_sales_quantity"]
    dims = wh._get_supported_dimensions(metrics)
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


def test_contains_sql_keyword():
    sql_with_keywords = [
        "select col from table",
        "drop table",
        "delete from *",
        # "where 1",
        # "column",
    ]

    sql_without_keywords = [
        "distinct",
        "avg(`select`)",
        "ifnull(col)",
        "avg(col)",
        "col",
        "a + b",
        "(a) + (b)",
        "(a + b)",
    ]

    for sql in sql_with_keywords:
        assert contains_sql_keywords(sql)

    for sql in sql_without_keywords:
        assert not contains_sql_keywords(sql)


def test_adhoc_datatable_no_columns():
    size = 10
    column_types = dict(partner_name=str, adhoc_metric=float)
    data = create_adhoc_data(column_types, size)

    name = "adhoc_table1"
    primary_key = ["partner_name"]

    dt = AdHocDataTable(
        name,
        data,
        TableTypes.METRIC,
        primary_key=primary_key,
        # With this setup it creates fields for all columns in the table
        columns=None,
        schema="main",
    )

    ds = AdHocDataSource([dt], name="adhoc_ds", if_exists="replace")
    try:
        assert ds.has_dimension("main_adhoc_table1_partner_name")
    finally:
        ds.clean_up()


def test_adhoc_datatable_has_columns():
    size = 10
    column_types = dict(partner_name=str, adhoc_metric=float)
    data = create_adhoc_data(column_types, size)

    name = "adhoc_table1"
    primary_key = ["partner_name"]

    columns = OrderedDict(partner_name={"fields": ["partner_name"]})

    dt = AdHocDataTable(
        name,
        data,
        TableTypes.METRIC,
        primary_key=primary_key,
        # With this setup it will only create fields for columns specified
        columns=columns,
        schema="main",
    )

    ds = AdHocDataSource([dt], name="adhoc_ds", if_exists="replace")
    try:
        assert ds.has_dimension("partner_name")
        assert not ds.has_metric("adhoc_metric")
    finally:
        ds.clean_up()


def test_csv_datatable():
    name = "dma_zip"
    file_name = "dma_zip.csv"
    primary_key = ["Zip_Code"]

    columns = OrderedDict(
        Zip_Code={"fields": ["Zip_Code"]}, DMA_Code={"fields": ["DMA_Code"]}
    )

    dt = CSVDataTable(
        name,
        file_name,
        TableTypes.DIMENSION,
        primary_key=primary_key,
        columns=columns,
        schema="main",
    )
    ds = AdHocDataSource([dt], "adhoc_ds", if_exists="replace")

    ds.print_info()

    try:
        assert "Zip_Code" in ds.get_dimensions()
    finally:
        ds.clean_up()


def test_excel_datatable():
    name = "dma_zip"
    file_name = "dma_zip.xlsx"
    primary_key = ["Zip_Code"]

    columns = OrderedDict(
        Zip_Code={"fields": ["Zip_Code"]}, DMA_Code={"fields": ["DMA_Code"]}
    )

    dt = ExcelDataTable(
        name,
        file_name,
        TableTypes.DIMENSION,
        primary_key=primary_key,
        columns=columns,
        schema="main",
    )
    ds = AdHocDataSource([dt], "adhoc_ds", if_exists="replace")

    try:
        dims = ds.get_dimensions()
        assert "Zip_Code" in dims
        assert "DMA_Description" not in dims
    finally:
        ds.clean_up()


def test_json_datatable():
    name = "dma_zip"
    file_name = "dma_zip.json"
    primary_key = ["Zip_Code"]

    dt = JSONDataTable(
        name, file_name, TableTypes.DIMENSION, primary_key=primary_key, schema="main"
    )
    ds = AdHocDataSource([dt], "adhoc_ds", if_exists="replace")

    try:
        assert "main_dma_zip_Zip_Code" in ds.get_dimensions()
    finally:
        ds.clean_up()


def test_html_datatable():
    name = "dma_zip"
    file_name = "dma_zip.html"
    primary_key = ["Zip_Code"]

    dt = HTMLDataTable(
        name, file_name, TableTypes.DIMENSION, primary_key=primary_key, schema="main"
    )
    ds = AdHocDataSource([dt], "adhoc_ds", if_exists="replace")

    try:
        assert "main_dma_zip_Zip_Code" in ds.get_dimensions()
    finally:
        ds.clean_up()
