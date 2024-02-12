from collections import OrderedDict
import os
import pytest
import time

from marshmallow import ValidationError
from tlbx import json
import yaml

from .test_utils import *
from zillion.configs import load_warehouse_config, load_warehouse_config_from_env
from zillion.core import *
from zillion.datasource import *
from zillion.sql_utils import contains_aggregation, contains_sql_keywords
from zillion.warehouse import Warehouse


def test_zillion_config():
    del os.environ["ZILLION_CONFIG"]
    cfg = load_zillion_config()
    assert cfg["DATASOURCE_CONTEXTS"] == {}

    os.environ["ZILLION_CONFIG"] = "/etc/skeleton/config/zillion/config.yaml"
    cfg = load_zillion_config()
    assert cfg["DATASOURCE_CONTEXTS"] != {}

    os.environ["ZILLION_LOG_LEVEL"] = "INFO"
    cfg = load_zillion_config()
    assert cfg["LOG_LEVEL"] == "INFO"

    os.environ["ZILLION_DEBUG"] = "true"
    cfg = load_zillion_config()
    set_log_level_from_config(cfg)
    assert default_logger.level == logging.DEBUG

    os.environ["ZILLION_DEBUG"] = "false"
    cfg = load_zillion_config()
    set_log_level_from_config(cfg)
    assert default_logger.level == logging.WARNING


def test_wh_config_init(config):
    pass


def test_wh_config_include(config):
    config["includes"] = ["test_include_wh_config.json"]
    wh = Warehouse(config=config)
    wh.get_metric("rpl_include")
    ds = wh.get_datasource("testdb1")
    assert not ds.metadata.tables["main.partners"].zillion.active


def test_datasource_config_init(ds_config):
    ds = DataSource("testdb1", config=ds_config)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_load_remote_wh_config():
    cfg = load_warehouse_config(REMOTE_CONFIG_URL)


def test_wh_from_url_wh_config():
    f = "test_wh_config.json"
    wh = Warehouse(config=f)


def test_wh_from_yaml_url_wh_config():
    json_file = "test_wh_config.json"
    with open(json_file, "r") as jf:
        json_data = json.load(jf)
    yaml_file = json_file.replace("json", "yaml")
    with open(yaml_file, "w") as yf:
        yaml.dump(json_data, yf, indent=2, sort_keys=False)
    wh = Warehouse(config=yaml_file)


def test_load_wh_config_from_env():
    var = "ZILLION_TEST_WH_CONFIG"
    os.environ[var] = REMOTE_CONFIG_URL
    cfg = load_warehouse_config_from_env(var)


def test_wh_save_and_load():
    name = "test_warehouse_%s" % time.time()
    config_url = REMOTE_CONFIG_URL
    wh = Warehouse(config=config_url)
    wh_id = wh.save(name, config_url, meta=dict(test=1))
    wh = Warehouse.load(wh_id)
    assert wh.name == name
    Warehouse.delete(wh_id)


def test_datasource_metadata_init(ds_config):
    metadata = create_test_metadata(ds_config)
    ds = DataSource("testdb1", metadata=metadata)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_metadata_and_config_init(ds_config):
    metadata = create_test_metadata(ds_config)
    del ds_config["connect"]
    # Pass metadata with existing zillion info as well as table config
    ds = DataSource("testdb1", metadata=metadata, config=ds_config)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_from_config(ds_config):
    ds = DataSource("testdb1", config=ds_config)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_skip_conversion_fields(ds_config):
    ds_config["skip_conversion_fields"] = True
    ds = DataSource("testdb1", config=ds_config)
    assert not ds.has_dimension("sale_hour")


def test_datasource_config_data_url(ds_config):
    ds_config["connect"] = {
        "params": {
            "data_url": "https://github.com/totalhack/zillion/blob/master/tests/testdb1?raw=true",
            "if_exists": "replace",
        }
    }
    ds = DataSource("testdb1", config=ds_config)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_config_data_url_replace_after(ds_config):
    ds_config["connect"] = {
        "params": {
            "data_url": "https://github.com/totalhack/zillion/blob/master/tests/testdb1?raw=true",
            "if_exists": "replace_after",
            "replace_after": "0 minutes",
        }
    }
    ds = DataSource("testdb1", config=ds_config)
    fname = "/tmp/testdb1.db"
    mtime = get_modified_time(fname)
    assert (time.time() - mtime) < 5  # Make sure the file is ~new
    assert ds

    ds_config["connect"]["params"]["replace_after"] = "1 minutes"
    ds = DataSource("testdb1", config=ds_config)
    new_mtime = get_modified_time(fname)
    assert new_mtime == mtime  # Make sure it was not replaced


def test_parse_replace_after():
    assert int(parse_replace_after("1 seconds")) == 1
    assert int(parse_replace_after("1 Minutes")) == 60
    assert int(parse_replace_after("10 hours")) == 60 * 60 * 10
    assert int(parse_replace_after("1.2 days")) == 60 * 60 * 24 * 1.2
    assert int(parse_replace_after("0 weeks")) == 0

    with pytest.raises(ZillionException):
        parse_replace_after(" weeks")
    with pytest.raises(ZillionException):
        parse_replace_after("a weeks")
    with pytest.raises(ZillionException):
        parse_replace_after("1 x")


def test_datasource_from_db_file(ds_config):
    ds_name = "testdb1"
    data_url = "https://github.com/totalhack/zillion/blob/master/tests/testdb1?raw=true"
    ds = DataSource.from_db_file(
        data_url, name=ds_name, config=ds_config, if_exists="replace"
    )
    assert ds
    print()  # Format test output
    ds.print_info()

    # Test with local file URL
    ds = DataSource.from_db_file("testdb1", config=ds_config, if_exists="replace")
    assert ds
    print()  # Format test output
    ds.print_info()


def test_datasource_config_table_data_url(adhoc_config):
    ds_config = adhoc_config["datasources"]["test_adhoc_db"]
    ds = DataSource("test_adhoc_db", config=ds_config)
    print()  # Format test output
    ds.print_info()
    assert ds


def test_datasource_metadata_and_table_data_url(ds_config, adhoc_config):
    copyfile("testdb1", "/tmp/testdb1")
    ds_config["connect"] = "sqlite:////tmp/testdb1"
    metadata = create_test_metadata(ds_config)
    del ds_config["connect"]

    drop_metadata_table_if_exists(metadata, "main.dma_zip")
    # Borrow the adhoc table config that has a data_url setup
    adhoc_table_config = adhoc_config["datasources"]["test_adhoc_db"]["tables"][
        "main.dma_zip"
    ]
    ds_config["tables"]["main.dma_zip"] = adhoc_table_config

    try:
        ds = DataSource("testdb1", metadata=metadata, config=ds_config)
        assert "main.dma_zip" in metadata.tables
    finally:
        drop_metadata_table_if_exists(metadata, "main.dma_zip")


def test_datasource_apply_config_table_data_url(ds_config, adhoc_config):
    copyfile("testdb1", "/tmp/testdb1")
    ds_config["connect"] = "sqlite:////tmp/testdb1"
    metadata = create_test_metadata(ds_config)
    del ds_config["connect"]

    drop_metadata_table_if_exists(metadata, "main.dma_zip")

    try:
        ds = DataSource("testdb1", metadata=metadata, config=ds_config)
        # Borrow the adhoc table config that has a data_url setup
        adhoc_table_config = adhoc_config["datasources"]["test_adhoc_db"]["tables"][
            "main.dma_zip"
        ]
        ds_config["tables"]["main.dma_zip"] = adhoc_table_config
        ds.apply_config(ds_config)

        assert "main.dma_zip" in metadata.tables
    finally:
        drop_metadata_table_if_exists(metadata, "main.dma_zip")


def test_warehouse_init(config):
    ds = DataSource("testdb1", config=config["datasources"]["testdb1"])
    wh = Warehouse(config=config, datasources=[ds])
    assert len(wh.datasources) == 2


def test_warehouse_no_config(ds_config):
    ds = DataSource("testdb1", config=ds_config)
    wh = Warehouse(datasources=[ds])
    assert wh.get_dimension_names()
    assert len(wh.datasource_names) == 1


def test_warehouse_has_zillion_info_no_config(ds_config):
    ds = DataSource("testdb1", config=ds_config)
    for table in ds.metadata.tables.values():
        table.info["zillion"].type = TableTypes.METRIC
    wh = Warehouse(datasources=[ds])
    assert not wh.get_datasource("testdb1").dimension_tables


def test_field_display_name(config):
    wh = Warehouse(config=config)
    partner_name = wh.get_field("partner_name")  # Regular dim/field
    assert partner_name.display_name == "Partner Name"
    rpl = wh.get_field("rpl")  # Formula Metric
    assert rpl.display_name == "Revenue/Lead"
    main_sales_created_at = wh.get_field("main_sales_created_at")  # Field from column
    assert main_sales_created_at.display_name == "Main Sales Created At"
    sale_hour = wh.get_field("sale_hour")  # Auto conversion field
    assert sale_hour.display_name == "Sale Hour"


def test_field_description(config):
    wh = Warehouse(config=config)
    partner_name = wh.get_field("partner_name")  # Regular dim/field
    assert partner_name.description is None
    print(partner_name.display_name, partner_name.description)
    rpl = wh.get_field("rpl")  # Formula Metric
    assert rpl.description is not None
    print(rpl.display_name, rpl.description)
    sale_hour = wh.get_field("sale_hour")  # Auto conversion field
    assert sale_hour.description is not None
    print(sale_hour.display_name, sale_hour.description)


def test_field_meta(config):
    wh = Warehouse(config=config)
    field = wh.get_field("rpl")
    assert field.meta and field.meta["metafield"] == "metavalue"


def test_reserved_field_name(config):
    config["datasources"]["testdb1"]["metrics"].append(
        {"name": "row_hash", "type": "integer", "aggregation": AggregationTypes.SUM}
    )
    ds = DataSource("testdb1", config=config["datasources"]["testdb1"])
    with pytest.raises(WarehouseException):
        wh = Warehouse(config=config, datasources=[ds])


def test_field_name_starts_with_number(config):
    config["datasources"]["testdb1"]["metrics"].append(
        {"name": "1test", "type": "integer", "aggregation": AggregationTypes.SUM}
    )
    with pytest.raises(ValidationError):
        ds = DataSource("testdb1", config=config["datasources"]["testdb1"])


def test_dimension_to_config(config):
    wh = Warehouse(config=config)
    field = wh.get_dimension("partner_name")
    info(field.to_config())


def test_metric_to_config(config):
    wh = Warehouse(config=config)
    field = wh.get_metric("revenue")
    cfg = field.to_config()
    assert "formula" not in cfg
    info(cfg)


def test_formula_metric_to_config(config):
    wh = Warehouse(config=config)
    field = wh.get_metric("rpl")
    cfg = field.to_config()
    assert "type" not in cfg
    info(cfg)


def test_get_metric_configs(config):
    wh = Warehouse(config=config)
    metrics = wh.get_metric_configs()
    assert "revenue" in metrics


def test_get_dimension_configs(config):
    wh = Warehouse(config=config)
    dims = wh.get_dimension_configs()
    assert "partner_name" in dims


def test_dimension_copy(config):
    wh = Warehouse(config=config)
    field = wh.get_dimension("partner_name")
    info(field.copy())


def test_metric_copy(config):
    wh = Warehouse(config=config)
    field = wh.get_metric("revenue")
    info(field.copy())


def test_formula_metric_copy(config):
    wh = Warehouse(config=config)
    field = wh.get_metric("rpl")
    info(field.copy())


def test_warehouse_technical_within_formula(config):
    config["metrics"].append(
        {
            "name": "revenue_ma_5_sum_5",
            "aggregation": AggregationTypes.SUM,
            "rounding": 2,
            "formula": "{revenue_ma_5}/{revenue_sum_5}",
        }
    )
    with pytest.raises(InvalidFieldException):
        wh = Warehouse(config=config)


def test_warehouse_metric_divisor(config):
    wh = Warehouse(config=config)
    # These are dynamically generated from the divisors config on the revenue metric
    assert wh.has_metric("revenue_per_lead")
    assert wh.has_metric("revenue_per_sale")


def test_warehouse_metric_multiple_aggregations(config):
    wh = Warehouse(config=config)
    # These are dynamically generated for metrics that have multiple aggregations
    assert wh.has_metric("sales_sum_custom_name")
    assert wh.has_metric("sales_variant_mean")


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
    assert wh.has_dimension("Zip_Code")


def test_warehouse_remote_google_sheet(adhoc_config):
    url = "https://docs.google.com/spreadsheets/d/1iCzY4av_tinUpG2Q0mQhbxd77XOREwfbPAVZPObzFeE/edit?usp=sharing"
    adhoc_config["datasources"]["test_adhoc_db"]["tables"]["main.dma_zip"][
        "data_url"
    ] = url
    wh = Warehouse(config=adhoc_config)
    assert wh.has_dimension("Zip_Code")


def test_warehouse_remote_xlsx_table(adhoc_config):
    url = (
        "https://raw.githubusercontent.com/totalhack/zillion/master/tests/dma_zip.xlsx"
    )
    adhoc_config["datasources"]["test_adhoc_db"]["tables"]["main.dma_zip"][
        "data_url"
    ] = url
    wh = Warehouse(config=adhoc_config)
    assert wh.has_dimension("Zip_Code")


def test_warehouse_remote_json_table(adhoc_config):
    url = (
        "https://raw.githubusercontent.com/totalhack/zillion/master/tests/dma_zip.json"
    )
    adhoc_config["datasources"]["test_adhoc_db"]["tables"]["main.dma_zip"][
        "data_url"
    ] = url
    wh = Warehouse(config=adhoc_config)
    assert wh.has_dimension("Zip_Code")


def test_warehouse_remote_html_table(adhoc_config):
    url = (
        "https://raw.githubusercontent.com/totalhack/zillion/master/tests/dma_zip.html"
    )
    adhoc_config["datasources"]["test_adhoc_db"]["tables"]["main.dma_zip"][
        "data_url"
    ] = url
    wh = Warehouse(config=adhoc_config)
    wh.print_info()
    assert wh.has_dimension("Zip_Code")


def test_reuse_existing_remote_table(adhoc_config):
    ds_name = "test_adhoc_db"
    ds_configs = adhoc_config["datasources"]

    for ds_name, ds_config in ds_configs.items():
        for table in ds_config["tables"].values():
            table["if_exists"] = IfExistsModes.IGNORE

    ds = DataSource(ds_name, config=ds_configs[ds_name])
    ds = DataSource(ds_name, config=ds_configs[ds_name])
    for ds_name, ds_config in ds_configs.items():
        for table in ds_config["tables"].values():
            table["if_exists"] = IfExistsModes.FAIL
    with pytest.raises(ValueError):
        dsf = DataSource(ds_name, config=ds_configs[ds_name])


def test_bad_table_data_url(ds_config):
    ds_config["tables"]["main.sales"]["data_url"] = "test"
    with pytest.raises(AssertionError):
        ds = DataSource("test", config=ds_config)


def test_warehouse_from_data_file():
    url = (
        "https://raw.githubusercontent.com/totalhack/zillion/master/tests/dma_zip.xlsx"
    )
    wh = Warehouse.from_data_file(url, ["Zip_Code"])
    wh.print_info()
    assert wh.has_dimension("Zip_Code")


def test_warehouse_from_db_file():
    data_url = "https://github.com/totalhack/zillion/blob/master/tests/testdb1?raw=true"
    wh = Warehouse.from_db_file(data_url, if_exists="replace")
    wh.print_info()


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
    assert dim.sa_type.length == 50


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
        if_exists=IfExistsModes.REPLACE,
        schema="main",
        use_full_column_names=True,
    )
    ds = DataSource.from_datatables("adhoc_ds", [dt])
    assert ds.has_dimension("main_adhoc_table1_partner_name")
    ds.print_info()


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
        if_exists=IfExistsModes.REPLACE,
        schema="main",
    )
    ds = DataSource.from_datatables("adhoc_ds", [dt])
    assert ds.has_dimension("partner_name")
    assert not ds.has_metric("adhoc_metric")


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
        if_exists=IfExistsModes.REPLACE,
        schema="main",
    )
    ds = DataSource.from_datatables("adhoc_ds", [dt])
    ds.print_info()
    assert "Zip_Code" in ds.get_dimensions()


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
        if_exists=IfExistsModes.REPLACE,
        schema="main",
    )
    ds = DataSource.from_datatables("adhoc_ds", [dt])
    dims = ds.get_dimensions()
    assert "Zip_Code" in dims
    assert "DMA_Description" not in dims


def test_json_datatable():
    name = "dma_zip"
    file_name = "dma_zip.json"
    primary_key = ["Zip_Code"]

    dt = JSONDataTable(
        name,
        file_name,
        TableTypes.DIMENSION,
        primary_key=primary_key,
        if_exists=IfExistsModes.REPLACE,
        schema="main",
        use_full_column_names=True,
    )
    ds = DataSource.from_datatables("adhoc_ds", [dt])
    assert "main_dma_zip_Zip_Code" in ds.get_dimensions()


def test_html_datatable():
    name = "dma_zip"
    file_name = "dma_zip.html"
    primary_key = ["Zip_Code"]

    dt = HTMLDataTable(
        name,
        file_name,
        TableTypes.DIMENSION,
        primary_key=primary_key,
        if_exists=IfExistsModes.REPLACE,
        schema="main",
        use_full_column_names=True,
    )
    ds = DataSource.from_datatables("adhoc_ds", [dt])
    assert "main_dma_zip_Zip_Code" in ds.get_dimensions()
