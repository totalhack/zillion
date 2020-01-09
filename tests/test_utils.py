import copy
import logging
import random

from tlbx import st, dbg, random_string

from zillion.configs import load_warehouse_config
from zillion.core import TableTypes, AggregationTypes
from zillion.datasource import DataSource, AdHocDataSource, AdHocDataTable
from zillion.report import Report
from zillion.warehouse import Warehouse


DEFAULT_TEST_DB = "testdb1"
TEST_CONFIG = load_warehouse_config("test_config.json")

logging.getLogger().setLevel(logging.INFO)


def create_adhoc_data(column_types, size):
    data = []

    def get_random_value(coltype):
        if coltype == str:
            return random_string()
        elif coltype == int:
            return random.randint(0, 1e2)
        elif coltype == float:
            return random.random() * 1e2
        else:
            assert False, "Unsupported column type: %s" % coltype

    for i in range(0, int(size)):
        row = dict()
        for column_name, column_type in column_types.items():
            row[column_name] = get_random_value(column_type)
        data.append(row)

    return data


def get_adhoc_datasource():
    table_config = {
        "type": TableTypes.METRIC,
        "autocolumns": False,
        "columns": {
            "partner_name": {"fields": ["partner_name"]},
            "adhoc_metric": {"fields": ["adhoc_metric"]},
        },
    }

    name = "adhoc_table1"
    primary_key = ["partner_name"]
    size = 10
    column_types = dict(partner_name=str, adhoc_metric=float)

    dt = create_adhoc_datatable(name, table_config, primary_key, column_types, size)

    config = {
        "metrics": [
            {
                "name": "adhoc_metric",
                "type": "Numeric(10,2)",
                "aggregation": AggregationTypes.SUM,
            }
        ],
        "dimensions": [{"name": "partner_name", "type": "String(32)"}],
    }

    adhoc_ds = AdHocDataSource([dt], config=config)
    return adhoc_ds


def create_adhoc_datatable(name, table_config, primary_key, column_types, size):
    assert (
        table_config["columns"].keys() == column_types.keys()
    ), "Mismatch between table_config columns and column_types"

    data = create_adhoc_data(column_types, size)
    dt = AdHocDataTable(name, data, primary_key, table_config)
    return dt


def get_testdb_url(dbname=DEFAULT_TEST_DB):
    return "sqlite:///%s" % dbname
