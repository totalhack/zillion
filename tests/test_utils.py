import copy
import random

from tlbx import st, dbg, random_string

from zillion.configs import load_warehouse_config
from zillion.core import TableTypes
from zillion.warehouse import DataSource, AdHocDataSource, AdHocDataTable, Warehouse


DEFAULT_TEST_DB = "testdb1"
TEST_CONFIG = load_warehouse_config("test_config.json")


def init_datasources():
    ds1 = DataSource("testdb1", get_testdb_url("testdb1"), reflect=True)
    ds2 = DataSource("testdb2", get_testdb_url("testdb2"), reflect=True)
    # ds2 will end up with a higher priority
    return [ds2, ds1]


def create_adhoc_data(column_defs, size):
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
        for column_name, column_def in column_defs.items():
            row[column_name] = get_random_value(column_def.get("type", str))
        data.append(row)

    return data


def get_adhoc_datasource():
    column_defs = {
        "partner_name": {"fields": ["partner_name"], "type": str},
        "adhoc_fact": {"fields": ["adhoc_fact"], "type": float},
    }

    size = 10
    dt = create_adhoc_datatable(
        "adhoc_table1", TableTypes.FACT, column_defs, ["partner_name"], size
    )
    adhoc_ds = AdHocDataSource([dt])
    return adhoc_ds


def create_adhoc_datatable(
    name, table_type, column_defs, primary_key, size, parent=None
):
    data = create_adhoc_data(column_defs, size)
    column_defs = copy.deepcopy(column_defs)
    for column_name, column_def in column_defs.items():
        if "type" in column_def:
            # The column schema doesn't allow this column
            del column_def["type"]
    dt = AdHocDataTable(
        name, table_type, primary_key, data, columns=column_defs, parent=parent
    )
    return dt


def get_testdb_url(dbname=DEFAULT_TEST_DB):
    return "sqlite:///%s" % dbname
