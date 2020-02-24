from collections import OrderedDict
from contextlib import contextmanager
import copy
import logging
import os
import random

import pymysql
from tlbx import st, dbg, info, random_string
import sqlalchemy as sa

from zillion.configs import load_warehouse_config, zillion_config
from zillion.core import TableTypes, AggregationTypes
from zillion.datasource import (
    DataSource,
    AdHocDataSource,
    AdHocDataTable,
    SQLiteDataTable,
)
from zillion.report import Report
from zillion.warehouse import Warehouse


DEFAULT_TEST_DB = "testdb1"
TEST_WH_CONFIG = load_warehouse_config("test_wh_config.json")
TEST_ADHOC_CONFIG = load_warehouse_config("adhoc_ds_config.json")
test_config = zillion_config["TEST"]

logging.getLogger().setLevel(logging.INFO)


@contextmanager
def update_zillion_config(updates):
    """Helper to make temporary updates to global config"""
    old = {k: v for k, v in zillion_config.items() if k in updates}
    try:
        zillion_config.update(updates)
        yield
    finally:
        zillion_config.update(old)


def get_pymysql_conn():
    host = test_config["MySQLHost"]
    port = int(test_config["MySQLPort"])
    user = test_config["MySQLUser"]
    password = test_config.get("MySQLPassword", None)
    schema = test_config["MySQLTestSchema"]
    conn = pymysql.connect(
        host=host,
        port=port,
        db=schema,
        user=user,
        passwd=password,
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn


def get_sqlalchemy_mysql_engine():
    host = test_config["MySQLHost"]
    port = int(test_config["MySQLPort"])
    user = test_config["MySQLUser"]
    password = test_config.get("MySQLPassword", None)
    schema = test_config["MySQLTestSchema"]
    if host in ["localhost", "127.0.0.1"] and not password:
        conn_str = "mysql+pymysql://%(user)s@%(host)s/%(schema)s" % locals()
    else:
        conn_str = (
            "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(schema)s"
            % locals()
        )
    engine = sa.create_engine(conn_str)
    return engine


def get_sqlalchemy_conn():
    engine = get_sqlalchemy_mysql_engine()
    return engine.connect()


def get_sql(sql):
    conn = get_pymysql_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()


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


def create_adhoc_datatable(name, table_config, primary_key, column_types, size):
    assert (
        table_config["columns"].keys() == column_types.keys()
    ), "Mismatch between table_config columns and column_types"

    data = create_adhoc_data(column_types, size)

    dt = AdHocDataTable(
        name,
        data,
        table_config["type"],
        primary_key=primary_key,
        columns=table_config.get("columns", None),
    )
    return dt


def get_adhoc_table_config():
    table_config = {
        "type": TableTypes.METRIC,
        "create_fields": False,
        "columns": OrderedDict(
            partner_name={"fields": ["partner_name"]},
            adhoc_metric={"fields": ["adhoc_metric"]},
        ),
    }
    return table_config


def get_dma_zip_table_config():
    table_config = {
        "type": TableTypes.DIMENSION,
        "create_fields": True,
        "columns": OrderedDict(
            Zip_Code={"fields": ["zip_code"]},
            DMA_Code={"fields": ["dma_code"]},
            DMA_Description={"fields": ["dma_description"]},
        ),
    }
    return table_config


def get_adhoc_datasource(size=10, name="adhoc_table1", reuse=False):
    table_config = get_adhoc_table_config()
    primary_key = ["partner_name"]
    column_types = dict(partner_name=str, adhoc_metric=float)
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

    if reuse:
        fname = AdHocDataSource.get_datasource_filename(name)
        if os.path.exists(fname):
            info("Reusing datasource file %s" % fname)
            dt = SQLiteDataTable(
                name,
                AdHocDataSource.get_datasource_url(name),
                table_config["type"],
                primary_key=primary_key,
                columns=table_config.get("columns", None),
            )
            return AdHocDataSource([dt], name=name, config=config)

    dt = create_adhoc_datatable(name, table_config, primary_key, column_types, size)
    return AdHocDataSource([dt], name=name, config=config)


def get_testdb_url(dbname=DEFAULT_TEST_DB):
    return "sqlite:///%s" % dbname
