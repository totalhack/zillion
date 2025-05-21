"""
To setup testing against local databases:

mysql -u root -h 127.0.0.1 < zillion_test.mysql.sql
psql -h 127.0.0.1 -U postgres zillion_test < zillion_test.postgres.sql

rm /tmp/adhoc_large_db.db (if it exists)
"""

from collections import OrderedDict
from contextlib import contextmanager
import copy
import logging
import os
import random
import sys
from shutil import copyfile

import pymysql
from tlbx import st, random_string, shell
import sqlalchemy as sa

from zillion.configs import (
    load_warehouse_config,
    load_datasource_config,
    zillion_config,
    TableInfo,
)
from zillion.core import *
from zillion.datasource import (
    DataSource,
    AdHocDataTable,
    SQLiteDataTable,
    get_adhoc_datasource_filename,
    get_adhoc_datasource_url,
)
from zillion.field import DATETIME_CONVERSION_FIELDS
from zillion.model import *
from zillion.report import Report
from zillion.warehouse import Warehouse


if os.path.exists("tests"):
    sys.exit("ERROR: Please run tests from within the tests directory")

DEFAULT_TEST_DB = "testdb1"
TEST_WH_CONFIG = load_warehouse_config("test_wh_config.json")
TEST_ADHOC_CONFIG = load_warehouse_config("test_adhoc_ds_config.json")
REMOTE_CONFIG_URL = "https://raw.githubusercontent.com/totalhack/zillion/master/tests/test_wh_config.json"

logging.getLogger().setLevel(logging.INFO)
default_logger.setLevel(logging.INFO)

logging.info(f"Config: {zillion_config}")
test_config = zillion_config["TEST"]


@contextmanager
def update_zillion_config(updates):
    """Helper to make temporary updates to global config"""
    old = {k: v for k, v in zillion_config.items() if k in updates}
    try:
        zillion_config.update(updates)
        yield
    finally:
        zillion_config.update(old)


def mysql_data_init():
    host = test_config["MySQLHost"]
    port = int(test_config["MySQLPort"])
    user = test_config["MySQLUser"]
    cmd = f"mysql -u {user} -h {host} -P {port}"
    cmd += " < setup/zillion_test.mysql.sql"
    res = shell(cmd)
    assert res.returncode == 0, f"Error initializing MySQL: {res.stderr}"


def postgresql_data_init():
    host = test_config["PostgreSQLHost"]
    port = int(test_config["PostgreSQLPort"])
    user = test_config["PostgreSQLUser"]
    schema = test_config["PostgreSQLTestSchema"]
    cmd = (
        f"psql -U {user} -h {host} -p {port} {schema} < setup/zillion_test.postgres.sql"
    )
    res = shell(cmd)
    assert res.returncode == 0, f"Error initializing PostgreSQL: {res.stderr}"


def duckdb_data_init(conn):
    for table in ["partners", "campaigns", "partner_sibling", "leads", "sales"]:
        print(f"Creating table {table}")
        conn.execute(
            f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('setup/{table}.csv')"
        )


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


def get_sqlalchemy_mysql_conn():
    engine = get_sqlalchemy_mysql_engine()
    return engine.connect()


def get_sqlalchemy_postgresql_engine():
    host = test_config["PostgreSQLHost"]
    port = int(test_config["PostgreSQLPort"])
    user = test_config["PostgreSQLUser"]
    schema = test_config["PostgreSQLTestSchema"]
    conn_str = "postgresql+psycopg2://%(user)s@%(host)s:%(port)s/%(schema)s" % locals()
    engine = sa.create_engine(conn_str)
    return engine


def get_sqlalchemy_postgresql_conn():
    engine = get_sqlalchemy_postgresql_engine()
    return engine.connect()


def get_sqlalchemy_duckdb_engine():
    schema = test_config["DuckDBTestSchema"]
    conn_str = f"duckdb:///{schema}"
    engine = sa.create_engine(conn_str)
    return engine


def get_sqlalchemy_duckdb_conn():
    engine = get_sqlalchemy_duckdb_engine()
    return engine.connect()


# NOTE: defaults to mysql, should be cleaned up
def get_sql(sql):
    conn = get_pymysql_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()


def create_test_metadata(ds_config):
    metadata = sa.MetaData()
    metadata.bind = sa.create_engine(ds_config["connect"])
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
    return metadata


def drop_metadata_table_if_exists(metadata, table_name):
    if table_name not in metadata.tables:
        return
    table = metadata.tables[table_name]
    table.drop()
    metadata.remove(table)


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
    assert table_config["columns"].keys() == column_types.keys(), (
        "Mismatch between table_config columns and column_types"
    )

    data = create_adhoc_data(column_types, size)

    dt = AdHocDataTable(
        name,
        data,
        table_config["type"],
        primary_key=primary_key,
        columns=table_config.get("columns", None),
        if_exists=table_config.get("if_exists", IfExistsModes.FAIL),
        schema="main",
    )
    return dt


def get_adhoc_table_config():
    table_config = {
        "type": TableTypes.METRIC,
        "if_exists": IfExistsModes.REPLACE,
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
        fname = get_adhoc_datasource_filename(name)
        if os.path.exists(fname):
            info("Reusing datasource file %s" % fname)
            dt = SQLiteDataTable(
                name,
                None,
                table_config["type"],
                primary_key=primary_key,
                columns=table_config.get("columns", None),
                schema="main",
            )
            return DataSource.from_datatables(name, [dt], config=config)

    dt = create_adhoc_datatable(name, table_config, primary_key, column_types, size)
    return DataSource.from_datatables(name, [dt], config=config)


def get_testdb_url(dbname=DEFAULT_TEST_DB):
    return "sqlite:///%s" % dbname


EXPECTED_DATE_CONVERSION_VALUES = [
    ("campaign_year", 2019),
    ("campaign_quarter", "2019-Q1"),
    ("campaign_quarter_of_year", 1),
    ("campaign_month", "2019-03"),
    ("campaign_month_name", "March"),
    ("campaign_month_of_year", 3),
    ("campaign_week", "2019-W13"),
    ("campaign_week_of_year", 13),
    ("campaign_week_of_month", 5),
    ("campaign_period_of_month_7d", 4),
    ("campaign_date", "2019-03-26"),
    ("campaign_day_name", "Tuesday"),
    ("campaign_day_of_week", 2),
    ("campaign_is_weekday", 1),
    ("campaign_day_of_month", 26),
    ("campaign_day_of_year", 85),
    ("campaign_hour", "2019-03-26 21:00:00"),
    ("campaign_hour_of_day", 21),
    ("campaign_minute", "2019-03-26 21:02:00"),
    ("campaign_minute_of_hour", 2),
    ("campaign_datetime", "2019-03-26 21:02:15"),
]


def get_date_conversion_test_params():
    metrics = None
    criteria = [("campaign_name", "=", "Campaign 2B")]
    dimensions = ["campaign_%s" % v.name for v in DATETIME_CONVERSION_FIELDS]
    return dict(metrics=metrics, criteria=criteria, dimensions=dimensions)


CRITERIA_CONVERSION_TESTS = [
    # TODO: add value validation for every case
    ("campaign_date", "=", "2019-03-26"),
    ("campaign_date", "!=", "2019-03-25"),
    ("campaign_date", ">", "2019-03-25"),
    ("campaign_date", ">=", "2019-03-26"),
    ("campaign_date", "<", "2019-03-27"),
    ("campaign_date", "<=", "2019-03-26"),
    ("campaign_date", "between", ["2019-03-26", "2019-03-27"]),
    ("campaign_date", "not between", ["2019-03-25", "2019-03-25"]),
    #
    ("campaign_year", "=", "2019"),
    ("campaign_year", "!=", "2020"),
    ("campaign_year", ">", "2018"),
    ("campaign_year", ">=", "2019"),
    ("campaign_year", "<", "2020"),
    ("campaign_year", "<=", "2019"),
    ("campaign_year", "between", ["2019", "2020"]),
    ("campaign_year", "not between", ["2017", "2018"]),
    #
    ("campaign_month", "=", "2019-03"),
    ("campaign_month", "!=", "2019-02"),
    ("campaign_month", ">", "2019-02"),
    ("campaign_month", ">=", "2019-03"),
    ("campaign_month", "<", "2019-04"),
    ("campaign_month", "<=", "2019-03"),
    ("campaign_month", "between", ["2019-03", "2019-03"]),
    ("campaign_month", "not between", ["2019-01", "2019-02"]),
    #
    ("campaign_hour", "=", "2019-03-26 21:00:00"),
    ("campaign_hour", "!=", "2019-03-26 20:00:00"),
    ("campaign_hour", ">", "2019-03-26 04:00:00"),
    ("campaign_hour", ">=", "2019-03-26 21:00:00"),
    ("campaign_hour", "<", "2019-03-26 22:00:00"),
    ("campaign_hour", ">=", "2019-03-26 21:00:00"),
    ("campaign_hour", "between", ["2019-03-26 20:00:00", "2019-03-26 22:00:00"]),
    ("campaign_hour", "not between", ["2019-03-26 04:00:00", "2019-03-26 05:00:00"]),
    #
    ("campaign_minute", "=", "2019-03-26 21:02:00"),
    ("campaign_minute", "!=", "2019-03-26 21:01:00"),
    ("campaign_minute", ">", "2019-03-26 21:01:00"),
    ("campaign_minute", ">=", "2019-03-26 21:02:00"),
    ("campaign_minute", "<", "2019-03-26 21:03:00"),
    ("campaign_minute", ">=", "2019-03-26 21:02:00"),
    ("campaign_minute", "between", ["2019-03-26 21:01:00", "2019-03-26 21:02:00"]),
    ("campaign_minute", "not between", ["2019-03-26 21:01:00", "2019-03-26 21:01:00"]),
    #
    ("campaign_datetime", "=", "2019-03-26 21:02:15"),
    ("campaign_datetime", "!=", "2019-03-26 21:02:00"),
    ("campaign_datetime", ">", "2019-03-26 21:02:10"),
    ("campaign_datetime", ">=", "2019-03-26 21:02:15"),
    ("campaign_datetime", "<", "2019-03-26 21:02:16"),
    ("campaign_datetime", "<=", "2019-03-26 21:02:15"),
    ("campaign_datetime", "between", ["2019-03-26 21:02:15", "2019-03-26 21:02:16"]),
    (
        "campaign_datetime",
        "not between",
        ["2019-03-26 21:00:01", "2019-03-26 21:00:02"],
    ),
]


def wh_execute_args(d):
    exec_params = set(
        [
            "metrics",
            "dimensions",
            "criteria",
            "row_filters",
            "rollup",
            "pivot",
            "order_by",
            "limit",
            "limit_first",
            "adhoc_datasources",
            "allow_partial",
            "disabled_tables",
        ]
    )
    return {k: v for k, v in d.items() if k in exec_params}


def wh_execute(wh, d):
    return wh.execute(**wh_execute_args(d))
