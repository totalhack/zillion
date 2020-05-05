Zillion: Make sense of it all
=============================

[![Generic badge](https://img.shields.io/badge/Status-Alpha-yellow.svg)](https://shields.io/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Documentation Status](https://readthedocs.org/projects/zillion/badge/?version=latest)](https://zillion.readthedocs.io/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![Python 3](https://img.shields.io/badge/python-3-blue.svg)

Introduction
------------

`Zillion` is a free, open data warehousing and dimensional modeling tool that
allows combining and analyzing data from multiple datasources through a simple
API. It writes SQL so you don't have to, and it easily bolts onto existing
database infrastructure that can be understood by SQLALchemy.

With `Zillion` you can:

- Define a warehouse that contains a variety of SQL and/or file-like
  datasources
- Define (or reflect) metrics and dimensions in your data and establish table
  relationships
- Define formulas at the datasource level or based on combinations of metrics
  and dimensions
- Query multiple datasources at once and combine the results in a simple
  result DataFrame
- Flexibly aggregate your data with multi-level rollups and table pivots
- Apply technical transformations on columns including rolling, cumulative,
  and rank statistics
- Save and load report specifications
- Optional automatic type conversions - i.e. get a "year" dimension for free
  from a "date" column
- Utilize "adhoc" datasources and fields to enrich specific report requests

Why `Zillion`? There are many commercial solutions out there that
provide data warehousing solutions. Many of them are cost-prohibitive, come with
strings attached or vendor lock-in, and are overly heavyweight. `Zillion` aims
to be a more democratic solution to data warehousing, and to provide powerful
data analysis capabilities to all.

Table of Contents
-----------------

* [Installation](#installation)
* [Primer](#primer)
  * [Theory](#theory)
  * [Query Layers](#query-layers)
  * [Metrics and Dimensions](#metrics-and-dimensions)
  * [Executing Reports](#executing-reports)
* [Simple Example](#simple-example)
  * [Configuration](#example-configuration)
  * [Reports](#example-reports)
* [Advanced Topics](#advanced-topics)
  * [FormulaMetrics](#formula-metrics)
  * [DataSource Formulas](#datasource-formulas)
  * [AdHocMetrics](#adhoc-metrics)
  * [AdHocDataSources](#adhoc-datasources)
  * [Type Conversions](#type-conversions)
  * [Config Overrides](#config-overrides)
  * [Config Variables](#config-variables)
  * [Remote Configs](#remote-configs)
  * [DataSource Priority](#datasource-priority)
* [Supported DataSources](#supported-datasources)
* [Docs](#documentation)
* [How to Contribute](#how-to-contribute)

<a name="installation"></a>
Installation
------------

> ⚠️ **Warning**: This project is in an alpha state and is rapidly changing.

```shell
$ pip install zillion
```

<a name="primer"></a>
Primer
------

Please also see the [docs](https://zillion.readthedocs.io/en/latest/) for more
details, or skip below for examples.

<a name="theory"></a>
### Theory

For background on dimensional modeling and the drill-across querying
technique `Zillion` employs, I recommend reading Ralph Kimball's [book](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) on
data warehousing.

To summarize, [drill-across querying](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/drilling-across/) forms one
or more queries to satisfy a report request for metrics that may exist across
multiple metric tables (sometimes called "facts" instead of "metrics").

`Zillion` supports [snowflake](https://en.wikipedia.org/wiki/Snowflake_schema)
or [star](https://en.wikipedia.org/wiki/Star_schema) schemas. You can specify
table relationships through a parent-child lineage, and `Zillion` can also
infer acceptable joins based on the presence of dimension table primary keys.

<a name="query-layers"></a>
### Query Layers

`Zillion` reports can be thought of as running in two layers:

1. DataSource Layer: SQL queries against the warehouse's datasources
2. Combined Layer: A final SQL query against the combined data from the
DataSource Layer

The Combined Layer is just another SQL database (in-memory SQLite by default)
that is used to tie the datasource data together and apply a few additional
features such as rollups, row filters, pivots, and technical computations.
Metric formulas may be defined at the DataSource Layer or the Combined Layer,
and in either case must adhere to the SQL dialect of the underlying database.
We'll get into this with examples later.

<a name="metrics-and-dimensions"></a>
### Metrics and Dimensions

In `Zillion` there are two main types of `Fields`:

1. `Dimensions`: attributes of data used for labelling, grouping, and filtering
2. `Metrics`: facts and measures that may be broken down along Dimensions

Note that a `Field` is not the same thing as a column. You can think of
a column as an instance of a `Field` in a particular table, with all of the
specifics of that table/datasource that come with it. A `Field` is more like
a class of a column.

For example, you may have a `Field` called "revenue". That `Field` may occur
across several datasources, possibly in multiple tables within a single
datasource, as specific table columns, possibly with different column names too.
`Zillion` understands that all of those columns represent the same concept, and
it can try to use any of them to satisfy reports requesting "revenue".

Likewise there are two main types of tables:

1. Dimension Tables: reference/attribute tables containing only related
dimensions.
2. Metric Tables: fact tables that may contain metrics and some related
dimensions/attributes.

<a name="executing-reports"></a>
### Executing Reports

The main purpose of `Zillion` is to execute reports against a `Warehouse`.
We'll get into more examples later, but at a high level you will be crafting
reports as follows:

```python
wh = Warehouse(...)

result = wh.execute(
    metrics=["revenue", "leads"],
    dimensions=["date"],
    criteria=[
        ("date", ">", "2020-01-01"),
        ("partner", "=", "Partner A")
    ]
)

print(result.df) # Pandas DataFrame
```

The `ReportResult` has a Pandas DataFrame with the dimensions as the index and
the metrics as the columns. For a report like the one above it's possible two
DataSource Layer queries were run (one for revenue and one for leads) if they
happen to be in different metric tables. Your criteria are applied in the
DataSource Layer queries.  All of the SQL is written for you.

A `Report` is said to have a `grain`, which defines the dimensions each metric
must be able to join to in order to satisfy the `Report` requirements. The
`grain` is a combination of **all** dimensions, including those referenced in
criteria or in metric formulas. In the example above, the `grain` would be
`{date, partner}`.  Both "revenue" and "leads" must be able to join to those
dimensions for this report to be possible.

<a name="simple-example"></a>
Simple Example
--------------

Below we will walk through a simple example that demonstrates basic
`DataSource` and `Warehouse` configuration and then shows some sample reports.
The data is a SQLite database that is part of the `Zillion` unit test [code](https://github.com/totalhack/zillion/blob/master/tests/testdb1). The schema is as follows:

```
CREATE TABLE partners (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE campaigns (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE,
  category VARCHAR NOT NULL,
  partner_id INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE leads (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  campaign_id INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE sales (
  id INTEGER PRIMARY KEY,
  item VARCHAR NOT NULL,
  quantity INTEGER NOT NULL,
  revenue DECIMAL(10, 2),
  lead_id INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

<a name="example-configuration"></a>
### Configuration

A `Warehouse` config has the following main sections:

* metrics: optional list of metric configs for global metrics
* dimensions: optional list of dimension configs for global dimensions
* datasources: mapping of datasource names to datasource configs

A `DataSource` config has the following main sections:

* url: connection string for a DataSource
* metrics: optional list of metric configs specific to this DataSource
* dimensions: optional list of dimension configs specific to this DataSource
* tables: mapping of table names to table configs

In this example we will use a JSON config file. You can also set your
config directly on your SQLAlchemy metadata, but we'll save that for a later
example.

The JSON config file we will use to init our `Warehouse` is located
[here](https://raw.githubusercontent.com/totalhack/zillion/master/tests/example_wh_config.json).
For a deeper dive of the config schema please see the full
[docs](https://zillion.readthedocs.io/en/latest/). For now let's summarize:

* There are lists of metrics and dimensions defined at the warehouse level. This
includes defining the name and type of the field, as well as the aggregation
and rounding settings for metrics.
* It has a single SQLite datasource named "testdb1". The connection string
assumes the DB file is in the current working directory. This must be valid
SQLAlchemy connection string.
* All four tables in our database are included in the config, two as dimension
tables and two as metric tables. The tables are linked through a parent->child
relationship: partners to campaigns, and leads to sales.
* Some tables utilize the `create_fields` flag to automatically create fields
on the datasource from column definitions.

To initialize a `Warehouse` from this config:

```python
config = load_warehouse_config("example_wh_config.json")
wh = Warehouse(config=config)
wh.print_info() # Formatted print of the Warehouse structure
```

The output of `print_info` above would show which tables and columns
are part of the `Warehouse` and which fields they support. It also lists
all of the metrics and dimensions at the `Warehouse` and `DataSource` levels.

<a name="example-reports"></a>
### Reports

**Note**: the test data in this sample database is not meant to mimic any
real world example. The numbers are just made up for testing.

Get sales, leads, and revenue by partner:

```python
result = wh.execute(
    metrics=["sales", "leads", "revenue"],
    dimensions=["partner_name"]
)

print(result.df)
"""
              sales  leads  revenue
partner_name
Partner A        11      4    165.0
Partner B         2      2     19.0
Partner C         5      1    118.5
"""
```

Let's limit to Partner A and break down by its campaigns:

```python
result = wh.execute(
    metrics=["sales", "leads", "revenue"],
    dimensions=["campaign_name"],
    criteria=[("partner_name", "=", "Partner A")]
)

print(result.df)
"""
               sales  leads  revenue
campaign_name
Campaign 1A        5      2       83
Campaign 2A        6      2       82
"""
```

Let's get a multi-level rollup by partner and campaign. You'll notice the
rollup index placeholder in the output. This is a special character to mark
DataFrame rows that represent rollups. The output below shows rollups at the
campaign level within each partner, and also a totals rollup at the partner
and campaign level.

```python
from zillion.core import RollupTypes

result = wh.execute(
    metrics=["sales", "leads", "revenue"],
    dimensions=["partner_name", "campaign_name"],
    rollup=RollupTypes.ALL
)

print(result.df)
"""
                            sales  leads  revenue
partner_name campaign_name
Partner A    Campaign 1A      5.0    2.0     83.0
             Campaign 2A      6.0    2.0     82.0
             􏿿               11.0    4.0    165.0
Partner B    Campaign 1B      1.0    1.0      6.0
             Campaign 2B      1.0    1.0     13.0
             􏿿                2.0    2.0     19.0
Partner C    Campaign 1C      5.0    1.0    118.5
             􏿿                5.0    1.0    118.5
􏿿            􏿿               18.0    7.0    302.5
"""
```

Save a report spec (not the data):

```python
spec_id = wh.save_report(
    metrics=["sales", "leads", "revenue"],
    dimensions=["partner_name"]
)
```

Load and run a report from a spec ID:

```python
result = wh.execute_id(spec_id)
```

> Note: The ZILLION_CONFIG environment var can point to a yaml config file.
The database used to store Zillion report specs can be configured by
setting the ZILLION_DB_URL value in your Zillion config to a valid database
connection string. By default a SQLite DB in /tmp is used.

If you attempt an impossible report, you will get an
`UnsupportedGrainException`. The report below is impossible because it
attempts to breakdown the leads metric by a dimension that only exists
in a child table. Generally speaking, child tables can join back up to
parents to find dimensions, but not the other way around.

```python
result = wh.execute(
    metrics=["leads"],
    dimensions=["sale_id"]
)
```

<a name="advanced-topics"></a>
Advanced Topics
---------------

<a name="formula-metrics"></a>
### FormulaMetrics

In our example above our config included a formula-based metric called "rpl",
which is simply revenue / leads. FormulaMetrics combine other metrics and/or
dimensions to calculate a new metric at the Combined Layer of querying. The
syntax must match your Combined Layer database, which is SQLite in our example.

```
{
    "name": "rpl",
    "aggregation": "MEAN",
    "rounding": 2,
    "formula": "{revenue}/{leads}"
}
```

<a name="datasource-formulas"></a>
### DataSource Formulas

Our example also includes a metric "sales" whose value is calculated via
formula at the DataSource Layer of querying. Note the following in the
`fields` list for the "id" param in the "main.sales" table. These formulas are
in the syntax of the particular `DataSource` database technology, which also
happens to be SQLite in our example.

```
"fields": [
    "sale_id",
    {"name":"sales", "ds_formula": "COUNT(DISTINCT sales.id)"}
]
```

<a name="type-conversions"></a>
### Type Conversions

Our example also automatically created a handful of dimensions from the
"created_at" columns of the leads and sales tables. Support for type
conversions is limited, but for date/datetime type columns in supported
`DataSource` technologies you can get a variety of dimensions for free this
way.

The output of `wh.print_info` will show the added dimensions, which are
prefixed with "lead_" or "sale_" as specified by the optional
`type_conversion_prefix` in the config for each table. Some examples include
sale_hour, sale_day_name, sale_day_of_month, sale_month, sale_year, etc.

<a name="config-variables"></a>
### Config Variables

If you'd like to avoid putting sensitive connection information directly in
your `DataSource` configs you can leverage config variables. In your `Zillion`
config you can specify a DATASOURCE_CONTEXTS section as follows:

```
DATASOURCE_CONTEXTS:
  my_ds_name:
    user: user123
    pass: goodpassword
    host: 127.0.0.1
    schema: reporting
```

Then when your `DataSource` config for the `DataSource` named "my_ds_name" is
read, it can use this context to populate variables in your connection url:

```
    "datasources": {
        "my_ds_name": {
            "url": "mysql+pymysql://{user}:{pass}@{host}/{schema}"
            ...
        }
    }
```

<a name="DataSource Priority"></a>
### DataSource Priority

On `Warehouse` init you can specify a default priority order for datasources
by name. This will come into play when a report could be satisfied by multiple
datasources.  `DataSources` earlier in the list will be higher priority. This
would be useful if you wanted to favor a set of faster, aggregate tables that
are grouped in a `DataSource`.

```python
wh = Warehouse(config=config, ds_priority=["ds1", "ds2", ...])
```

<a name="adhoc-metrics"></a>
### AdHocMetrics

You may also define metrics "adhoc" with each report request. Below is an
example that creates a revenue-per-lead metric on the fly. These only exist
within the scope of the report, and the name can not conflict with any existig
fields:

```python
result = wh.execute(
    metrics=[
        "revenue",
        "leads",
        {"formula": "{revenue}/{leads}", "name": "my_rpl"}
    ],
    dimensions=["partner_name"]
)
```

<a name="supported-datasources"></a>
Supported DataSources
---------------------

`Zillion's` goal is to support any database technology that SQLAlchemy
supports. That said the support and testing levels in `Zillion` vary at the
moment. In particular, the ability to do type conversions, database
reflection, and kill running queries all require some database specific code
for support. The following list summarizes support:

* SQLite: supported and tested
* MySQL: supported and tested
* PostgreSQL: supported and *very lightly* tested
* MSSQL: not tested
* Oracle: not tested
* BigQuery, Redshift, Snowflake, etc: not tested

Note that this is different than the database support for the Combined Layer
database. Currently only SQLite is supported there, though it is planned to
make this more generic such that any SQLAlchemy supported database could be
used.

<a name="documentation"></a>
Documentation
-------------

More thorough documentation can be found [here](https://zillion.readthedocs.io/en/latest/).
You can supplement your knowledge by perusing the [tests](https://github.com/totalhack/zillion/tree/master/tests) directory
or the [module reference](https://zillion.readthedocs.io/en/latest/zillion.html).

<a name="how-to-contribute"></a>
How to Contribute
-----------------

See the [CONTRIBUTING](https://github.com/totalhack/zillion/blob/master/CONTRIBUTING.md) guide.
