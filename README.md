Zillion: Make sense of it all
=============================

[![Generic badge](https://img.shields.io/badge/Status-Alpha-yellow.svg)](https://shields.io/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![License: MIT](https://img.shields.io/badge/license-MIT-blue)
![Python 3.6+](https://img.shields.io/badge/python-3.6%2B-blue)
[![Downloads](https://static.pepy.tech/badge/zillion)](https://pepy.tech/project/zillion)

**Introduction**
----------------

`Zillion` is a data modeling and analytics tool that allows combining and
analyzing data from multiple datasources through a simple API. It acts as a semantic layer
on top of your data, writes SQL so you don't have to, and easily bolts onto existing
database infrastructure via SQLAlchemy Core. The `Zillion` NLP extension has experimental
support for AI-powered natural language querying and warehouse configuration.

With `Zillion` you can:

* Define a warehouse that contains a variety of SQL and/or file-like
  datasources
* Define or reflect metrics, dimensions, and relationships in your data
* Run multi-datasource reports and combine the results in a DataFrame
* Flexibly aggregate your data with multi-level rollups and table pivots
* Customize or combine fields with formulas
* Apply technical transformations including rolling, cumulative, and rank
  statistics
* Apply automatic type conversions - i.e. get a "year" dimension for free
  from a "date" column
* Save and share report specifications
* Utilize ad hoc or public datasources, tables, and fields to enrich reports
* Query your warehouse with natural language (NLP extension)
* Leverage AI to bootstrap your warehouse configurations (NLP extension)

**Table of Contents**
---------------------

* [Installation](#installation)
* [Primer](#primer)
    * [Metrics and Dimensions](#metrics-and-dimensions)
    * [Warehouse Theory](#warehouse-theory)
    * [Query Layers](#query-layers)
    * [Warehouse Creation](#warehouse-creation)
    * [Executing Reports](#executing-reports)
    * [Natural Language Querying](#natural-language-querying)
    * [Zillion Configuration](#zillion-configuration)
* [Example - Sales Analytics](#example-sales-analytics)
    * [Warehouse Configuration](#example-warehouse-config)
    * [Reports](#example-reports)
* [Advanced Topics](#advanced-topics)
    * [Subreports](#subreports)
    * [FormulaMetrics](#formula-metrics)
    * [Divisor Metrics](#divisor-metrics)
    * [Divisor Metrics](#aggregation-variants)
    * [FormulaDimensions](#formula-dimensions)
    * [DataSource Formulas](#datasource-formulas)
    * [Type Conversions](#type-conversions)
    * [AdHocMetrics](#adhoc-metrics)
    * [AdHocDimensions](#adhoc-dimensions)
    * [AdHocDataTables](#adhoc-data-tables)
    * [Technicals](#technicals)
    * [Config Variables](#config-variables)
    * [DataSource Priority](#datasource-priority)
* [Supported DataSources](#supported-datasources)
* [Multiprocess Considerations](#multiprocess-considerations)
* [Demo UI / Web API](#demo-ui)
* [Docs](#documentation)
* [How to Contribute](#how-to-contribute)

<a name="installation"></a>

**Installation**
----------------

> **Warning**: This project is in an alpha state and is subject to change. Please test carefully for production usage and report any issues.

```shell
$ pip install zillion

or

$ pip install zillion[nlp]
```

---

<a name="primer"></a>

**Primer**
----------

The following is meant to give a quick overview of some theory and
nomenclature used in data warehousing with `Zillion` which will be useful
if you are newer to this area. You can also skip below for a usage [example](#example-sales-analytics) or warehouse/datasource creation [quickstart](#warehouse-creation) options.

In short: `Zillion` writes SQL for you and makes data accessible through a very simple API:

```python
result = warehouse.execute(
    metrics=["revenue", "leads"],
    dimensions=["date"],
    criteria=[
        ("date", ">", "2020-01-01"),
        ("partner", "=", "Partner A")
    ]
)
```

<a name="metrics-and-dimensions"></a>

### **Metrics and Dimensions**

In `Zillion` there are two main types of `Fields` that will be used in
your report requests:

1. `Dimensions`: attributes of data used for labelling, grouping, and filtering
2. `Metrics`: facts and measures that may be broken down along dimensions

A `Field` encapsulates the concept of a column in your data. For example, you
may have a `Field` called "revenue". That `Field` may occur across several
datasources or possibly in multiple tables within a single datasource. `Zillion` 
understands that all of those columns represent the same concept, and it can try 
to use any of them to satisfy reports requesting "revenue".

Likewise there are two main types of tables used to structure your warehouse:

1. `Dimension Tables`: reference/attribute tables containing only related
dimensions
2. `Metric Tables`: fact tables that may contain metrics and some related
dimensions/attributes

Dimension tables are often static or slowly growing in terms of row count and contain
attributes tied to a primary key. Some common examples would be lists of US Zip Codes or
company/partner directories.

Metric tables are generally more transactional in nature. Some common examples
would be records for web requests, ecommerce sales, or stock market price history.

<a name="warehouse-theory"></a>

### **Warehouse Theory**

If you really want to go deep on dimensional modeling and the drill-across
querying technique `Zillion` employs, I recommend reading Ralph Kimball's
[book](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) on data warehousing.

To summarize, [drill-across
querying](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/drilling-across/)
forms one or more queries to satisfy a report request for `metrics` that may
exist across multiple datasources and/or tables at a particular `dimension` grain.

`Zillion` supports flexible warehouse setups such as
[snowflake](https://en.wikipedia.org/wiki/Snowflake_schema) or
[star](https://en.wikipedia.org/wiki/Star_schema) schemas, though it isn't
picky about it. You can specify table relationships through a parent-child
lineage, and `Zillion` can also infer acceptable joins based on the presence
of dimension table primary keys. `Zillion` does not support many-to-many relationships at this time, though most analytics-focused scenarios should be able to work around that by adding views to the model if needed.

<a name="query-layers"></a>

### **Query Layers**

`Zillion` reports can be thought of as running in two layers:

1. `DataSource Layer`: SQL queries against the warehouse's datasources
2. `Combined Layer`: A final SQL query against the combined data from the
DataSource Layer

The Combined Layer is just another SQL database (in-memory SQLite by default)
that is used to tie the datasource data together and apply a few additional
features such as rollups, row filters, row limits, sorting, pivots, and technical computations.

<a name="warehouse-creation"></a>

### **Warehouse Creation**

There are multiple ways to quickly initialize a warehouse from a local or remote file:

```python
# Path/link to a CSV, XLSX, XLS, JSON, HTML, or Google Sheet
# This builds a single-table Warehouse for quick/ad-hoc analysis.
url = "https://raw.githubusercontent.com/totalhack/zillion/master/tests/dma_zip.xlsx"
wh = Warehouse.from_data_file(url, ["Zip_Code"]) # Second arg is primary key

# Path/link to a sqlite database
# This can build a single or multi-table Warehouse
url = "https://github.com/totalhack/zillion/blob/master/tests/testdb1?raw=true"
wh = Warehouse.from_db_file(url)

# Path/link to a WarehouseConfigSchema (or pass a dict)
# This is the recommended production approach!
config = "https://raw.githubusercontent.com/totalhack/zillion/master/examples/example_wh_config.json"
wh = Warehouse(config=config)
```

Zillion also provides a helper script to boostrap a DataSource configuration file for an existing database. See `zillion.scripts.bootstrap_datasource_config.py`. The bootstrap script requires a connection/database url and output file as arguments. See `--help` output for more options, including the optional `--nlp` flag that leverages OpenAI to infer configuration information such as column types, table types, and table relationships. The NLP feature requires the NLP extension to be installed as well as the following set in your `Zillion` config file:

* OPENAI_MODEL
* OPENAI_API_KEY

<a name="executing-reports"></a>

### **Executing Reports**

The main purpose of `Zillion` is to execute reports against a `Warehouse`.
At a high level you will be crafting reports as follows:

```python
result = warehouse.execute(
    metrics=["revenue", "leads"],
    dimensions=["date"],
    criteria=[
        ("date", ">", "2020-01-01"),
        ("partner", "=", "Partner A")
    ]
)
print(result.df) # Pandas DataFrame
```

When comparing to writing SQL, it's helpful to think of the dimensions as the
target columns of a **group by** SQL statement. Think of the metrics as the
columns you are **aggregating**. Think of the criteria as the **where
clause**. Your criteria are applied in the DataSource Layer SQL queries.

The `ReportResult` has a Pandas DataFrame with the dimensions as the index and
the metrics as the columns.

A `Report` is said to have a `grain`, which defines the dimensions each metric
must be able to join to in order to satisfy the `Report` requirements. The
`grain` is a combination of **all** dimensions, including those referenced in
criteria or in metric formulas. In the example above, the `grain` would be
`{date, partner}`. Both "revenue" and "leads" must be able to join to those
dimensions for this report to be possible.

These concepts can take time to sink in and obviously vary with the specifics
of your data model, but you will become more familiar with them as you start
putting together reports against your data warehouses.

<a name="natural-language-querying"></a>

### **Natural Language Querying**

With the NLP extension `Zillion` has experimental support for natural language querying of your data warehouse. For example:

```python
result = warehouse.execute_text("revenue and leads by date last month")
print(result.df) # Pandas DataFrame
```

This NLP feature requires a running instance of Qdrant (vector database) and the following values set in your `Zillion` config file:

* QDRANT_HOST
* OPENAI_API_KEY

Embeddings will be produced and stored in both Qdrant and a local cache. The
vector database will be initialized the first time you try to use this by
analyzing all fields in your warehouse. An example docker file to run Qdrant is provided in the root of this repo.

You have some control over how fields get embedded. Namely in the configuration for any field you can choose whether to exclude a field from embeddings or override which embeddings map to that field. All fields are
included by default. The following example would exclude the `net_revenue` field from being embedded and map `revenue` metric requests to the `gross_revenue` field.

```javascript
{
    "name": "gross_revenue",
    "type": "numeric(10,2)",
    "aggregation": "sum",
    "rounding": 2,
    "meta": {
        "nlp": {
            // enabled defaults to true
            "embedding_text": "revenue" // str or list of str
        }
    }
},
{
    "name": "net_revenue",
    "type": "numeric(10,2)",
    "aggregation": "sum",
    "rounding": 2,
    "meta": {
        "nlp": {
            "enabled": false
        }
    }
},
```

Additionally you may also exclude fields via the following warehouse-level configuration settings:

```javascript
{
    "meta": {
        "nlp": {
            "field_disabled_patterns": [
                // list of regex patterns to exclude
                "rpl_ma_5"
            ],
            "field_disabled_groups": [
                // list of "groups" to exclude, assuming you have
                // set group value in the field's meta dict.
                "No NLP"
            ]
        }
    },
    ...
}
```

If a field is disabled at any of the aforementioned levels it will be ignored. This type of control becomes useful as your data model gets more complex and you want to guide the NLP logic in cases where it could confuse similarly named fields. Any time you adjust which fields are excluded you will want to force recreation of your embeddings collection using the `force_recreate` flag on `Warehouse.init_embeddings`.

> *Note:* This feature is in its infancy. It's usefulness will depend on the
quality of both the input query and your data model (i.e. good field names)!

<a name="zillion-configuration"></a>

### **Zillion Configuration**

In addition to configuring the structure of your `Warehouse`, which will be
discussed further below, `Zillion` has a global configuration to control some
basic settings. The `ZILLION_CONFIG` environment var can point to a yaml config file. See `examples/sample_config.yaml` for more details on what values can be set. Environment vars prefixed with ZILLION_ can override config settings (i.e. ZILLION_DB_URL will override DB_URL).

The database used to store Zillion report specs can be configured by setting the DB_URL value in your `Zillion` config to a valid database connection string. By default a SQLite DB in /tmp is used.

---

<a name="example-sales-analytics"></a>

**Example - Sales Analytics**
-----------------------------

Below we will walk through a simple hypothetical sales data model that
demonstrates basic `DataSource` and `Warehouse` configuration and then shows
some sample [reports](#example-reports). The data is a simple SQLite database
that is part of the `Zillion` test code. For reference, the schema is as
follows:

```sql
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

<a name="example-warehouse-config"></a>

### **Warehouse Configuration**

A `Warehouse` may be created from a JSON or YAML configuration that defines
its fields, datasources, and tables. The code below shows how it can be done in as little as one line of code if you have a pointer to a JSON/YAML `Warehouse` config.

```python
from zillion import Warehouse

wh = Warehouse(config="https://raw.githubusercontent.com/totalhack/zillion/master/examples/example_wh_config.json")
```

This example config uses a `data_url` in its `DataSource` `connect` info that
tells `Zillion` to dynamically download that data and connect to it as a
SQLite database. This is useful for quick examples or analysis, though in most
scenarios you would put a connection string to an existing database like you
see
[here](https://raw.githubusercontent.com/totalhack/zillion/master/tests/test_mysql_ds_config.json)

The basics of `Zillion's` warehouse configuration structure are as follows:

A `Warehouse` config has the following main sections:

* `metrics`: optional list of metric configs for global metrics
* `dimensions`: optional list of dimension configs for global dimensions
* `datasources`: mapping of datasource names to datasource configs or config URLs

A `DataSource` config has the following main sections:

* `connect`: database connection url or dict of connect params
* `metrics`: optional list of metric configs specific to this datasource
* `dimensions`: optional list of dimension configs specific to this datasource
* `tables`: mapping of table names to table configs or config URLs

> Tip: datasource and table configs may also be replaced with a URL that points
to a local or remote config file.

In this example all four tables in our database are included in the config,
two as dimension tables and two as metric tables. The tables are linked
through a parent->child relationship: partners to campaigns, and leads to
sales.  Some tables also utilize the `create_fields` flag to automatically
create `Fields` on the datasource from column definitions. Other metrics and
dimensions are defined explicitly.

To view the structure of this `Warehouse` after init you can use the `print_info`
method which shows all metrics, dimensions, tables, and columns that are part
of your data warehouse:

```python
wh.print_info() # Formatted print of the Warehouse structure
```

For a deeper dive of the config schema please see the full
[docs](https://totalhack.github.io/zillion/zillion.configs/).

<a name="example-reports"></a>

### **Reports**

**Example:** Get sales, leads, and revenue by partner:

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

**Example:** Let's limit to Partner A and break down by its campaigns:

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

**Example:** The output below shows rollups at the campaign level within each
partner, and also a rollup of totals at the partner and campaign level.

> *Note:* the output contains a special character to mark DataFrame rollup rows
that were added to the result. The
[ReportResult](https://totalhack.github.io/zillion/zillion.report/#reportresult)
object contains some helper attributes to automatically access or filter
rollups, as well as a `df_display` attribute that returns the result with
friendlier display values substituted for special characters. The
under-the-hood special character is left here for illustration, but may not
render the same in all scenarios.

```python
from zillion import RollupTypes

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

See the `Report`
[docs](https://totalhack.github.io/zillion/zillion.report/#report) for more
information on supported rollup behavior.

**Example:** Save a report spec (not the data):

First you must make sure you have saved your `Warehouse`, as saved reports
are scoped to a particular `Warehouse` ID. To save a `Warehouse`
you must provide a URL that points to the complete config.

```python
name = "My Unique Warehouse Name"
config_url = <some url pointing to a complete warehouse config>
wh.save(name, config_url) # wh.id is populated after this

spec_id = wh.save_report(
    metrics=["sales", "leads", "revenue"],
    dimensions=["partner_name"]
)
```

> *Note*: If you built your `Warehouse` in python from a list of `DataSources`,
or passed in a `dict` for the `config` param on init, there currently is not
a built-in way to output a complete config to a file for reference when saving.

**Example:** Load and run a report from a spec ID:

```python
result = wh.execute_id(spec_id)
```

This assumes you have saved this report ID previously in the database specified by the DB_URL in your `Zillion` yaml configuration.

**Example:** Unsupported Grain

If you attempt an impossible report, you will get an
`UnsupportedGrainException`. The report below is impossible because it
attempts to break down the leads metric by a dimension that only exists
in a child table. Generally speaking, child tables can join back up to
parents (and "siblings" of parents) to find dimensions, but not the other
way around.

```python
# Fails with UnsupportedGrainException
result = wh.execute(
    metrics=["leads"],
    dimensions=["sale_id"]
)
```

---

<a name="advanced-topics"></a>

**Advanced Topics**
-------------------

<a name="subreports"></a>

### **Subreports**

Sometimes you need subquery-like functionality in order to filter one
report to the results of some other (that perhaps required a different grain).
Zillion provides a simplistic way of doing that by using the `in report` or `not in report`
criteria operations. There are two supported ways to specify the subreport: passing a
report spec ID or passing a dict of report params.

```python
# Assuming you have saved report 1234 and it has "partner" as a dimension:

result = warehouse.execute(
    metrics=["revenue", "leads"],
    dimensions=["date"],
    criteria=[
        ("date", ">", "2020-01-01"),
        ("partner", "in report", 1234)
    ]
)

# Or with a dict:

result = warehouse.execute(
    metrics=["revenue", "leads"],
    dimensions=["date"],
    criteria=[
        ("date", ">", "2020-01-01"),
        ("partner", "in report", dict(
            metrics=[...],
            dimension=["partner"],
            criteria=[...]
        ))
    ]
)
```

The criteria field used in `in report` or `not in report` must be a dimension
in the subreport. Note that subreports are executed at `Report` object initialization
time instead of during `execute` -- as such they can not be killed using `Report.kill`.
This may change down the road.

<a name="formula-metrics"></a>

### **Formula Metrics**

In our example above our config included a formula-based metric called "rpl",
which is simply `revenue / leads`. A `FormulaMetric` combines other metrics
and/or dimensions to calculate a new metric at the Combined Layer of
querying. The syntax must match your Combined Layer database, which is SQLite
in our example.

```json
{
    "name": "rpl",
    "aggregation": "mean",
    "rounding": 2,
    "formula": "{revenue}/{leads}"
}
```

<a name="divisor-metrics"></a>

### **Divisor Metrics**

As a convenience, rather than having to repeatedly define formula metrics for
rate variants of a core metric, you can specify a divisor metric configuration on a non-formula metric. As an example, say you have a `revenue` metric and want to create variants for `revenue_per_lead` and `revenue_per_sale`. You can define your revenue metric as follows:

```json
{
    "name": "revenue",
    "type": "numeric(10,2)",
    "aggregation": "sum",
    "rounding": 2,
    "divisors": {
        "metrics": [
            "leads",
            "sales"
        ]
    }
}
```

See `zillion.configs.DivisorsConfigSchema` for more details on configuration options, such as overriding naming templates, formula templates, and rounding.

<a name="aggregation-variants"></a>

### **Aggregation Variants**

Another minor convenience feature is the ability to automatically generate variants of metrics for different aggregation types in a single field configuration instead of across multiple fields in your config file. As an example, say you have a `sales` column in your data and want to create variants for `sales_mean` and `sales_sum`. You can define your metric as follows:

```json
{
    "name": "sales",
    "aggregation": {
        "mean": {
            "type": "numeric(10,2)",
            "rounding": 2
        },
        "sum": {
            "type": "integer"
        }
    }
}
```

The final config would not have a `sales` metric, but would instead have `sales_mean` and `sales_sum`. Note that you can further customize the settings for the generated fields, such as getting a custom name, by specifying that in the nested settings for that aggregation type. In practice it's not a big savings over just defining the metrics separately, but some may prefer this approach.

<a name="formula-dimensions"></a>

### **Formula Dimensions**

Experimental support exists for `FormulaDimension` fields as well. A `FormulaDimension` can only use other dimensions as part of its formula, and it also gets evaluated in the Combined Layer database. As an additional restriction, a `FormulaDimension` can not be used in report criteria as those filters are evaluated at the DataSource Layer. The following example assumes a SQLite Combined Layer database:


```json
{
    "name": "partner_is_a",
    "formula": "{partner_name} = 'Partner A'"
}
```

<a name="datasource-formulas"></a>

### **DataSource Formulas**

Our example also includes a metric "sales" whose value is calculated via
formula at the DataSource Layer of querying. Note the following in the
`fields` list for the "id" param in the "main.sales" table. These formulas are
in the syntax of the particular `DataSource` database technology, which also
happens to be SQLite in our example.

```json
"fields": [
    "sale_id",
    {"name":"sales", "ds_formula": "COUNT(DISTINCT sales.id)"}
]
```

<a name="type-conversions"></a>

### **Type Conversions**

Our example also automatically created a handful of dimensions from the
"created_at" columns of the leads and sales tables. Support for automatic type
conversions is limited, but for date/datetime columns in supported
`DataSource` technologies you can get a variety of dimensions for free this
way.

The output of `wh.print_info` will show the added dimensions, which are
prefixed with "lead_" or "sale_" as specified by the optional
`type_conversion_prefix` in the config for each table. Some examples of
auto-generated dimensions in our example warehouse include sale_hour,
sale_day_name, sale_month, sale_year, etc. 

As an optimization in the where clause of underlying report queries, `Zillion` 
will try to apply conversions to criteria values instead of columns. For example, 
it is generally more efficient to query as `my_datetime > '2020-01-01' and my_datetime < '2020-01-02'`
instead of `DATE(my_datetime) == '2020-01-01'`, because the latter can prevent index
usage in many database technologies. The ability to apply conversions to values
instead of columns varies by field and `DataSource` technology as well. 

To prevent type conversions, set `skip_conversion_fields` to `true` on your
`DataSource` config.

See `zillion.field.TYPE_ALLOWED_CONVERSIONS` and `zillion.field.DIALECT_CONVERSIONS`
for more details on currently supported conversions.

<a name="adhoc-metrics"></a>

### **Ad Hoc Metrics**

You may also define metrics "ad hoc" with each report request. Below is an
example that creates a revenue-per-lead metric on the fly. These only exist
within the scope of the report, and the name can not conflict with any existing
fields:

```python
result = wh.execute(
    metrics=[
        "leads",
        {"formula": "{revenue}/{leads}", "name": "my_rpl"}
    ],
    dimensions=["partner_name"]
)
```

<a name="adhoc-dimensions"></a>

### **Ad Hoc Dimensions**

You may also define dimensions "ad hoc" with each report request. Below is an
example that creates a dimension that partitions on a particular dimension value on the fly. Ad Hoc Dimensions are a subclass of `FormulaDimension`s and therefore have the same restrictions, such as not being able to use a metric as a formula field. These only exist within the scope of the report, and the name can not conflict with any existing fields:

```python
result = wh.execute(
    metrics=["leads"],
    dimensions=[{"name": "partner_is_a", "formula": "{partner_name} = 'Partner A'"]
)
```

<a name="adhoc-tables"></a>

### **Ad Hoc Tables**

`Zillion` also supports creation or syncing of ad hoc tables in your database
during `DataSource` or `Warehouse` init. An example of a table config that
does this is shown
[here](https://github.com/totalhack/zillion/blob/master/tests/test_adhoc_ds_config.json).
It uses the table config's `data_url` and `if_exists` params to control the
syncing and/or creation of the "main.dma_zip" table from a remote CSV in a
SQLite database.  The same can be done in other database types too.

The potential performance drawbacks to such an approach should be obvious,
particularly if you are initializing your warehouse often or if the remote
data file is large. It is often better to sync and create your data ahead of
time so you have complete schema control, but this method can be very useful
in certain scenarios.

> **Warning**: be careful not to overwrite existing tables in your database!

<a name="technicals"></a>

### **Technicals**

There are a variety of technical computations that can be applied to metrics to
compute rolling, cumulative, or rank statistics. For example, to compute a 5-point
moving average on revenue one might define a new metric as follows:

```json
{
    "name": "revenue_ma_5",
    "type": "numeric(10,2)",
    "aggregation": "sum",
    "rounding": 2,
    "technical": "mean(5)"
}
```

Technical computations are computed at the Combined Layer, whereas the "aggregation"
is done at the DataSource Layer (hence needing to define both above). 

For more info on how shorthand technical strings are parsed, see the
[parse_technical_string](https://totalhack.github.io/zillion/zillion.configs/#parse_technical_string)
code. For a full list of supported technical types see
`zillion.core.TechnicalTypes`.

Technicals also support two modes: "group" and "all". The mode controls how to
apply the technical computation across the data's dimensions. In "group" mode,
it computes the technical across the last dimension, whereas in "all" mode in
computes the technical across all data without any regard for dimensions.

The point of this becomes more clear if you try to do a "cumsum" technical
across data broken down by something like ["partner_name", "date"]. If "group"
mode is used (the default in most cases) it will do cumulative sums *within*
each partner over the date ranges. If "all" mode is used, it will do a
cumulative sum across every data row. You can be explicit about the mode by
appending it to the technical string: i.e. "cumsum:all" or "mean(5):group"

---

<a name="config-variables"></a>

### **Config Variables**

If you'd like to avoid putting sensitive connection information directly in
your `DataSource` configs you can leverage config variables. In your `Zillion`
yaml config you can specify a `DATASOURCE_CONTEXTS` section as follows:

```yaml
DATASOURCE_CONTEXTS:
  my_ds_name:
    user: user123
    pass: goodpassword
    host: 127.0.0.1
    schema: reporting
```

Then when your `DataSource` config for the datasource named "my_ds_name" is
read, it can use this context to populate variables in your connection url:

```json
"datasources": {
    "my_ds_name": {
        "connect": "mysql+pymysql://{user}:{pass}@{host}/{schema}"
        ...
    }
}
```

<a name="datasource-priority"></a>

### **DataSource Priority**

On `Warehouse` init you can specify a default priority order for datasources
by name. This will come into play when a report could be satisfied by multiple
datasources. `DataSources` earlier in the list will be higher priority. This
would be useful if you wanted to favor a set of faster, aggregate tables that
are grouped in a `DataSource`.

```python
wh = Warehouse(config=config, ds_priority=["aggr_ds", "raw_ds", ...])
```

<a name="supported-datasources"></a>

**Supported DataSources**
-------------------------

`Zillion's` goal is to support any database technology that SQLAlchemy
supports (pictured below). That said the support and testing levels in `Zillion` vary at the
moment. In particular, the ability to do type conversions, database
reflection, and kill running queries all require some database-specific code
for support. The following list summarizes known support levels. Your mileage
may vary with untested database technologies that SQLAlchemy supports (it
might work just fine, just hasn't been tested yet). Please report bugs and
help add more support!

* SQLite: supported
* MySQL: supported
* PostgreSQL: supported
* DuckDB: supported
* BigQuery, Redshift, Snowflake, SingleStore, PlanetScale, etc: not tested but would like to support these

SQLAlchemy has connectors to many popular databases. The barrier to support many of these is likely
pretty low given the simple nature of the sql operations `Zillion` uses.

![SQLAlchemy Connectors](https://github.com/totalhack/zillion/blob/master/docs/images/sqlalchemy_connectors.webp?raw=true)

Note that the above is different than the database support for the Combined Layer
database. Currently only SQLite is supported there; that should be sufficient for
most use cases but more options will be added down the road.

<a name="multiprocess-considerations"></a>

**Multiprocess Considerations**
-------------------------------

If you plan to run `Zillion` in a multiprocess scenario, whether on a single
node or across multiple nodes, there are a couple of things to consider:

* SQLite DataSources do not scale well and may run into locking issues with multiple processes trying to access them on the same node.
* Any file-based database technology that isn't centrally accessible would be challenging when using multiple nodes.
* Ad Hoc DataSource and Ad Hoc Table downloads should be avoided as they may conflict/repeat across each process. Offload this to an external
ETL process that is better suited to manage those data flows in a scalable production scenario.

Note that you can still use the default SQLite in-memory Combined Layer DB without issues, as that is made on the fly with each report request and
requires no coordination/communication with other processes or nodes.

<a name="demo-ui"></a>

**Demo UI / Web API**
--------------------

[Zillion Web UI](https://github.com/totalhack/zillion-web) is a demo UI and web API for Zillion that also includes an experimental ChatGPT plugin. See the README there for more info on installation and project structure. Please note that the code is light on testing and polish, but is expected to work in modern browsers. Also ChatGPT plugins are quite slow at the moment, so currently that is mostly for fun and not that useful.

---

<a name="documentation"></a>

**Documentation**
-----------------

More thorough documentation can be found [here](https://totalhack.github.io/zillion/).
You can supplement your knowledge by perusing the [tests](https://github.com/totalhack/zillion/tree/master/tests) directory
or the [API reference](https://totalhack.github.io/zillion/).

---

<a name="how-to-contribute"></a>

**How to Contribute**
---------------------

Please See the
[contributing](https://github.com/totalhack/zillion/blob/master/CONTRIBUTING.md)
guide for more information. If you are looking for inspiration, adding support and tests for additional database technologies would be a great help.



