"""
This is a helper script to bootstrap the creation of a json config for a datasource.
This is useful when you have an existing, not small schema that you want to create
a warehouse for, but would like to save some time on the boilerplate work that
goes into creating and structuring the config file.

WARNING: This is not intended to produce a production-ready config output. You should
still review and customize the metric, dimension, and table configs and will probably
need to rename fields when there are common fields between tables. It's purely a
convenience to save time.

TODO:
- More CLI options for filtering
- Ability to step through and confirm naming for each table and field
- Print stats about number of tables, metrics, and dimensions added
"""

import argparse
import getpass

from tlbx import Script, Arg, prompt_user, json, st

from zillion.configs import field_safe_name, DataSourceConfigSchema
from zillion.core import AggregationTypes, FieldTypes, TableTypes
from zillion.datasource import connect_url_to_metadata, reflect_metadata
from zillion.sql_utils import column_fullname, is_probably_metric, to_generic_sa_type


class SecureAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        val = getpass.getpass(self.dest + ": ")
        setattr(namespace, self.dest, val)


@Script(
    Arg(
        "conn_url",
        action=SecureAction,
        nargs=0,
        type=str,
        help="Databased connection string",
    ),
    Arg("filename", type=str, help="Filename to write to"),
)
def main(conn_url, filename):
    metadata = connect_url_to_metadata(conn_url)
    reflect_metadata(metadata)

    metrics = []
    dimensions = []
    tables = {}

    for table_name, table in metadata.tables.items():
        print(f"---- Table: {table_name}")
        table_metrics = []
        table_dimensions = []
        table_columns = {}
        primary_key = [
            field_safe_name(column_fullname(x)) for x in table.primary_key.columns
        ]

        type_prompt = prompt_user(
            "Metric table (m), Dimension table (d), or skip (s)?", ["m", "d", "s"]
        )
        if type_prompt == "s":
            continue

        table_type = TableTypes.METRIC if type_prompt == "m" else TableTypes.DIMENSION

        for column in table.c:
            field_name = field_safe_name(column_fullname(column))
            table_columns[column.name] = dict(fields=[field_name])
            coltype = str(to_generic_sa_type(column.type)).lower()
            if table_type == TableTypes.METRIC and is_probably_metric(column):
                field_type = FieldTypes.METRIC
                params = dict(
                    name=field_name, type=coltype, aggregation=AggregationTypes.SUM
                )
                if getattr(column.type, "scale", None):
                    params["rounding"] = column.type.scale
                table_metrics.append(params)
            else:
                field_type = FieldTypes.DIMENSION
                table_dimensions.append(dict(name=field_name, type=coltype))

            print(f"Adding {field_type} for {table_name}.{column.name}")

        tables[table_name] = dict(
            type=table_type, primary_key=primary_key, columns=table_columns
        )
        metrics.extend(table_metrics)
        dimensions.extend(table_dimensions)

    res = dict(metrics=metrics, dimensions=dimensions, tables=tables)
    schema = DataSourceConfigSchema()
    config = schema.load(res)

    with open(filename, "w") as f:
        f.write(json.dumps(res, indent=4))
    print(f"Wrote output to {filename}")


if __name__ == "__main__":
    main()
