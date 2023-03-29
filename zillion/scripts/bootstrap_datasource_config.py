"""
This is a helper script to bootstrap the creation of a config for a datasource.
This is useful when you have an existing database that you want to create a 
DataSource or Warehouse for, but would like to save some time on the boilerplate 
work that goes into creating and structuring the config file. This script will reflect
the database schema and do its best to infer the metric, dimension, and table
configurations. 

WARNING: Any secret information (e.g. passwords) in the given connection url will 
be stored in plaintext in the output file. See https://totalhack.github.io/zillion/#config-variables
for more information on how to use config variables as secret placeholders.

WARNING: This is not intended to produce a production-ready config output! It is strongly 
recommended that you review the output and customize to your needs.

"""

import argparse
import getpass
import os

from tlbx import Script, Arg, prompt_user, json, st
import yaml

from zillion.configs import (
    field_safe_name,
    default_field_display_name,
    DataSourceConfigSchema,
)
from zillion.core import (
    set_log_level,
    info,
    warn,
    FieldTypes,
    TableTypes,
    IfFileExistsModes,
)
from zillion.datasource import (
    entity_name_from_file,
    data_url_to_metadata,
    connect_url_to_metadata,
    reflect_metadata,
    DataSource,
)
from zillion.sql_utils import (
    column_fullname,
    is_probably_metric,
    to_generic_sa_type,
    get_nlp_table_info,
    get_nlp_table_relationships,
    infer_aggregation_and_rounding,
)
from zillion.warehouse import Warehouse


set_log_level("INFO")


def get_primary_key(table, full_names=False):
    if full_names:
        return [field_safe_name(column_fullname(x)) for x in table.primary_key.columns]
    return [
        field_safe_name(table.name + "_" + x.name) for x in table.primary_key.columns
    ]


def get_field_name(table, column, full_names=False):
    if full_names:
        return field_safe_name(column_fullname(column))
    return field_safe_name(table.name + "_" + column.name)


def infer_table_relationships(metadata, table_configs, nlp=False):
    # TODO: Also analyze foreign keys on reflected metadata

    if not nlp:
        return

    metric_tables = {
        k for k, v in table_configs.items() if v["type"] == TableTypes.METRIC
    }
    dim_tables = {
        k for k, v in table_configs.items() if v["type"] == TableTypes.DIMENSION
    }

    metric_rel = get_nlp_table_relationships(metadata, metric_tables)
    dim_rel = get_nlp_table_relationships(metadata, dim_tables)
    all_rel = {**metric_rel, **dim_rel}

    for child_column_full, parent_column_full in all_rel.items():
        child_table = ".".join(child_column_full.split(".")[:-1])
        child_column = child_column_full.split(".")[-1]
        child_config = table_configs[child_table]
        if child_config.get("parent", None):
            warn(f"Parent already set for {child_table}, skipping")
            continue

        parent_table = ".".join(parent_column_full.split(".")[:-1])
        parent_config = table_configs[parent_table]
        parent_pk = parent_config["primary_key"]
        if len(parent_pk) != 1:
            warn(f"Multiple primary key columns found for {parent_table}, skipping")
            continue

        parent_pk = parent_pk[0]
        child_config["parent"] = parent_table
        for cname, cconfig in child_config["columns"].items():
            if cname != child_column:
                continue
            fields = cconfig["fields"]
            if parent_pk in fields:
                # Field already references the parent pk
                continue

            fields.append(parent_pk)


def get_configs(
    metadata, tables=None, manual_table_types=False, full_names=False, nlp=False
):
    metrics = []
    dimensions = []
    table_configs = {}

    for table_name, table in metadata.tables.items():
        if tables and table_name not in tables:
            info(f"Skipping table: {table_name}")
            continue

        info(f"---- Table: {table_name}")

        table_type = None
        table_metrics = []
        table_dimensions = []
        table_columns = {}
        primary_key = get_primary_key(table, full_names=full_names)

        nlp_table_info = get_nlp_table_info(table) if nlp else {}

        if manual_table_types:
            type_prompt = prompt_user(
                "Metric table (m), Dimension table (d), or skip (s)?", ["m", "d", "s"]
            )
            if type_prompt == "s":
                continue

            table_type = (
                TableTypes.METRIC if type_prompt == "m" else TableTypes.DIMENSION
            )

        for column in table.c:
            field_name = get_field_name(table, column, full_names=full_names)
            table_columns[str(column.name)] = dict(fields=[field_name])
            coltype = str(to_generic_sa_type(column.type)).lower()
            nlp_column_info = nlp_table_info.get(column.name, {})

            if (
                table_type == TableTypes.METRIC or (not manual_table_types)
            ) and is_probably_metric(column, nlp_column_info=nlp_column_info):
                if table_type is None:
                    info(f"{table_name} inferred as metric table")
                    table_type = TableTypes.METRIC

                field_type = FieldTypes.METRIC
                aggregation = nlp_column_info.get("aggregation", None)
                rounding = nlp_column_info.get("rounding", None)
                _aggregation, _rounding = infer_aggregation_and_rounding(column)
                table_metrics.append(
                    dict(
                        name=field_name,
                        display_name=default_field_display_name(field_name),
                        type=coltype,
                        aggregation=(
                            aggregation.lower() if aggregation != None else _aggregation
                        ),
                        rounding=int(rounding) if rounding != None else _rounding,
                    )
                )
            else:
                field_type = FieldTypes.DIMENSION
                table_dimensions.append(
                    dict(
                        name=field_name,
                        display_name=default_field_display_name(field_name),
                        type=coltype,
                    )
                )

            info(f"Adding {field_type} for {table_name}.{column.name}")

        if table_type is None:
            info(f"{table_name} inferred as dimension table")
            table_type = TableTypes.DIMENSION

        table_configs[table_name] = dict(
            type=table_type,
            primary_key=primary_key,
            columns=table_columns,
        )
        metrics.extend(table_metrics)
        dimensions.extend(table_dimensions)

    return table_configs, metrics, dimensions


class SecureAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        val = getpass.getpass(self.dest + ": ")
        setattr(namespace, self.dest, val)


@Script(
    Arg(
        "url",
        action=SecureAction,
        nargs=0,
        type=str,
        help="Database connection string or url. You will be prompted for this on script run.",
    ),
    Arg("filename", type=str, help="Filename to output config to"),
    Arg(
        "--tables",
        nargs="+",
        default=None,
        help="A list of full table names to filter to (<schema>.<table>)",
    ),
    Arg("--ds-name", type=str, default=None, help="Name to give the DataSource"),
    Arg(
        "--full-names", action="store_true", default=False, help="Use full column names"
    ),
    Arg(
        "--verify",
        action="store_true",
        default=False,
        help="Verify config by loading as a Warehouse to run integrity checks",
    ),
    Arg(
        "--manual-table-types",
        action="store_true",
        default=False,
        help="Prompt to confirm table types instead of inferring from presence of metrics",
    ),
    Arg(
        "--yaml",
        action="store_true",
        dest="use_yaml",
        default=False,
        help="Output config as YAML instead of JSON",
    ),
    Arg("--indent", type=int, default=4, help="Config file indentation"),
    Arg(
        "--nlp",
        action="store_true",
        default=False,
        help="Leverage NLP/AI to bootstrap the configuration. Requires OpenAI settings in your zillion config file.",
    ),
)
def main(
    url,
    filename,
    tables=None,
    ds_name=None,
    full_names=False,
    verify=False,
    manual_table_types=False,
    use_yaml=False,
    indent=None,
    nlp=False,
):
    connect_params = {}

    if os.path.isfile(url) or url.startswith("http"):
        ds_name = ds_name or entity_name_from_file(url)
        metadata = data_url_to_metadata(
            url, ds_name, if_exists=IfFileExistsModes.IGNORE
        )
        connect_params = dict(data_url=url, if_exists=IfFileExistsModes.IGNORE)
    else:
        metadata = connect_url_to_metadata(url, ds_name=ds_name)
        connect_params = dict(connect_url=url)

    reflect_metadata(metadata)

    table_configs, metrics, dimensions = get_configs(
        metadata,
        tables=tables,
        manual_table_types=manual_table_types,
        full_names=full_names,
        nlp=nlp,
    )

    infer_table_relationships(metadata, table_configs, nlp=nlp)

    res = dict(
        connect=dict(params=connect_params),
        metrics=metrics,
        dimensions=dimensions,
        tables=table_configs,
    )
    schema = DataSourceConfigSchema()
    schema.load(res)

    info("Creating Datasource")
    ds = DataSource(ds_name or "bootstrap", config=res)

    with open(filename, "w") as f:
        if use_yaml:
            f.write(yaml.dump(res, indent=indent, sort_keys=False))
        else:
            f.write(json.dumps(res, indent=indent))
    info(f"Wrote output to {filename}")
    warn(
        f"WARNING: your connection string is stored in {filename}! If it has sensitive information you should use datasource context vars with placeholders instead.\nSee https://totalhack.github.io/zillion/#config-variables"
    )

    if verify:
        info("Verifying Warehouse")
        Warehouse(datasources=[ds])


if __name__ == "__main__":
    main()
