#!/usr/bin/env python
"""Helper script to run reports in the command line"""

import ast
import logging

from tlbx import Script, Arg, raiseif, st

from zillion.core import info, set_log_level, nlp_installed
from zillion.datasource import DataSource
from zillion.warehouse import Warehouse


@Script(
    Arg("config", help="Path to warehouse config file"),
    Arg(
        "-ds",
        "--ds-config",
        action="store_true",
        default=False,
        help="Interpret the config as a DataSource config and create a Warehouse from the DataSource",
    ),
    Arg("-m", "--metrics", nargs="+", help="Metrics to include in report"),
    Arg("-d", "--dimensions", nargs="+", help="Dimensions to include in report"),
    Arg("-c", "--criteria", help="String to be eval'd as criteria param"),
    Arg("-o", "--order-by", help="String to be eval'd as order by param"),
    Arg("-r", "--rollup", help="Rollup setting"),
    Arg("-rf", "--row-filters", help="String to be eval'd as row filter param"),
    Arg("-l", "--limit", type=int, help="Limit setting"),
    Arg(
        "-t",
        "--text",
        type=str,
        help="Execute a report from natural language text. Requires the NLP extension.",
    ),
    Arg("-ll", "--log-level", type=int, default=logging.INFO, help="Set log level"),
)
def main(
    config=None,
    ds_config=False,
    metrics=None,
    dimensions=None,
    criteria=None,
    order_by=None,
    row_filters=None,
    rollup=None,
    limit=None,
    text=None,
    log_level=None,
):
    if log_level:
        set_log_level(log_level)

    raiseif(
        text and not nlp_installed,
        "The NLP extension is not installed. Please install it with `pip install zillion[nlp]` to use the --text option.",
    )

    if ds_config:
        ds = DataSource("bootstrap", config=config)
        wh = Warehouse(datasources=[ds])
    else:
        wh = Warehouse(config=config)

    if text:
        info(f"Executing report from text: {text}")
        result = wh.execute_text(text)
    else:
        if criteria:
            criteria = ast.literal_eval(criteria)
        if order_by:
            order_by = ast.literal_eval(order_by)
        if row_filters:
            row_filters = ast.literal_eval(row_filters)

        params = dict(
            metrics=metrics,
            dimensions=dimensions,
            criteria=criteria,
            order_by=order_by,
            row_filters=row_filters,
            rollup=rollup,
            limit=limit,
        )
        info("Executing report with params:")
        info(params)
        result = wh.execute(**params)

    info(result.df_display)


if __name__ == "__main__":
    main()
