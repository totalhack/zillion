#!/usr/bin/env python
"""Helper script to test out reports"""
import ast
import logging

from tlbx import Script, Arg, st, pp

from test_utils import *


@Script(
    Arg("-m", "--metrics", nargs="+", help="Metrics to include in report"),
    Arg("-d", "--dimensions", nargs="+", help="Dimensions to include in report"),
    Arg("-c", "--criteria", help="String to be eval'd as criteria param"),
    Arg("-r", "--rollup", help="Rollup setting"),
    Arg("-rf", "--row_filters", help="String to be eval'd as row filter param"),
    Arg("-ll", "--log_level", type=int, default=None, help="Set log level"),
)
def main(
    metrics=None,
    dimensions=None,
    criteria=None,
    row_filters=None,
    rollup=None,
    log_level=None,
):
    if log_level:
        logger = logging.getLogger()
        logger.setLevel(log_level)

    if criteria:
        criteria = ast.literal_eval(criteria)
    if row_filters:
        row_filters = ast.literal_eval(row_filters)

    pp(locals())

    config = copy.deepcopy(TEST_WH_CONFIG)
    wh = Warehouse(config=config)
    result = wh.execute(
        metrics=metrics,
        dimensions=dimensions,
        criteria=criteria,
        row_filters=row_filters,
        rollup=rollup,
    )
    print(result.df_display)


if __name__ == "__main__":
    main()
