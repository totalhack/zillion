#!/usr/bin/env python
"""Helper script to load a config for testing/inspection. This will drop 
you into a PDB shell with the loaded Warehouse object available as `wh`."""

import ast
import logging
import time

from tlbx import Script, Arg, raiseif, st

from zillion.core import info, set_log_level
from zillion.datasource import DataSource
from zillion.warehouse import Warehouse


@Script(
    Arg("config", help="Path to warehouse config file"),
    Arg(
        "-ds",
        "--ds_config",
        action="store_true",
        default=False,
        help="Interpret the config as a DataSource config and create a Warehouse from the DataSource",
    ),
    Arg("-ll", "--log_level", type=int, default=logging.INFO, help="Set log level"),
)
def main(
    config=None,
    ds_config=False,
    log_level=None,
):
    if log_level:
        set_log_level(log_level)

    start = time.time()
    if ds_config:
        ds = DataSource("bootstrap", config=config)
        wh = Warehouse(datasources=[ds])
    else:
        wh = Warehouse(config=config)

    info(f"Loaded config in {time.time() - start:.2f} seconds")
    info(f"Use the 'wh' variable to inspect the loaded Warehouse object.")
    st()


if __name__ == "__main__":
    main()
