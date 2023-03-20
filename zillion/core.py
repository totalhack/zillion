# pylint: disable=unused-import,missing-class-docstring
from collections.abc import MutableMapping
import logging
import os
import requests
import sys
import time

from tlbx import (
    st,
    dbg as _dbg,
    dbgsql as _dbgsql,
    info as _info,
    warn as _warn,
    error as _error,
    pf,
    pp,
    format_msg,
    sqlformat,
    rmfile,
    get_caller,
    open_filepath_or_buffer,
    get_string_format_args,
    json,
    chunks,
    iter_or,
    powerset,
    is_int,
    orderedsetify,
    initializer,
    PrintMixin,
    MappingMixin,
    ClassValueContainsMeta,
    get_class_var_values,
    get_class_vars,
    import_object,
)
import yaml

# Last unicode char - this helps get the rollup rows to sort last, but may
# need to be replaced for presentation.
ROLLUP_INDEX_LABEL = chr(1114111)
# HACK: pandas can't group MultiIndex NaN values, so we replace them with a
# value we *hope* to never see in the index as a workaround.
NAN_DIMENSION_VALUE_LABEL = chr(1114110)
# This is more friendly for front-end viewing, but has a better chance of
# conflicting with actual report data.
ROLLUP_INDEX_DISPLAY_LABEL = "::"

ADHOC_DS_URL = "adhoc"  # A placeholder to denote its an adhoc datasource
RESERVED_FIELD_NAMES = set(["row_hash"])
DEFAULT_REPLACE_AFTER = "1 days"
CRITERIA_OPERATIONS = set(
    [
        ">",
        ">=",
        "<",
        "<=",
        "=",
        "!=",
        "in",
        "not in",
        "between",
        "not between",
        "like",
        "not like",
    ]
)
ROW_FILTER_OPERATIONS = set([">", ">=", "<", "<=", "=", "!="])


class ZillionException(Exception):
    pass


class InvalidTechnicalException(ZillionException):
    pass


class WarehouseException(ZillionException):
    pass


class InvalidWarehouseIdException(ZillionException):
    pass


class ReportException(ZillionException):
    pass


class InvalidReportIdException(ZillionException):
    pass


class UnsupportedGrainException(ZillionException):
    pass


class UnsupportedKillException(ZillionException):
    pass


class FailedKillException(ZillionException):
    pass


class DataSourceQueryTimeoutException(ZillionException):
    pass


class ExecutionKilledException(ZillionException):
    pass


class ExecutionLockException(ZillionException):
    pass


class InvalidFieldException(ZillionException):
    pass


class InvalidDimensionValueException(ZillionException):
    pass


class DisallowedSQLException(ZillionException):
    pass


class MaxFormulaDepthException(ZillionException):
    pass


class FieldTypes(metaclass=ClassValueContainsMeta):
    """Allowed field types"""

    DIMENSION = "dimension"
    METRIC = "metric"


class TableTypes(metaclass=ClassValueContainsMeta):
    """Allowed table types"""

    DIMENSION = "dimension"
    METRIC = "metric"


class AggregationTypes(metaclass=ClassValueContainsMeta):
    """Allowed aggregation types. These aggregations are limited by what can be
    done in most SQL databases"""

    MEAN = "mean"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"


class TechnicalTypes(metaclass=ClassValueContainsMeta):
    """Allowed technical types"""

    MEAN = "mean"
    SUM = "sum"
    MEDIAN = "median"
    STD = "std"
    VAR = "var"
    MIN = "min"
    MAX = "max"
    BOLL = "boll"
    DIFF = "diff"
    PCT_CHANGE = "pct_change"
    CUMSUM = "cumsum"
    CUMMIN = "cummin"
    CUMMAX = "cummax"
    RANK = "rank"
    PCT_RANK = "pct_rank"


class TechnicalModes(metaclass=ClassValueContainsMeta):
    """Allowed Technical modes

    **Attributes:**

    * **GROUP** - (*str*) Apply the technical to the last grouping of the data
    for a multi-dimensional report
    * **ALL** - (*str*) Apply the technical across all result data

    """

    GROUP = "group"
    ALL = "all"


class RollupTypes(metaclass=ClassValueContainsMeta):
    """Allowed Rollup Types"""

    TOTALS = "totals"
    ALL = "all"


class OrderByTypes(metaclass=ClassValueContainsMeta):
    """Allowed Order By Types"""

    ASC = "asc"
    DESC = "desc"


class DataSourceQueryModes(metaclass=ClassValueContainsMeta):
    """Allowed datasource query modes"""

    SEQUENTIAL = "sequential"
    MULTITHREAD = "multithread"


class ExecutionState:
    """Allowed report/query execution states"""

    READY = "ready"
    QUERYING = "querying"
    KILLED = "killed"


class IfExistsModes(metaclass=ClassValueContainsMeta):
    """Allowed modes when creating tables from data. This is based off of pandas
    `if_exists` param in the `DataFrame.to_sql` method, with the addition of an
    "ignore" option. The "append" option is also removed for now since there
    isn't a safe/generic way to guarantee a proper primary key has been set on
    the table."""

    FAIL = "fail"
    REPLACE = "replace"
    # APPEND = "append"
    IGNORE = "ignore"


class IfFileExistsModes(IfExistsModes):
    """An extension of the modes above specific to downloaded files. This
    allows the config to specify that a downloaded file should be replaced
    after a certain amount of time. See code that uses this for implementation
    details.
    """

    REPLACE_AFTER = "replace_after"


# ---- TODO: move below to utils file


def raiseif(cond, msg="", exc=ZillionException):
    """Convenience assert-like utility"""
    if cond:
        raise exc(msg)


def raiseifnot(cond, msg="", exc=ZillionException):
    """Convenience assert-like utility"""
    if not cond:
        raise exc(msg)


def igetattr(obj, attr, *args):
    """Case-insensitive getattr"""
    for a in dir(obj):
        if a.lower() == attr.lower():
            return getattr(obj, a)
    if args:
        return args[0]
    raise AttributeError("type object '%s' has no attribute '%s'" % (type(obj), attr))


def read_filepath_or_buffer(f, open_flags="r", compression=None):
    """Open and read files or buffers, local or remote"""
    f, handles, close = open_filepath_or_buffer(
        f, open_flags=open_flags, compression=compression
    )
    try:
        data = f.read()
    finally:
        if close:
            try:
                f.close()
            except ValueError:
                pass
    return data


def download_file(url, outfile=None):
    """Utility to download a datafile"""
    if not outfile:
        outfile = url.split("/")[-1]
    info("Downloading %s to %s" % (url, outfile))
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(outfile, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return outfile


def get_modified_time(fname):
    """Utility to get the modified time of a file"""
    return os.stat(fname).st_mtime


def get_time_since_modified(fname):
    """Utility to get the time since a file was last modified"""
    return time.time() - get_modified_time(fname)


def load_yaml(fname):
    """Wrapper to safe_load that also expands environment vars"""
    with open(fname) as f:
        val = yaml.safe_load(os.path.expandvars(f.read()))
    return val


def load_json_or_yaml_from_str(string, f=None, schema=None):
    """Load the file as json or yaml depending on the extension (if f is a string)
    or by trying both (if f is not a string). If you know ahead of time that your
    data is one or the other, you should use yaml or json load directly.

    **Parameters:**

    * **string** - (*str*) The raw json or yaml string
    * **f** - (*str or buffer*) A file path or buffer where contents were read from
    * **schema** - (*optional*) Validate against this schema

    **Returns:**

    (*dict*) - A dict structure loaded from the json/yaml

    """

    load_result = None
    if f and isinstance(f, str):  # f is assumed to be a filename
        f = f.lower()
        if f.endswith("yaml") or f.endswith("yml"):
            load_result = yaml.safe_load(string)
        elif f.endswith("json"):
            load_result = json.loads(string)

    if load_result is None:
        # f is not a filename, we try json and then fall back to yaml
        try:
            load_result = json.loads(string)
        except Exception as ej:
            try:
                load_result = yaml.safe_load(string)
            except Exception as ey:
                raise Exception("Could not load string as json or yaml")

    if schema:
        return schema.load(load_result)
    return load_result


def dictmerge(x, y, path=None, overwrite=False, extend=False):
    """Adapted version of tlbx's dictmerge that supports extending lists"""
    if path is None:
        path = []
    for key in y:
        if key in x:
            if isinstance(x[key], (dict, MutableMapping)) and isinstance(
                y[key], (dict, MutableMapping)
            ):
                dictmerge(
                    x[key],
                    y[key],
                    path + [str(key)],
                    overwrite=overwrite,
                    extend=extend,
                )
            elif x[key] == y[key]:
                pass  # same leaf value
            else:
                if not overwrite:
                    raise Exception("Conflict at %s" % ".".join(path + [str(key)]))
                if isinstance(x[key], list) and isinstance(y[key], list) and extend:
                    x[key].extend(y[key])
                else:
                    x[key] = y[key]
        else:
            x[key] = y[key]
    return x


def load_zillion_config():
    """If the ZILLION_CONFIG environment variable is defined, read the YAML
    config from this file. Environment variable substitution is supported
    in the yaml file. Otherwise return a default config. Environment variables
    prefixed with "ZILLION_"  will also be read in (with the prefix stripped)
    and take precedence.

    **Returns:**

    (*dict*) - The zillion config dict.

    """
    zillion_config_fname = os.environ.get("ZILLION_CONFIG", None)
    if zillion_config_fname:
        # Load with support for filling in env var values
        config = load_yaml(zillion_config_fname)
        for k, v in config.copy().items():
            # Hack: some older config items had ZILLION prefixed. As a workaround
            # we now always remove that prefix and map to the key without the prefix.
            if k.startswith("ZILLION_"):
                config[k.replace("ZILLION_", "")] = v
                del config[k]
    else:
        print("No ZILLION_CONFIG specified, using default settings")
        config = dict(
            DEBUG=False,
            LOG_LEVEL="WARNING",
            DB_URL="sqlite:////tmp/zillion.db",
            ADHOC_DATASOURCE_DIRECTORY="/tmp",
            LOAD_TABLE_CHUNK_SIZE=5000,
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=None,
            DATASOURCE_CONTEXTS={},
        )

    for k, v in os.environ.items():
        if k.startswith("ZILLION_"):
            config[k.replace("ZILLION_", "")] = v

    return config


zillion_config = load_zillion_config()


def get_zillion_config_log_level():
    return getattr(logging, zillion_config.get("LOG_LEVEL", "WARNING").upper())


default_logger = logging.getLogger("zillion")


def set_log_level_from_config(cfg):
    global default_logger
    if str(cfg.get("DEBUG", "false")).lower() in ("true", "1"):
        default_logger.setLevel(logging.DEBUG)
        # Make sure logs can show up in testing
        handler = logging.StreamHandler(sys.stdout)
        default_logger.handlers = []
        default_logger.propagate = False
        default_logger.addHandler(handler)
        print("---- Zillion debug logging enabled ----")
    else:
        default_logger.setLevel(get_zillion_config_log_level())


set_log_level_from_config(zillion_config)


def dbg(msg, **kwargs):
    """Call tlbx dbg with zillion logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _dbg(msg, **kwargs)


def dbgsql(msg, **kwargs):
    """Call tlbx dbgsql with zillion logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _dbgsql(msg, **kwargs)


def info(msg, **kwargs):
    """Call tlbx info with zillion logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _info(msg, **kwargs)


def warn(msg, **kwargs):
    """Call tlbx warn with zillion logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _warn(msg, **kwargs)


def error(msg, **kwargs):
    """Call tlbx error with zillion logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _error(msg, **kwargs)
