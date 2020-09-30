# pylint: disable=unused-import,missing-class-docstring
import logging
import os
import requests
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


default_logger = logging.getLogger("zillion")
default_logger.setLevel(logging.INFO)


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


class ZillionException(Exception):
    pass


class InvalidTechnicalException(ZillionException):
    pass


class WarehouseException(ZillionException):
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
