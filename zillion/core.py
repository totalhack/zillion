# pylint: disable=unused-import,missing-class-docstring
import logging

from tlbx import (
    st,
    dbg as _dbg,
    dbgsql as _dbgsql,
    info as _info,
    warn as _warn,
    error as _error,
    pf,
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
)

ADHOC_DS_URL = "adhoc"  # A placeholder to denote its an adhoc datasource
RESERVED_FIELD_NAMES = set(["row_hash"])

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


class DisallowedSQLException(ZillionException):
    pass


class MaxFormulaDepthException(ZillionException):
    pass


class FieldTypes(metaclass=ClassValueContainsMeta):
    """Allowed field types"""

    DIMENSION = "DIMENSION"
    METRIC = "METRIC"


class TableTypes(metaclass=ClassValueContainsMeta):
    """Allowed table types"""

    DIMENSION = "DIMENSION"
    METRIC = "METRIC"


class AggregationTypes(metaclass=ClassValueContainsMeta):
    """Allowed aggregation types. These aggregations are limited
    by what can be done in most SQL databases"""

    MEAN = "MEAN"
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"


class TechnicalTypes(metaclass=ClassValueContainsMeta):
    """Allowed technical types"""

    MEAN = "MEAN"
    SUM = "SUM"
    MEDIAN = "MEDIAN"
    STD = "STD"
    VAR = "VAR"
    MIN = "MIN"
    MAX = "MAX"
    BOLL = "BOLL"
    DIFF = "DIFF"
    PCT_CHANGE = "PCT_CHANGE"
    CUMSUM = "CUMSUM"
    CUMMIN = "CUMMIN"
    CUMMAX = "CUMMAX"
    RANK = "RANK"
    PCT_RANK = "PCT_RANK"


class TechnicalModes(metaclass=ClassValueContainsMeta):
    """Allowed Technical modes

    Attributes
    ----------
    GROUP : str
        Apply the technical to the last grouping of the data for a
        multi-dimensional report
    ALL : str
        Apply the technical across all result data

    """

    GROUP = "GROUP"
    ALL = "ALL"


class RollupTypes(metaclass=ClassValueContainsMeta):
    """Allowed Rollup Types """

    TOTALS = "TOTALS"
    ALL = "ALL"


class DataSourceQueryModes(metaclass=ClassValueContainsMeta):
    """Allowed datasource query modes"""

    SEQUENTIAL = "SEQUENTIAL"
    MULTITHREAD = "MULTITHREAD"


class ExecutionState:
    """Allowed report/query execution states"""

    READY = "READY"
    QUERYING = "QUERYING"
    KILLED = "KILLED"


def raiseif(cond, msg="", exc=ZillionException):
    """Convenience assert-like utility"""
    if cond:
        raise exc(msg)


def raiseifnot(cond, msg="", exc=ZillionException):
    """Convenience assert-like utility"""
    if not cond:
        raise exc(msg)
