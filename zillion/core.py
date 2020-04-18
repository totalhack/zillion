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

ADHOC_URL = "adhoc"
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
    DIMENSION = "dimension"
    METRIC = "metric"


class TableTypes(metaclass=ClassValueContainsMeta):
    DIMENSION = "dimension"
    METRIC = "metric"


class AggregationTypes(metaclass=ClassValueContainsMeta):
    AVG = "avg"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    MIN = "min"
    MAX = "max"
    SUM = "sum"


class TechnicalTypes(metaclass=ClassValueContainsMeta):
    MA = "MA"
    SUM = "SUM"
    CUMSUM = "CUMSUM"
    BOLL = "BOLL"
    DIFF = "DIFF"
    PCT_DIFF = "PCT_DIFF"


class DataSourceQueryModes(metaclass=ClassValueContainsMeta):
    SEQUENTIAL = "sequential"
    MULTITHREAD = "multithread"


class ExecutionState:
    READY = "ready"
    QUERYING = "querying"
    KILLED = "killed"


def raiseif(cond, msg="", exc=ZillionException):
    if cond:
        raise exc(msg)


def raiseifnot(cond, msg="", exc=ZillionException):
    if not cond:
        raise exc(msg)
