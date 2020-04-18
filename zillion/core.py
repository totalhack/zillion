from tlbx import (
    st,
    dbg,
    dbgsql,
    info,
    warn,
    error,
    pf,
    format_msg,
    sqlformat,
    rmfile,
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
