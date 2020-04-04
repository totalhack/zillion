from tlbx import ClassValueContainsMeta

ADHOC_URL = "adhoc"


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


class InvalidTechnicalException(Exception):
    pass


class WarehouseException(Exception):
    pass


class ReportException(Exception):
    pass


class UnsupportedGrainException(Exception):
    pass


class UnsupportedKillException(Exception):
    pass


class FailedKillException(Exception):
    pass


class DataSourceQueryTimeoutException(Exception):
    pass


class ExecutionKilledException(Exception):
    pass


class ExecutionLockException(Exception):
    pass


class InvalidFieldException(Exception):
    pass


class DisallowedSQLException(Exception):
    pass


class MaxFormulaDepthException(Exception):
    pass
