import string

from tlbx import ClassValueContainsMeta


class UnsupportedGrainException(Exception):
    pass


class InvalidFieldException(Exception):
    pass


class MaxFormulaDepthException(Exception):
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
    BOLL = "BOLL"


class DataSourceQueryModes(metaclass=ClassValueContainsMeta):
    SEQUENTIAL = "sequential"
    MULTITHREADED = "multithreaded"


def parse_technical_string(val):
    min_period = None
    center = None
    parts = val.split("-")

    if len(parts) == 2:
        ttype, window = parts
    elif len(parts) == 3:
        ttype, window, min_period = parts

    val = dict(type=ttype, window=window)
    if min_period is not None:
        val["min_period"] = min_period
    if center is not None:
        val["center"] = center
    return val


ROW_FILTER_OPS = [">", ">=", "<", "<=", "==", "!=", "in", "not in"]

FIELD_ALLOWABLE_CHARS_STR = (
    string.ascii_uppercase + string.ascii_lowercase + string.digits + "_"
)
FIELD_ALLOWABLE_CHARS = set(FIELD_ALLOWABLE_CHARS_STR)

DATASOURCE_ALLOWABLE_CHARS_STR = (
    string.ascii_uppercase + string.ascii_lowercase + string.digits + "_"
)
DATASOURCE_ALLOWABLE_CHARS = set(DATASOURCE_ALLOWABLE_CHARS_STR)


def field_safe_name(name):
    for char in name:
        if char not in FIELD_ALLOWABLE_CHARS:
            name = name.replace(char, "_")
    return name
