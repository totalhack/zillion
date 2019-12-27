from tlbx import ClassValueContainsMeta


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


class UnsupportedGrainException(Exception):
    pass


class WarehouseException(Exception):
    pass


class InvalidFieldException(Exception):
    pass


class MaxFormulaDepthException(Exception):
    pass
