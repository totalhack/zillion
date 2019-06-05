import string

import sqlalchemy as sa

from sqlaw.utils import ClassValueContainsMeta

class FieldTypes(metaclass=ClassValueContainsMeta):
    DIMENSION = 'dimension'
    FACT = 'fact'

class TableTypes(metaclass=ClassValueContainsMeta):
    DIMENSION = 'dimension'
    FACT = 'fact'

class AggregationTypes(metaclass=ClassValueContainsMeta):
    AVG = 'avg'
    COUNT = 'count'
    COUNT_DISTINCT = 'count_distinct'
    MIN = 'min'
    MAX = 'max'
    SUM = 'sum'

class TechnicalTypes(metaclass=ClassValueContainsMeta):
    MA = 'MA'
    SUM = 'SUM'
    BOLL = 'BOLL'

def parse_technical_string(val):
    min_period = None
    center = None
    parts = val.split('-')

    if len(parts) == 2:
        ttype, window = parts
    elif len(parts) == 3:
        ttype, window, min_period = parts

    val = dict(type=ttype, window=window)
    if min_period is not None:
        val['min_period'] = min_period
    if center is not None:
        val['center'] = center
    return val

ROW_FILTER_OPS = [
    '>',
    '>=',
    '<',
    '<=',
    '==',
    '!=',
    'in',
    'not in',
]

INTEGER_SA_TYPES = [
    sa.BigInteger,
    sa.sql.sqltypes.BIGINT,
    sa.Integer,
    sa.sql.sqltypes.INTEGER,
    sa.SmallInteger,
    sa.sql.sqltypes.SMALLINT,
]

FLOAT_SA_TYPES = [
    sa.sql.sqltypes.DECIMAL,
    sa.Float,
    sa.sql.sqltypes.FLOAT,
    sa.Numeric,
    sa.sql.sqltypes.NUMERIC,
    sa.sql.sqltypes.REAL,
]

NUMERIC_SA_TYPES = INTEGER_SA_TYPES + FLOAT_SA_TYPES

FIELD_ALLOWABLE_CHARS_STR = string.ascii_uppercase + string.ascii_lowercase + string.digits + '_'
FIELD_ALLOWABLE_CHARS = set(FIELD_ALLOWABLE_CHARS_STR)

def field_safe_name(name):
    for char in name:
        if char not in FIELD_ALLOWABLE_CHARS:
            name = name.replace(char, '_')
    return name
