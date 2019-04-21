import sqlalchemy as sa

TABLE_TYPES = [
    'fact',
    'dimension'
]

COLUMN_TYPES = [
    'auto',
    'fact',
    'dimension'
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
