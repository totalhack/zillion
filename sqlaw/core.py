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

NUMERIC_SA_TYPES = [
    sa.BigInteger,
    sa.sql.sqltypes.BIGINT,
    sa.sql.sqltypes.DECIMAL,
    sa.Float,
    sa.sql.sqltypes.FLOAT,
    sa.Integer,
    sa.sql.sqltypes.INTEGER,
    sa.Numeric,
    sa.sql.sqltypes.NUMERIC,
    sa.sql.sqltypes.REAL,
    sa.SmallInteger,
    sa.sql.sqltypes.SMALLINT,
]
