import ast
import re

import sqlalchemy as sa
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.engine import reflection
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression as exp
import sqlparse as sp

from zillion.core import *

DIGIT_THRESHOLD_FOR_MEAN_AGGR = 1

INTEGER_SA_TYPES = [
    sa.BigInteger,
    sa.BIGINT,
    sa.Integer,
    sa.INT,
    sa.INTEGER,
    sa.SmallInteger,
    sa.SMALLINT,
]

FLOAT_SA_TYPES = [
    sa.DECIMAL,
    sa.Float,
    sa.FLOAT,
    sa.Numeric,
    sa.NUMERIC,
    sa.REAL,
    sa.dialects.postgresql.DOUBLE_PRECISION,
    sa.dialects.postgresql.MONEY,
]

NUMERIC_SA_TYPES = INTEGER_SA_TYPES + FLOAT_SA_TYPES

DATETIME_SA_TYPES = [sa.DateTime, sa.DATETIME, sa.Time, sa.TIME, sa.TIMESTAMP]

DATE_SA_TYPES = [sa.Date, sa.DATE]

AGGREGATION_SQLA_FUNC_MAP = {
    AggregationTypes.MEAN: sa.func.AVG,
    AggregationTypes.COUNT: sa.func.COUNT,
    AggregationTypes.COUNT_DISTINCT: lambda x: sa.func.COUNT(sa.distinct(x)),
    AggregationTypes.MIN: sa.func.MIN,
    AggregationTypes.MAX: sa.func.MAX,
    AggregationTypes.SUM: sa.func.SUM,
}

SQL_AGGREGATION_FUNCS = set(
    [
        "AVG",
        "SUM",
        "MIN",
        "MAX",
        "COUNT",
        "COUNT_DISTINCT",
        "STD",
        "MEDIAN",
        "MODE",
        "VAR",
    ]
)

# This establishes a baseline of schemas to ignore during reflection
DIALECT_IGNORE_SCHEMAS = {
    "mysql": set(["information_schema", "performance_schema", "mysql", "sys"]),
    "postgresql": set(["information_schema", r"pg_(.*)"]),
}


class InvalidSQLAlchemyTypeString(Exception):
    pass


def contains_sql_keywords(sql):
    """Determine whether a SQL query contains special SQL keywords (DML, DDL, etc.)

    Parameters
    ----------
    sql : str or sqlparse result
        The SQL query to check for keywords

    Returns
    -------
    bool
        True if the SQL string contains keywords

    """
    if isinstance(sql, str):
        sql = sp.parse(sql)

    for token in sql:
        if token.ttype in (sp.tokens.DML, sp.tokens.DDL, sp.tokens.CTE):
            return True

        if isinstance(token, sp.sql.TokenList):
            token_result = contains_sql_keywords(token)
            if token_result:
                return True
    return False


def contains_aggregation(sql):
    """Determine whether a SQL query contains aggregation functions.

    WARNING: this relies on a non-exhaustive list of SQL aggregation
    functions to look for. This will likely need updating.

    Parameters
    ----------
    sql : str or sqlparse result
        The SQL query to check for aggregation functions

    Returns
    -------
    bool
        True if the SQL string contains aggregation

    """
    if isinstance(sql, str):
        sql = sp.parse(sql)

    for token in sql:
        if isinstance(token, sp.sql.Function):
            name = token.get_name()
            if name.upper() in SQL_AGGREGATION_FUNCS:
                return True
        if isinstance(token, sp.sql.TokenList):
            token_result = contains_aggregation(token)
            if token_result:
                return True
    return False


def type_string_to_sa_type(type_string):
    """Convert a field type string to a SQLAlchemy type. The type string will
    be evaluated as a python statement or class name to init from the
    SQLAlchemy top level module. Dialect-specific SQLAlchemy types are not
    currently supported.

    Parameters
    ----------
    type_string : str
        A string representing a SQLAlchemy type, such as "Integer", or "String(32)".

    Returns
    -------
    SQLAlchemy type object
        An init'd SQLAlchemy type object

    """
    parts = type_string.split("(")
    type_args = []
    if len(parts) > 1:
        raiseifnot(len(parts) == 2, "Unable to parse type string: %s" % type_string)
        type_args = ast.literal_eval(parts[1].rstrip(")") + ",")
    type_name = parts[0]
    type_cls = getattr(sa.types, type_name, None)
    if not type_cls:
        raise InvalidSQLAlchemyTypeString(
            "Could not find matching type for %s" % type_name
        )
    return type_cls(*type_args)


def infer_aggregation_and_rounding(column):
    """Infer the aggregation and rounding settings based on the column type

    Parameters
    ----------
    column : SQLAlchemy column
        The column to analyze

    Returns
    -------
    AggregationType, int
        A 2-item tuple of the aggregation type and rounding to use

    """
    if isinstance(column.type, tuple(INTEGER_SA_TYPES)):
        return AggregationTypes.SUM, 0
    if isinstance(column.type, tuple(FLOAT_SA_TYPES)):
        rounding = column.type.scale
        precision = column.type.precision
        if rounding is None and precision is None:
            aggregation = AggregationTypes.SUM
        else:
            whole_digits = precision - rounding
            if whole_digits <= DIGIT_THRESHOLD_FOR_MEAN_AGGR:
                aggregation = AggregationTypes.MEAN
            else:
                aggregation = AggregationTypes.SUM
        return aggregation, rounding
    raise ZillionException("Column %s is not a numeric type" % column)


def aggregation_to_sqla_func(aggregation):
    """Convert an AggregationType string to a SQLAlchemy function"""
    return AGGREGATION_SQLA_FUNC_MAP[aggregation]


def is_numeric_type(type):
    """Determine if this is a numeric SQLAlchemy type"""
    if isinstance(type, tuple(NUMERIC_SA_TYPES)):
        return True
    return False


def is_probably_metric(column, formula=None):
    """Determine if a column is probably a metric. This is used when trying to
    automatically init/reflect a datasource and determine the field types for
    columns. The logic is very coarse, and should not be relied on for more
    than quick/convenient use cases.

    Parameters
    ----------
    column : SQLAlchemy column
        The column to analyze
    formula : str, optional
        A formula to calculate the column

    Returns
    -------
    bool
        True if the column is probably a metric

    """
    if formula and contains_aggregation(formula):
        return True
    if not isinstance(column.type, tuple(NUMERIC_SA_TYPES)):
        return False
    if column.primary_key:
        return False
    if column.name.endswith("_id") or column.name.endswith("Id") or column.name == "id":
        return False
    return True


def sqla_compile(expr):
    """Compile a SQL expression

    Parameters
    ----------
    expr : SQLAlchemy expression
        The SQLAlchemy expression to compile

    Returns
    -------
    str
        The compiled expression string

    """
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


def printexpr(expr):
    """Print a SQLAlchemy expression"""
    print(sqla_compile(expr))


def column_fullname(column, prefix=None):
    """Get a fully qualified name for a column

    Parameters
    ----------
    column : SQLAlchemy column
        A SQLAlchemy column object to get the full name for
    prefix : str, optional
        If specified, a manual prefix to prepend to the output string. This
        will automatically be separted with a ".".

    Returns
    -------
    str
        A fully qualified column name. The exact format will vary depending on
        your SQLAlchemy metadata, but an example would be: schema.table.column

    """
    name = "%s.%s" % (column.table.fullname, column.name)
    if prefix:
        name = prefix + "." + name
    return name


def get_sqla_criterion_expr(column, criterion, negate=False):
    """Create a SQLAlchemy criterion expression

    Parameters
    ----------
    column : SQLAlchemy column
        A SQLAlchemy column object to be used in the expression
    criterion : 3-item iterable
        A 3-item tuple or list of the format [field, operation, value(s)]. The
        supported operations are: =, !=, >, <, >=, <=, in, not in, between,
        not between, like, not like. The value item may take on different
        formats depending on the operation. In most cases passing an iterable
        will result in multiple criteria of that operation being formed. For
        example, ("my_field", "=", [1,2,3]) would logically or 3 conditions of
        equality to the 3 values in the list. The "between" operations expect
        each value to be a 2-item iterable representing the lower and upper
        bound of the criterion.

        TODO: more complete documentation of options/examples
    negate : bool, optional
        Negate the expression

    Note
    ----
    Postgresql "like" is case sensitive, but mysql "like" is not. Postgresql
    also supports "ilike" to specify case insensitive, so one option is to
    look at the dialect to determine the function, but that is not supported
    yet.

    Returns
    -------
    SQLAlchemy expression
        A SQLALchemy expression representing the criterion

    """
    field, op, values = criterion
    op = op.lower()
    if not isinstance(values, (list, tuple)):
        values = [values]

    use_or = True
    has_null = any([v is None for v in values])

    if op == "=":
        clauses = [column == v if v is not None else column._is(None) for v in values]
    elif op == "!=":
        clauses = [column != v if v is not None else column.isnot(None) for v in values]
    elif op == ">":
        clauses = [column > v for v in values]
    elif op == "<":
        clauses = [column < v for v in values]
    elif op == ">=":
        clauses = [column >= v for v in values]
    elif op == "<=":
        clauses = [column <= v for v in values]
    elif op == "in":
        if has_null:
            clauses = [
                column == v if v is not None else column._is(None) for v in values
            ]
        else:
            clauses = [column.in_(values)]
    elif op == "not in":
        use_or = False
        if has_null:
            clauses = [
                column != v if v is not None else column.isnot(None) for v in values
            ]
        else:
            clauses = [sa.not_(column.in_(values))]
    elif op == "between":
        clauses = []
        for value in values:
            raiseifnot(len(value) == 2, "Between clause value must have length of 2")
            clauses.append(column.between(value[0], value[1]))
    elif op == "not between":
        clauses = []
        for value in values:
            raiseifnot(len(value) == 2, "Between clause value must have length of 2")
            clauses.append(sa.not_(column.between(value[0], value[1])))
    elif op == "like":
        clauses = [column.like(v) for v in values]
    elif op == "not like":
        use_or = False
        clauses = [sa.not_(column.like(v)) for v in values]
    else:
        raise ZillionException("Invalid criterion operand: %s" % op)

    if use_or:
        clause = sa.or_(*clauses)
    else:
        clause = sa.and_(*clauses)

    if negate:
        clause = sa.not_(clause)

    return clause


def comment(self, c):
    """See https://github.com/sqlalchemy/sqlalchemy/wiki/CompiledComments"""
    self._added_comment = c
    return self


exp.ClauseElement.comment = comment
exp.ClauseElement._added_comment = None


def _compile_element(elem, prepend_newline=False):
    """See https://github.com/sqlalchemy/sqlalchemy/wiki/CompiledComments"""

    @compiles(elem)
    def add_comment(element, compiler, **kw):
        meth = getattr(compiler, "visit_%s" % element.__visit_name__)
        text = meth(element, **kw)
        if element._added_comment:
            # Modified this line to not add newline
            text = "-- %s\n" % element._added_comment + text
        elif prepend_newline:
            text = "\n" + text
        return text


_compile_element(exp.Case)
_compile_element(exp.Label, True)
_compile_element(exp.ColumnClause)
_compile_element(exp.Join)
_compile_element(exp.Select)
_compile_element(exp.Alias)
_compile_element(exp.Exists)


def get_schemas(engine):
    """Inspect the SQLAlchemy engine to get a list of schemas"""
    insp = reflection.Inspector.from_engine(engine)
    return insp.get_schema_names()


# -------- Some DB-specific stuff


def to_mysql_type(type):
    """Compile into a MySQL SQLAlchemy type"""
    return type.compile(dialect=mysql_dialect())


def to_postgresql_type(type):
    """Compile into a PostgreSQL SQLAlchemy type"""
    return type.compile(dialect=postgresql_dialect())


def to_sqlite_type(type):
    """Compile into a SQLite SQLAlchemy type"""
    return type.compile(dialect=sqlite_dialect())


def filter_dialect_schemas(schemas, dialect):
    """Filter out a set of baseline/system schemas for a dialect

    Parameters
    ----------
    schemas : list
        A list of schema names
    dialect : str
        The name of a SQLAlchemy dialect

    Returns
    -------
    list
        A filtered list of schema names

    """
    ignores = DIALECT_IGNORE_SCHEMAS.get(dialect, None)
    if not ignores:
        return schemas

    final = []
    for schema in schemas:
        add = True
        for ignore in ignores:
            if re.match(ignore, schema):
                add = False
                break
        if add:
            final.append(schema)

    return final


def get_postgres_schemas(conn):
    """Helper to list PostgreSQL schemas"""
    qr = conn.execute(
        sa.text(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name not LIKE 'pg_%' and schema_name != 'information_schema'"
        )
    )
    return [x["schema_name"] for x in qr.fetchall()]


def get_postgres_pid(conn):
    """Helper to get the PostgreSQL connection PID"""
    qr = conn.execute("select pg_backend_pid()")
    pid = qr.fetchone()[0]
    return pid
