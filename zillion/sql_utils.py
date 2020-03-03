"""
Useful reference around SQLAlchemy types by dialect:
https://github.com/zzzeek/sqlalchemy/blob/master/lib/sqlalchemy/dialects/type_migration_guidelines.txt
"""

import ast

import sqlalchemy as sa
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression as exp
import sqlparse as sp
from tlbx import dbg, st, get_class_vars, get_string_format_args

from zillion.core import AggregationTypes

DIGIT_THRESHOLD_FOR_AVG_AGGR = 1

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

# Order of this matters!
DATE_HIERARCHY = [
    "year",
    "month",
    "month_of_year",
    "date",
    "day_of_week",
    "day_of_month",
    "day_of_year",
    "hour",
    "hour_of_day",
    "minute",
    "minute_of_hour",
    "datetime",
    "unix_timestamp",
]

DIALECT_DATE_CONVERSIONS = {
    "sqlite": {
        # https://www.sqlite.org/lang_datefunc.html
        "year": "strftime('%Y', {})",
        "month": "strftime('%Y-%m', {})",
        "month_of_year": "strftime('%m', {})",
        "date": "strftime('%Y-%m-%d', {})",
        "day_of_week": "strftime('%w', {})",  # day of week 0-6 with Sunday==0
        "day_of_month": "strftime('%d', {})",
        "day_of_year": "strftime('%j', {})",
        "hour": "strftime('%Y-%m-%d %H', {})",
        "hour_of_day": "strftime('%H', {})",
        "minute": "strftime('%Y-%m-%d %H:%M', {})",
        "minute_of_hour": "strftime('%M', {})",
        "datetime": "strftime('%Y-%m-%d %H:%M:%S', {})",
        "unix_timestamp": "strftime('%s', {})",
    },
    #'mysql': {
    #   TODO
    # },
    #'postgresql': {
    #   TODO
    # }
}

TYPE_ALLOWED_CONVERSIONS = {
    sa.DateTime: {
        "allowed_conversions": DATE_HIERARCHY,
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.DATETIME: {
        "allowed_conversions": DATE_HIERARCHY,
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.TIMESTAMP: {
        "allowed_conversions": DATE_HIERARCHY,
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.Time: {
        "allowed_conversions": DATE_HIERARCHY,
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.TIME: {
        "allowed_conversions": DATE_HIERARCHY,
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.Date: {
        "allowed_conversions": DATE_HIERARCHY[0 : DATE_HIERARCHY.index("hour")],
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.DATE: {
        "allowed_conversions": DATE_HIERARCHY[0 : DATE_HIERARCHY.index("hour")],
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
}

AGGREGATION_SQLA_FUNC_MAP = {
    AggregationTypes.AVG: sa.func.avg,
    AggregationTypes.COUNT: sa.func.count,
    AggregationTypes.COUNT_DISTINCT: lambda x: sa.func.count(sa.distinct(x)),
    AggregationTypes.MIN: sa.func.min,
    AggregationTypes.MAX: sa.func.max,
    AggregationTypes.SUM: sa.func.sum,
}


class InvalidSQLAlchemyTypeString(Exception):
    pass


def get_dialect_type_conversions(dialect, column):
    coltype = type(column.type)
    conv_info = TYPE_ALLOWED_CONVERSIONS.get(coltype, None)
    if not conv_info:
        return []

    results = []
    allowed = conv_info["allowed_conversions"]
    convs = conv_info["dialect_conversions"]

    for field in allowed:
        conv = convs[dialect].get(field, None)
        if not conv:
            continue
        format_args = get_string_format_args(conv)
        assert not any([x != "" for x in format_args]), (
            "Field conversion has non-named format arguments: %s" % conv
        )
        if format_args:
            conv = conv.format(*[column_fullname(column) for i in format_args])
        results.append((field, conv))

    return results


def contains_sql_keywords(sql):
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
    if isinstance(sql, str):
        sql = sp.parse(sql)

    aggr_types = {x.lower() for x in get_class_vars(AggregationTypes)}

    for token in sql:
        if isinstance(token, sp.sql.Function):
            name = token.get_name()
            # NOTE: If AggregationTypes naming is changed, this could fail
            if name.lower() in aggr_types:
                return True
        if isinstance(token, sp.sql.TokenList):
            token_result = contains_aggregation(token)
            if token_result:
                return True
    return False


def type_string_to_sa_type(type_string):
    # This only checks the top level sqlalchemy module for matching type
    # classes. Therefore you can not specify dialect-specific types at this
    # time.
    parts = type_string.split("(")
    type_args = []
    if len(parts) > 1:
        assert len(parts) == 2, "Unable to parse type string: %s" % type_string
        type_args = ast.literal_eval(parts[1].rstrip(")") + ",")
    type_name = parts[0]
    type_cls = getattr(sa, type_name, None)
    if not type_cls:
        raise InvalidSQLAlchemyTypeString(
            "Could not find matching type for %s" % type_name
        )
    return type_cls(*type_args)


def infer_aggregation_and_rounding(column):
    if isinstance(column.type, tuple(INTEGER_SA_TYPES)):
        return AggregationTypes.SUM, 0
    if isinstance(column.type, tuple(FLOAT_SA_TYPES)):
        rounding = column.type.scale
        precision = column.type.precision
        if rounding is None and precision is None:
            aggregation = AggregationTypes.SUM
        else:
            whole_digits = precision - rounding
            if whole_digits <= DIGIT_THRESHOLD_FOR_AVG_AGGR:
                aggregation = AggregationTypes.AVG
            else:
                aggregation = AggregationTypes.SUM
        return aggregation, rounding
    assert False, "Column %s is not a numeric type" % column


def aggregation_to_sqla_func(aggregation):
    return AGGREGATION_SQLA_FUNC_MAP[aggregation]


def is_probably_metric(column, formula=None):
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
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


def printexpr(expr):
    print(sqla_compile(expr))


def column_fullname(column, prefix=None):
    name = "%s.%s" % (column.table.fullname, column.name)
    if prefix:
        name = prefix + "." + name
    return name


def get_sqla_clause(column, criterion, negate=False):
    """
    TODO: postgresql like is case sensitive, but mysql like is not
    - postgres also supports ilike to specify case insensitive
    OPTION: look at dialect to determine function
    """
    field, op, values = criterion
    op = op.lower()
    if not isinstance(values, (list, tuple)):
        values = [values]

    use_or = True
    has_null = False
    for v in values:
        # len() check is to avoid calling string.lower() on huge strings
        if v is None or (isinstance(v, str) and len(v) == 4 and v.lower() == "null"):
            has_null = True

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
            assert len(value) == 2, "Between clause value must have length of 2"
            clauses.append(column.between(value[0], value[1]))
    elif op == "not between":
        clauses = []
        for value in values:
            assert len(value) == 2, "Between clause value must have length of 2"
            clauses.append(sa.not_(column.between(value[0], value[1])))
    elif op == "like":
        clauses = [column.like(v) for v in values]
    elif op == "not like":
        use_or = False
        clauses = [sa.not_(column.like(v)) for v in values]
    else:
        assert False, "Invalid criterion operand: %s" % op

    if use_or:
        clause = sa.or_(*clauses)
    else:
        clause = sa.and_(*clauses)

    if negate:
        clause = sa.not_(clause)

    return clause


def to_mysql_type(type):
    return type.compile(dialect=mysql_dialect())


def to_postgresql_type(type):
    return type.compile(dialect=postgresql_dialect())


def to_sqlite_type(type):
    return type.compile(dialect=sqlite_dialect())


# https://github.com/sqlalchemy/sqlalchemy/wiki/CompiledComments
def comment(self, comment):
    self._added_comment = comment
    return self


exp.ClauseElement.comment = comment
exp.ClauseElement._added_comment = None


def _compile_element(elem, prepend_newline=False):
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


def get_postgres_schemas(conn):
    qr = conn.execute(
        sa.text(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name not LIKE 'pg_%' and schema_name != 'information_schema'"
        )
    )
    return [x["schema_name"] for x in qr.fetchall()]


def get_postgres_pid(conn):
    qr = conn.execute("select pg_backend_pid()")
    pid = qr.fetchone()[0]
    return pid
