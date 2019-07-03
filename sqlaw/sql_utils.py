import ast

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
import sqlparse as sp
from toolbox import dbg, st, get_class_vars

from sqlaw.core import (NUMERIC_SA_TYPES,
                        INTEGER_SA_TYPES,
                        FLOAT_SA_TYPES,
                        AggregationTypes)

DIGIT_THRESHOLD_FOR_AVG_AGGR = 1

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
    parts = type_string.split('(')
    type_args = []
    if len(parts) > 1:
        assert len(parts) == 2, 'Unable to parse type string: %s' % type_string
        type_args = ast.literal_eval(parts[1].rstrip(')') + ',')
    type_name = parts[0]
    type_cls = getattr(sa, type_name, None)
    if not type_cls:
        raise InvalidSQLAlchemyTypeString('Could not find matching type for %s' % type_name)
    return type_cls(*type_args)

def infer_aggregation_and_rounding(column):
    if type(column.type) in INTEGER_SA_TYPES:
        return AggregationTypes.SUM, 0
    if type(column.type) in FLOAT_SA_TYPES:
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
    assert False, 'Column %s is not a numeric type' % column

def aggregation_to_sqla_func(aggregation):
    return AGGREGATION_SQLA_FUNC_MAP[aggregation]

def is_probably_fact(column):
    if type(column.type) not in NUMERIC_SA_TYPES:
        return False
    if column.primary_key:
        return False
    if column.name.endswith('_id') or column.name.endswith('Id') or column.name == 'id':
        return False
    return True

def sqla_compile(expr):
    return str(expr.compile(compile_kwargs={"literal_binds": True}))

def printexpr(expr):
    print(sqla_compile(expr))

def column_fullname(column):
    return '%s.%s' % (column.table.fullname, column.name)

def get_sqla_clause(column, criterion, negate=False):
    field, op, values = criterion
    op = op.lower()
    if not isinstance(values, (list, tuple)):
        values = [values]

    use_or = True
    has_null = False
    for v in values:
        # len() check is to avoid string.lower() on huge strings
        if v is None or (isinstance(v, str) and len(v) == 4 and v.lower() == 'null'):
            has_null = True

    if op == '=':
        clauses = [column == v if v is not None else column._is(None) for v in values]
    elif op == '!=':
        clauses = [column != v if v is not None else column.isnot(None) for v in values]
    elif op == '>':
        clauses = [column > v for v in values]
    elif op == '<':
        clauses = [column < v for v in values]
    elif op == '>=':
        clauses = [column >= v for v in values]
    elif op == '<=':
        clauses = [column <= v for v in values]
    elif op == 'in':
        if has_null:
            clauses = [column == v if v is not None else column._is(None) for v in values]
        else:
            clauses = [column.in_(values)]
    elif op == 'not in':
        use_or = False
        if has_null:
            clauses = [column != v if v is not None else column.isnot(None) for v in values]
        else:
            clauses = [sa.not_(column.in_(values))]
    elif op == 'between':
        clauses = []
        for value in values:
            assert len(value) == 2, 'Between clause value must have length of 2'
            clauses.append(column.between(value[0], value[1]))
    elif op == 'like':
        clauses = [column.like(v) for v in values]
    elif op == 'not like':
        use_or = False
        clauses = [sa.not_(column.like(v)) for v in values]
    else:
        assert False, 'Invalid criterion operand: %s' % op

    if use_or:
        clause = sa.or_(*clauses)
    else:
        clause = sa.and_(*clauses)

    if negate:
        clause = sa.not_(clause)
    return clause

def to_sqlite_type(type):
    return type.compile(dialect=sqlite_dialect())
