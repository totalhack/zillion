from collections import defaultdict, OrderedDict
import decimal
import random
from sqlite3 import connect, Row
import time

import networkx as nx
import numpy as np
from orderedset import OrderedSet
import pandas as pd
import sqlalchemy as sa

from sqlaw.configs import (AdHocFieldSchema,
                           AdHocFactSchema,
                           ColumnInfoSchema,
                           TableInfoSchema,
                           FactConfigSchema,
                           TechnicalInfoSchema,
                           DimensionConfigSchema,
                           is_valid_field_name)
from sqlaw.core import (NUMERIC_SA_TYPES,
                        INTEGER_SA_TYPES,
                        FLOAT_SA_TYPES,
                        ROW_FILTER_OPS,
                        AggregationTypes,
                        TableTypes,
                        FieldTypes,
                        TechnicalTypes,
                        parse_technical_string,
                        field_safe_name)
from sqlaw.sql_utils import (infer_aggregation_and_rounding,
                             aggregation_to_sqla_func,
                             contains_aggregation,
                             type_string_to_sa_type,
                             is_probably_fact,
                             sqla_compile,
                             column_fullname,
                             get_sqla_clause,
                             to_sqlite_type)
from sqlaw.utils import (dbg,
                         dbgsql,
                         warn,
                         error,
                         st,
                         initializer,
                         is_int,
                         orderedsetify,
                         get_string_format_args,
                         PrintMixin,
                         MappingMixin)

DEFAULT_IFNULL_VALUE = '--'
MAX_FORMULA_DEPTH = 3
# Last unicode char - this helps get the rollup rows to sort last, but may
# need to be replaced for presentation
ROLLUP_INDEX_LABEL = chr(1114111)
ROLLUP_INDEX_PRETTY_LABEL = '::'
ROLLUP_TOTALS = 'totals'

PANDAS_ROLLUP_AGGR_TRANSLATION = {
    AggregationTypes.AVG: 'mean',
    AggregationTypes.COUNT: 'sum',
    AggregationTypes.COUNT_DISTINCT: 'sum',
}

class InvalidFieldException(Exception):
    pass

class MaxFormulaDepthException(Exception):
    pass

class DataSourceMap(dict):
    pass

class SQLAWInfo(MappingMixin):
    schema = None

    @initializer
    def __init__(self, **kwargs):
        assert self.schema, 'SQLAWInfo subclass must have a schema defined'
        self.schema().load(self)

    @classmethod
    def create(cls, sqlaw_info):
        if isinstance(sqlaw_info, cls):
            return sqlaw_info
        assert isinstance(sqlaw_info, dict), 'Raw info must be a dict: %s' % sqlaw_info
        sqlaw_info = cls.schema().load(sqlaw_info)
        return cls(**sqlaw_info)

class TableInfo(SQLAWInfo, PrintMixin):
    repr_attrs = ['type', 'active', 'autocolumns', 'parent']
    schema = TableInfoSchema

class ColumnInfo(SQLAWInfo, PrintMixin):
    repr_attrs = ['fields', 'active']
    schema = ColumnInfoSchema

    @classmethod
    def create(cls, sqlaw_info):
        if isinstance(sqlaw_info, cls):
            return sqlaw_info
        assert isinstance(sqlaw_info, dict), 'Raw info must be a dict: %s' % sqlaw_info

        # Need to reformat the fields from config format to ColumnInfo format
        fields_dict = {}
        for field_row in sqlaw_info.get('fields', []):
            if isinstance(field_row, str):
                fields_dict[field_row] = None
                continue

            if isinstance(field_row, (tuple, list)):
                name = field_row[0]
                config = field_row[1]
                # TODO: column formulas should probably be verified to be using fully
                # qualified column names. We could use something like sqlparse to
                # help analyze this for SQL-based datasources, but not everything
                # will be sql.
                fields_dict[name] = config
                continue

            assert False, 'Invalid field row format: %s' % sqlaw_info

        sqlaw_info['fields'] = fields_dict
        sqlaw_info = cls.schema().load(sqlaw_info)
        return cls(**sqlaw_info)

class TableSet(PrintMixin):
    repr_attrs = ['ds_name', 'join_list', 'grain', 'target_fields']

    @initializer
    def __init__(self, ds_name, ds_table, join_list, grain, target_fields):
        pass

    def get_covered_facts(self, warehouse):
        covered_facts = get_table_facts(warehouse, self.ds_table)
        return covered_facts

    def get_covered_fields(self, warehouse):
        covered_fields = get_table_fields(warehouse, self.ds_table)
        return covered_fields

class NeighborTable(PrintMixin):
    repr_attrs = ['table_name', 'join_fields']

    @initializer
    def __init__(self, table, join_fields):
        self.table_name = table.fullname

class JoinInfo(PrintMixin):
    repr_attrs = ['table_names', 'join_fields']

    @initializer
    def __init__(self, table_names, join_fields):
        pass

class JoinList(PrintMixin):
    '''Field map represents the requested fields this join list is meant to satisfy'''
    repr_attrs = ['table_names', 'field_map']

    @initializer
    def __init__(self, joins, field_map):
        self.table_names = OrderedSet()
        for join in self.joins:
            for table_name in join.table_names:
                self.table_names.add(table_name)

    def __key(self):
        return tuple(self.table_names)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.__key() == other.__key()

def joins_from_path(graph, path, field_map=None):
    joins = []
    if len(path) == 1:
        # A placeholder join that is really just a single table
        join_info = JoinInfo(path, None)
        joins.append(join_info)
    else:
        for i, node in enumerate(path):
            if i == (len(path) - 1):
                break
            start, end = path[i], path[i+1]
            edge = graph.edges[start, end]
            join_info = JoinInfo([start, end], edge['join_fields'])
            joins.append(join_info)
    return JoinList(joins, field_map=field_map)

def get_table_fields(table):
    fields = set()
    for col in table.c:
        for field in col.sqlaw.fields:
            fields.add(field)
    return fields

def get_table_field_column(table, field_name):
    for col in table.c:
        for field in col.sqlaw.fields:
            if field == field_name:
                return col
    assert False, 'Field %s is not present in table %s' % (field_name, table.fullname)

def get_table_facts(warehouse, table):
    facts = set()
    for col in table.c:
        for field in col.sqlaw.fields:
            if field in warehouse.facts:
                facts.add(field)
    return facts

def get_table_dimensions(warehouse, table):
    dims = set()
    for col in table.c:
        for field in col.sqlaw.fields:
            if field in warehouse.dimensions:
                dims.add(field)
    return dims

class Technical(MappingMixin, PrintMixin):
    repr_attrs = ['type', 'window', 'min_periods']

    @initializer
    def __init__(self, **kwargs):
        pass

    @classmethod
    def create(cls, info):
        if isinstance(info, cls):
            return info
        if isinstance(info, str):
            info = parse_technical_string(info)
        assert isinstance(info, dict), 'Raw info must be a dict: %s' % info
        info = TechnicalInfoSchema().load(info)
        return cls(**info)

class Field(PrintMixin):
    repr_attrs = ['name']
    ifnull_value = DEFAULT_IFNULL_VALUE

    @initializer
    def __init__(self, name, type, **kwargs):
        is_valid_field_name(name)
        self.column_map = defaultdict(list)
        if isinstance(type, str):
            self.type = type_string_to_sa_type(type)

    def add_column(self, ds_name, column):
        current_cols = [column_fullname(col) for col in self.column_map[ds_name]]
        fullname = column_fullname(column)
        if fullname in current_cols:
            warn('Column %s.%s is already mapped to field %s' % (ds_name, fullname, self.name))
            return
        self.column_map[ds_name].append(column)

    def get_columns(self, ds_name):
        return self.column_map[ds_name]

    def get_formula_fields(self, warehouse, depth=0):
        return []

    def get_ds_expression(self, column):
        ds_formula = column.sqlaw.fields[self.name].get('ds_formula', None) if column.sqlaw.fields[self.name] else None
        if not ds_formula:
            return sa.func.ifnull(column, self.ifnull_value).label(self.name)
        return sa.func.ifnull(sa.text(ds_formula), self.ifnull_value).label(self.name)

    def get_final_select_clause(self, *args, **kwargs):
        return self.name

    # https://stackoverflow.com/questions/2909106/whats-a-correct-and-good-way-to-implement-hash
    def __key(self):
        return self.name

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.__key() == other.__key()

class Fact(Field):
    def __init__(self, name, type, aggregation=AggregationTypes.SUM, rounding=None,
                 weighting_fact=None, technical=None, **kwargs):
        # TODO: enforce this in config instead?
        if weighting_fact:
            assert aggregation == AggregationTypes.AVG,\
                'Weighting facts are only supported for aggregation type: %s' % AggregationTypes.AVG

        if technical:
            technical = Technical.create(technical)

        super(Fact, self).__init__(name, type, aggregation=aggregation, rounding=rounding,
                                   weighting_fact=weighting_fact, technical=technical, **kwargs)

    def get_ds_expression(self, column):
        expr = column
        aggr = aggregation_to_sqla_func(self.aggregation)
        skip_aggr = False

        ds_formula = column.sqlaw.fields[self.name].get('ds_formula', None) if column.sqlaw.fields[self.name] else None
        if ds_formula:
            if contains_aggregation(ds_formula):
                warn('Datasource formula contains aggregation, skipping default logic!')
                skip_aggr = True
            # TODO: is literal_column() appropriate vs text()?
            expr = sa.literal_column(ds_formula)

        if not skip_aggr:
            if self.aggregation in [AggregationTypes.COUNT, AggregationTypes.COUNT_DISTINCT]:
                if self.rounding:
                    warn('Ignoring rounding for count field: %s' % self.name)
                return aggr(expr).label(self.name)

            if self.weighting_fact:
                # TODO: check weighting fact is present in this table when reading config
                w_column = get_table_field_column(column.table, self.weighting_fact)
                w_column_name = column_fullname(w_column)
                # NOTE: 1.0 multiplication is a hack to ensure results are not rounded
                # to integer values improperly by some database dialects such as sqlite
                expr = sa.func.sum(sa.text('1.0') * expr * sa.text(w_column_name)) / sa.func.sum(sa.text(w_column_name))
            else:
                expr = aggr(expr)

        # XXX Only applying rounding on final result DataFrame
        # if self.rounding:
        #    expr = sa.func.round(expr, self.rounding)

        return expr.label(self.name)

    def get_final_select_clause(self, *args, **kwargs):
        return self.name

class Dimension(Field):
    pass

class FormulaField(Field):
    def __init__(self, name, formula, **kwargs):
        super(FormulaField, self).__init__(name, None, formula=formula, **kwargs)

    def get_formula_fields(self, warehouse, depth=0):
        if depth > MAX_FORMULA_DEPTH:
            raise MaxFormulaDepthException()

        raw_formula = self.formula
        raw_fields = set()
        formula_fields = get_string_format_args(self.formula)
        field_formula_map = {}

        for field_name in formula_fields:
            field = warehouse.get_field(field_name)
            if isinstance(field, FormulaFact):
                try:
                    sub_fields, sub_formula = field.get_formula_fields(warehouse, depth=depth+1)
                except MaxFormulaDepthException as e:
                    if depth != 0:
                        raise
                    raise MaxFormulaDepthException('Maximum formula recursion depth exceeded for %s: %s' %
                                                   (self.name, self.formula))
                for sub_field in sub_fields:
                    raw_fields.add(sub_field)
                field_formula_map[field_name] = '(' + sub_formula + ')'
            else:
                field_formula_map[field_name] = '{' + field_name + '}'
                raw_fields.add(field_name)

        raw_formula = self.formula.format(**field_formula_map)
        return raw_fields, raw_formula

    def get_ds_expression(self, column):
        assert False, 'Formula-based Fields do not support get_ds_expression'

    def get_final_select_clause(self, warehouse):
        formula_fields, raw_formula = self.get_formula_fields(warehouse)
        format_args = {k:k for k in formula_fields}
        clause = sa.text(raw_formula.format(**format_args))
        return sqla_compile(clause)

class FormulaFact(FormulaField):
    repr_atts = ['name', 'formula', 'technical']

    def __init__(self, name, formula, aggregation=AggregationTypes.SUM, rounding=None, weighting_fact=None,
                 technical=None, **kwargs):
        # TODO: ensure formula params are valid fields as objects are formed
        # We'd need to defer formula facts until the end to achieve this.
        if technical:
            technical = Technical.create(technical)

        super(FormulaFact, self).__init__(name, formula, aggregation=aggregation, rounding=rounding,
                                          weighting_fact=weighting_fact, technical=technical, **kwargs)

    def get_final_select_clause(self, warehouse):
        formula_fields, raw_formula = self.get_formula_fields(warehouse)
        # XXX Only applying rounding on final result DataFrame
        if False and self.rounding:
            # NOTE: 1.0 multiplication is a hack to ensure results are not rounded
            # to integer values improperly by some database dialects such as sqlite
            format_args = {k:('(1.0*%s)' % k) for k in formula_fields}
            raw = sa.text(raw_formula.format(**format_args))
            clause = sa.func.round(raw, self.rounding)
        else:
            format_args = {k:k for k in formula_fields}
            clause = sa.text(raw_formula.format(**format_args))

        return sqla_compile(clause)

class AdHocField(FormulaField):
    @classmethod
    def create(cls, obj):
        schema = AdHocFieldSchema()
        field_def = schema.load(obj)
        return cls(field_def['name'], field_def['formula'])

class AdHocFact(FormulaFact):
    def __init__(self, name, formula, technical=None, rounding=None):
        super(AdHocFact, self).__init__(name, formula, technical=technical, rounding=rounding)

    @classmethod
    def create(cls, obj):
        schema = AdHocFactSchema()
        field_def = schema.load(obj)
        return cls(field_def['name'], field_def['formula'], technical=field_def['technical'],
                   rounding=field_def['rounding'])

class AdHocDimension(AdHocField):
    pass

def create_fact(fact_def):
    if fact_def['formula']:
        fact = FormulaFact(
            fact_def['name'],
            fact_def['formula'],
            aggregation=fact_def['aggregation'],
            rounding=fact_def['rounding'],
            weighting_fact=fact_def['weighting_fact'],
            technical=fact_def['technical']
        )
    else:
        fact = Fact(
            fact_def['name'],
            fact_def['type'],
            aggregation=fact_def['aggregation'],
            rounding=fact_def['rounding'],
            weighting_fact=fact_def['weighting_fact'],
            technical=fact_def['technical']
        )
    return fact

def create_dimension(dim_def):
    dim = Dimension(dim_def['name'], dim_def['type'])
    return dim

class Warehouse:
    @initializer
    def __init__(self, ds_map, config=None, ds_priority=None):
        if ds_priority:
            assert isinstance(ds_priority, list),\
                'Invalid format for ds_priority, must be list of datesource names: %s' % ds_priority
            for ds_name in ds_priority:
                assert ds_name in ds_map, 'Datasource %s is in ds_priority but not in datasource map' % ds_name
        self.tables = defaultdict(dict)
        self.fact_tables = defaultdict(dict)
        self.dimension_tables = defaultdict(dict)
        self.facts = defaultdict(dict)
        self.dimensions = defaultdict(dict)
        self.table_field_map = defaultdict(dict)
        self.ds_graphs = {}

        if config:
            self.apply_config(self.config)

        for ds_name, metadata in ds_map.items():
            self.ensure_metadata_info(metadata)
            self.populate_table_field_map(ds_name, metadata)
            for table in metadata.tables.values():
                self.add_table(ds_name, table)
            self.add_ds_graph(ds_name)

    def __repr__(self):
        return 'Datasources: %s' % (self.ds_map.keys())

    def ensure_metadata_info(self, metadata):
        '''Ensure that all sqlaw info are of proper type'''
        for table in metadata.tables.values():
            sqlaw_info = table.info.get('sqlaw', None)
            if not sqlaw_info:
                setattr(table, 'sqlaw', None)
                continue

            table.info['sqlaw'] = TableInfo.create(sqlaw_info)
            setattr(table, 'sqlaw', table.info['sqlaw'])

            for column in table.c:
                sqlaw_info = column.info.get('sqlaw', None)
                if not sqlaw_info:
                    if not table.info['sqlaw'].autocolumns:
                        setattr(column, 'sqlaw', None)
                        continue
                    else:
                        sqlaw_info = {}
                sqlaw_info['fields'] = sqlaw_info.get('fields', {field_safe_name(column_fullname(column)): None})

                if column.primary_key:
                    dim_count = 0
                    for field in sqlaw_info['fields']:
                        if field in self.dimensions or field not in self.facts:
                            dim_count += 1
                        assert dim_count < 2, 'Primary key column may only map to a single dimension: %s' % column

                column.info['sqlaw'] = ColumnInfo.create(sqlaw_info)
                setattr(column, 'sqlaw', column.info['sqlaw'])

    def populate_table_field_map(self, ds_name, metadata):
        for table in metadata.tables.values():
            if not table.sqlaw:
                continue
            for column in table.c:
                if not column.sqlaw:
                    continue
                fields = column.sqlaw.fields
                for field in fields:
                    self.table_field_map[ds_name].setdefault(table.fullname, {}).setdefault(field, []).append(column)

    def field_exists(self, field):
        if field in self.dimensions or field in self.facts:
            return True
        return False

    def get_field(self, obj):
        if isinstance(obj, str):
            if obj in self.facts:
                return self.facts[obj]
            if obj in self.dimensions:
                return self.dimensions[obj]
            assert False, 'Field %s does not exist' % obj
        elif isinstance(obj, dict):
            return AdHocField.create(obj)
        else:
            raise InvalidFieldException('Invalid field object: %s' % obj)

    def get_fact(self, obj):
        if isinstance(obj, str):
            if obj not in self.facts:
                raise InvalidFieldException('Invalid fact name: %s' % obj)
            return self.facts[obj]
        elif isinstance(obj, dict):
            fact = AdHocFact.create(obj)
            assert fact.name not in self.facts, 'AdHocFact can not use name of an existing fact: %s' % fact.name
            return fact
        else:
            raise InvalidFieldException('Invalid fact object: %s' % obj)

    def get_dimension(self, obj):
        if isinstance(obj, str):
            if obj not in self.dimensions:
                raise InvalidFieldException('Invalid dimensions name: %s' % obj)
            return self.dimensions[obj]
        elif isinstance(obj, dict):
            dim = AdHocDimension.create(obj)
            assert dim.name not in self.dimensions, 'AdHocDimension can not use name of an existing dimension: %s' % dim.name
            return dim
        else:
            raise InvalidFieldException('Invalid fact object: %s' % obj)

    def get_primary_key_fields(self, primary_key):
        pk_fields = set()
        for col in primary_key:
            pk_dims = [x for x in col.sqlaw.fields if x in self.dimensions]
            assert len(pk_dims) == 1, \
                'Primary key column has multiple dimensions: %s/%s' % (col, col.sqlaw.fields)
            pk_fields.add(pk_dims[0])
        return pk_fields

    def find_neighbor_tables(self, ds_name, table):
        neighbor_tables = []
        fields = get_table_fields(table)

        if table.sqlaw.type == TableTypes.FACT:
            # Find dimension tables whose primary key is contained in the fact table
            dim_tables = self.dimension_tables.get(ds_name, [])
            for dim_table in self.dimension_tables[ds_name].values():
                dt_pk_fields = self.get_primary_key_fields(dim_table.primary_key)
                can_join = True
                for field in dt_pk_fields:
                    if field not in fields:
                        can_join = False
                        break
                if can_join:
                    neighbor_tables.append(NeighborTable(dim_table, dt_pk_fields))

        # Add parent table if present
        parent_name = table.sqlaw.parent
        if parent_name:
            parent = self.tables[ds_name][parent_name]
            pk_fields = self.get_primary_key_fields(parent.primary_key)
            for pk_field in pk_fields:
                assert pk_field in fields, ('Table %s is parent of %s but primary key %s is not in both' %
                                            (parent.fullname, table.fullname, pk_fields))
            neighbor_tables.append(NeighborTable(parent, pk_fields))
        return neighbor_tables

    def add_ds_graph(self, ds_name):
        assert not (ds_name in self.ds_graphs), 'Datasource %s already has a graph' % ds_name
        graph = nx.DiGraph()
        self.ds_graphs[ds_name] = graph
        tables = self.tables[ds_name].values()
        for table in tables:
            graph.add_node(table.fullname)
            neighbors = self.find_neighbor_tables(ds_name, table)
            for neighbor in neighbors:
                graph.add_node(neighbor.table.fullname)
                graph.add_edge(table.fullname, neighbor.table.fullname, join_fields=neighbor.join_fields)

    def add_table(self, ds_name, table):
        if not table.sqlaw:
            return
        self.tables[ds_name][table.fullname] = table
        if table.sqlaw.type == TableTypes.FACT:
            self.add_fact_table(ds_name, table)
        elif table.sqlaw.type == TableTypes.DIMENSION:
            self.add_dimension_table(ds_name, table)
        else:
            assert False, 'Invalid table type: %s' % table_info.type

    def add_fact_table(self, ds_name, table):
        self.fact_tables[ds_name][table.fullname] = table
        for column in table.c:
            if not column.sqlaw:
                continue

            for field in column.sqlaw.fields:
                if field in self.facts:
                    self.facts[field].add_column(ds_name, column)
                elif field in self.dimensions:
                    self.dimensions[field].add_column(ds_name, column)
                elif table.sqlaw.autocolumns:
                    if is_probably_fact(column):
                        self.add_fact_column(ds_name, column, field)
                    else:
                        self.add_dimension_column(ds_name, column, field)

    def get_ds_tables_with_fact(self, fact):
        ds_tables = defaultdict(list)
        ds_fact_columns = self.facts[fact].column_map
        for ds_name, columns in ds_fact_columns.items():
            for column in columns:
                ds_tables[ds_name].append(column.table)
        dbg('found %d datasource tables with fact %s' % (len(ds_tables), fact))
        return ds_tables

    def get_ds_dim_tables_with_dim(self, dim):
        ds_tables = defaultdict(list)
        ds_dim_columns = self.dimensions[dim].column_map
        for ds_name, columns in ds_dim_columns.items():
            for column in columns:
                if column.table.sqlaw.type != TableTypes.DIMENSION:
                    continue
                ds_tables[ds_name].append(column.table)
        dbg('found %d datasource tables with dim %s' % (len(ds_tables), dim))
        return ds_tables

    def add_dimension_table(self, ds_name, table):
        self.dimension_tables[ds_name][table.fullname] = table
        for column in table.c:
            if not column.sqlaw:
                continue

            for field in column.sqlaw.fields:
                if field in self.facts:
                    assert False, 'Dimension table has fact field: %s' % field
                elif field in self.dimensions:
                    self.dimensions[field].add_column(ds_name, column)
                elif table.sqlaw.autocolumns:
                    self.add_dimension_column(ds_name, column, field)

    def add_fact(self, fact):
        if fact.name in self.facts:
            warn('Fact %s is already in warehouse facts' % fact.name)
            return
        self.facts[fact.name] = fact

    def add_fact_column(self, ds_name, column, field):
        if field not in self.facts:
            dbg('Adding fact %s from column %s.%s' % (field, ds_name, column_fullname(column)))
            aggregation, rounding = infer_aggregation_and_rounding(column)
            fact = Fact(field, column.type, aggregation=aggregation, rounding=rounding)
            self.add_fact(fact)
        self.facts[field].add_column(ds_name, column)

    def add_dimension(self, dimension):
        if dimension.name in self.dimensions:
            warn('Dimension %s is already in warehouse dimensions' % dimension.name)
            return
        self.dimensions[dimension.name] = dimension

    def add_dimension_column(self, ds_name, column, field):
        if field not in self.dimensions:
            dbg('Adding dimension %s from column %s.%s' % (field, ds_name, column_fullname(column)))
            dimension = Dimension(field, column.type)
            self.add_dimension(dimension)
        self.dimensions[field].add_column(ds_name, column)

    def get_tables_with_dimension(self, ds_name, dimension):
        return [x.table for x in self.dimensions[dimension][ds_name]]

    def find_joins_to_dimension(self, ds_name, table, dimension):
        ds_graph = self.ds_graphs[ds_name]
        joins = []

        for column in self.dimensions[dimension].get_columns(ds_name):
            if column.table == table:
                paths = [[table.fullname]]
            else:
                # TODO: use shortest_simple_paths instead?
                paths = nx.all_simple_paths(ds_graph, table.fullname, column.table.fullname)

            if not paths:
                continue

            for path in paths:
                field_map = {dimension:column}
                join_list = joins_from_path(ds_graph, path, field_map=field_map)
                joins.append(join_list)

        dbg('Found joins to dim %s for table %s:' % (dimension, table.fullname))
        dbg(joins)
        return joins

    def get_possible_joins(self, ds_name, table, grain):
        assert table.fullname in self.tables[ds_name],\
            'Could not find table %s in datasource %s' % (table.fullname, ds_name)

        if not grain:
            dbg('No grain specified, ignoring joins')
            return None

        possible_dim_joins = {}
        for dimension in grain:
            dim_joins = self.find_joins_to_dimension(ds_name, table, dimension)
            if not dim_joins:
                dbg('table %s can not satisfy dimension %s' % (table.fullname, dimension))
                return None

            possible_dim_joins[dimension] = dim_joins

        possible_joins = self.consolidate_field_joins(possible_dim_joins)
        dbg('possible joins:')
        dbg(possible_joins)
        return possible_joins

    def find_possible_table_sets(self, ds_name, ds_tables_with_field, field, grain):
        table_sets = []
        for field_table in ds_tables_with_field:
            if (not grain) or grain.issubset(get_table_fields(field_table)):
                table_set = TableSet(ds_name, field_table, None, grain, set([field]))
                table_sets.append(table_set)
                continue

            joins = self.get_possible_joins(ds_name, field_table, grain)
            if not joins:
                dbg('table %s can not join at grain %s' % (field_table.fullname, grain))
                continue

            dbg('adding %d possible join(s) to table %s' % (len(joins), field_table.fullname))
            for join_list, covered_dims in joins.items():
                table_set = TableSet(ds_name, field_table, join_list, grain, set([field]))
                table_sets.append(table_set)

        dbg(table_sets)
        return table_sets

    def get_ds_table_sets(self, ds_tables, field, grain):
        '''Returns all table sets that can satisfy grain in each datasource'''
        ds_table_sets = {}
        for ds_name, ds_tables_with_field in ds_tables.items():
            possible_table_sets = self.find_possible_table_sets(ds_name, ds_tables_with_field, field, grain)
            if not possible_table_sets:
                continue
            ds_table_sets[ds_name] = possible_table_sets
        dbg(ds_table_sets)
        return ds_table_sets

    def choose_best_data_source(self, ds_names):
        if self.ds_priority:
            for ds_name in self.ds_priority:
                if ds_name in ds_names:
                    return ds_name

        # TODO: eventually it would be nice to choose a datasource if:
        #  A) Its historically been faster
        #  B) All of the requested data can be pulled from one datasource
        warn('No datasource priorities established, picking random option')
        assert ds_names, 'No datasource names provided'
        return random.choice(ds_names)

    def choose_best_table_set(self, ds_table_sets):
        ds_name = self.choose_best_data_source(list(ds_table_sets.keys()))
        # TODO: establish table set priorities based on query performance/complexity
        warn('Just picking first available table set for now')
        return ds_table_sets[ds_name][0]

    def get_fact_table_set(self, fact, grain):
        dbg('fact:%s grain:%s' % (fact, grain))
        ds_fact_tables = self.get_ds_tables_with_fact(fact)
        ds_table_sets = self.get_ds_table_sets(ds_fact_tables, fact, grain)
        assert ds_table_sets, 'No table set found for fact %s at grain %s' % (fact, grain)
        table_set = self.choose_best_table_set(ds_table_sets)
        return table_set

    def get_dimension_table_set(self, grain):
        # TODO: this needs more thorough review/testing
        dbg('grain:%s' % grain)

        table_set = None
        for dim_name in grain:
            ds_dim_tables = self.get_ds_dim_tables_with_dim(dim_name)
            ds_table_sets = self.get_ds_table_sets(ds_dim_tables, dim_name, grain)
            if not ds_table_sets:
                continue
            table_set = self.choose_best_table_set(ds_table_sets)

        assert table_set, 'No dimension table set found to meet grain: %s' % grain
        return table_set

    @classmethod
    def consolidate_field_joins(cls, field_joins):
        '''This takes a mapping of fields to joins that satisfy that field and
        returns a minimized map of joins to fields satisfied by that join'''

        join_fields = defaultdict(set) # map of join to fields it covers

        # First get all joins and fields they cover
        for field, joins in field_joins.items():
            for join in joins:
                if join in join_fields:
                    dbg('join %s already used, adding field %s' % (join, field))
                join_fields[join].add(field)

        # Then consolidate based on subsets
        joins_to_delete = []
        for join in list(join_fields.keys()):
            for other_join in list(join_fields.keys()):
                if join == other_join or other_join in joins_to_delete:
                    continue
                if join.table_names.issubset(other_join.table_names):
                    joins_to_delete.append(join)
                    fields = join_fields[join]
                    dbg('Fields %s satisfied by other join %s' % (fields, other_join.table_names))
                    for key, value in join.field_map.items():
                        if key not in other_join.field_map:
                            other_join.field_map[key] = value
                    join_fields[other_join] = join_fields[other_join].union(fields)

        for join in joins_to_delete:
            if join in join_fields:
                del join_fields[join]

        return join_fields

    def apply_config(self, config):
        '''
        This will update or add sqlaw info to the schema item info dict if it
        appears in the datasource config
        '''

        for fact_def in config.get('facts', []):
            if isinstance(fact_def, dict):
                schema = FactConfigSchema()
                fact_def = schema.load(fact_def)
                fact = create_fact(fact_def)
            else:
                assert isinstance(fact_def, Fact),\
                    'Fact definition must be a dict-like object or a Fact object'
                fact = fact_def
            self.add_fact(fact)

        for dim_def in config.get('dimensions', []):
            if isinstance(dim_def, dict):
                schema = DimensionConfigSchema()
                dim_def = schema.load(dim_def)
                dim = create_dimension(dim_def)
            else:
                assert isinstance(dim_def, Dimension),\
                    'Dimension definition must be a dict-like object or a Dimension object'
                dim = dim_def
            self.add_dimension(dim)

        for ds_name, metadata in self.ds_map.items():
            if ds_name not in config['datasources']:
                continue

            ds_config = config['datasources'][ds_name]

            for table in metadata.tables.values():
                if table.fullname not in ds_config['tables']:
                    continue

                table_config = ds_config['tables'][table.fullname]
                column_configs = table_config.get('columns', None)
                if 'columns' in table_config:
                    del table_config['columns']

                sqlaw_info = table.info.get('sqlaw', {})
                # Config takes precendence over values on table objects
                sqlaw_info.update(table_config)
                table.info['sqlaw'] = TableInfo.create(sqlaw_info)

                autocolumns = table.info['sqlaw'].autocolumns
                if not autocolumns:
                    assert column_configs, ('Table %s.%s has autocolumns=False and no column configs' %
                                            (ds_name, table.fullname))
                if not column_configs:
                    continue

                for column in table.columns:
                    if column.name not in column_configs:
                        continue

                    column_config = column_configs[column.name]
                    sqlaw_info = column.info.get('sqlaw', {})
                    # Config takes precendence over values on column objects
                    sqlaw_info.update(column_config)
                    sqlaw_info['fields'] = sqlaw_info.get('fields', [field_safe_name(column_fullname(column))])
                    column.info['sqlaw'] = ColumnInfo.create(sqlaw_info)

    def build_report(self, facts=None, dimensions=None, criteria=None, row_filters=None, rollup=None):
        return Report(self, facts=facts, dimensions=dimensions, criteria=criteria, row_filters=row_filters, rollup=rollup)

    def report(self, facts=None, dimensions=None, criteria=None, row_filters=None, rollup=None):
        report = self.build_report(facts=facts,
                                   dimensions=dimensions,
                                   criteria=criteria,
                                   row_filters=row_filters,
                                   rollup=rollup)
        result = report.execute()
        return result

class DataSourceQuery(PrintMixin):
    repr_attrs = ['facts', 'dimensions', 'criteria']

    @initializer
    def __init__(self, warehouse, facts, dimensions, criteria, table_set):
        self.field_map = {}
        self.facts = orderedsetify(facts) if facts else []
        self.dimensions = orderedsetify(dimensions) if dimensions else []
        self.select = self.build_select()

    def get_conn(self):
        datasource = self.warehouse.ds_map[self.table_set.ds_name]
        assert datasource.bind, 'Datasource "%s" does not have metadata.bind set' % self.table_set.ds_name
        conn = datasource.bind.connect()
        return conn

    def build_select(self):
        # https://docs.sqlalchemy.org/en/latest/core/selectable.html
        select = sa.select()

        join = self.get_join()
        select = select.select_from(join)

        for dimension in self.dimensions:
            select = select.column(self.get_field_expression(dimension))

        for fact in self.facts:
            select = select.column(self.get_field_expression(fact))

        select = self.add_where(select)
        select = self.add_group_by(select)
        return select

    def column_for_field(self, field, table=None):
        ts = self.table_set
        if table is not None:
            columns = self.warehouse.table_field_map[ts.ds_name][table.fullname][field]
            # TODO: add check for this in warehouse formation? Make it not a list?
            assert len(columns) == 1, 'Multiple columns for same field in single table not supported yet'
            column = columns[0]
        else:
            if ts.join_list and field in ts.join_list.field_map:
                column = ts.join_list.field_map[field]
            elif field in self.warehouse.table_field_map[ts.ds_name][ts.ds_table.fullname]:
                column = self.column_for_field(field, table=ts.ds_table)
            else:
                assert False, 'Could not determine column for field %s' % field
        self.field_map[field] = column
        return column

    def get_field_expression(self, field):
        ts = self.table_set
        column = self.column_for_field(field)
        field_obj = self.warehouse.get_field(field)
        return field_obj.get_ds_expression(column)

    def get_join(self):
        ts = self.table_set
        sqla_join = None
        last_table = None

        if not ts.join_list:
            return ts.ds_table

        for join in ts.join_list.joins:
            for table_name in join.table_names:
                table = self.warehouse.tables[ts.ds_name][table_name]
                if sqla_join is None:
                    sqla_join = table
                    last_table = table
                    continue

                if table == last_table:
                    continue

                conditions = []
                for field in join.join_fields:
                    last_column = self.column_for_field(field, table=last_table)
                    column = self.column_for_field(field, table=table)
                    conditions.append(column==last_column)
                sqla_join = sqla_join.outerjoin(table, *tuple(conditions))
                last_table = table

        return sqla_join

    def add_where(self, select):
        if not self.criteria:
            return select
        for row in self.criteria:
            field = row[0]
            column = self.column_for_field(field)
            clause = sa.and_(get_sqla_clause(column, row))
            select = select.where(clause)
        return select

    def add_group_by(self, select):
        if not self.dimensions:
            return select
        return select.group_by(*[sa.text(x) for x in self.dimensions])

    def add_order_by(self, select, asc=True):
        if not self.dimensions:
            return select
        order_func = sa.asc
        if not asc:
            order_func = sa.desc
        return select.order_by(*[order_func(sa.text(x)) for x in self.dimensions])

    def covers_fact(self, fact):
        if fact in self.table_set.get_covered_facts(self.warehouse):
            return True
        return False

    def covers_field(self, field):
        if field in self.table_set.get_covered_fields(self.warehouse):
            return True
        return False

    def add_fact(self, fact):
        assert self.covers_fact(fact), 'Fact %s can not be covered by query' % fact
        # TODO: improve the way we maintain targeted facts/dims
        self.table_set.target_fields.add(fact)
        self.facts.add(fact)
        self.select = self.select.column(self.get_field_expression(fact))

class DataSourceQueryResult(PrintMixin):
    repr_attrs = ['rowcount']

    @initializer
    def __init__(self, query, data):
        self.rowcount = len(data)

class BaseCombinedResult:
    @initializer
    def __init__(self, warehouse, ds_query_results, primary_ds_dimensions):
        self.conn = self.get_conn()
        self.cursor = self.get_cursor(self.conn)
        self.table_name = 'sqlaw_%s_%s' % (str(time.time()).replace('.','_'), random.randint(0,1E6))
        self.primary_ds_dimensions = orderedsetify(primary_ds_dimensions) if primary_ds_dimensions else []
        self.ds_dimensions, self.ds_facts = self.get_fields()
        self.create_table()
        self.load_table()

    def get_conn(self):
        raise NotImplementedError

    def get_cursor(self, conn):
        raise NotImplementedError

    def create_table(self):
        raise NotImplementedError

    def load_table(self):
        raise NotImplementedError

    def clean_up(self):
        raise NotImplementedError

    def get_final_result(self, facts, dimensions, row_filters, rollup):
        raise NotImplementedError

    def get_row_hash(self, row):
        # TODO: should we limit length of particular primary dims if they are long strings?
        hash_bytes = str([row[k] for k in row.keys() if k in self.primary_ds_dimensions]).encode('utf8')
        return hash(hash_bytes)

    def get_fields(self):
        dimensions = OrderedDict()
        facts = OrderedDict()

        for qr in self.ds_query_results:
            for dim_name in qr.query.dimensions:
                if dim_name in dimensions:
                    continue
                dim = self.warehouse.get_dimension(dim_name)
                dimensions[dim_name] = dim

            for fact_name in qr.query.facts:
                if fact_name in facts:
                    continue
                fact = self.warehouse.get_fact(fact_name)
                facts[fact_name] = fact

        return dimensions, facts

class SQLiteMemoryCombinedResult(BaseCombinedResult):
    def get_conn(self):
        return connect(":memory:")

    def get_cursor(self, conn):
        conn.row_factory = Row
        return conn.cursor()

    def create_table(self):
        create_sql = 'CREATE TEMP TABLE %s (' % self.table_name
        column_clauses = ['hash BIGINT NOT NULL PRIMARY KEY']

        for field_name, field in self.ds_dimensions.items():
            type_str = str(to_sqlite_type(field.type))
            clause = '%s %s NOT NULL' % (field_name, type_str)
            column_clauses.append(clause)

        for field_name, field in self.ds_facts.items():
            type_str = str(to_sqlite_type(field.type))
            clause = '%s %s DEFAULT NULL' % (field_name, type_str)
            column_clauses.append(clause)

        create_sql += ', '.join(column_clauses)
        create_sql += ') WITHOUT ROWID'
        dbg(create_sql) # Creates don't pretty print well with dbgsql?
        self.cursor.execute(create_sql)
        if self.primary_ds_dimensions:
            index_sql = 'CREATE INDEX idx_dims ON %s (%s)' % (self.table_name, ', '.join(self.primary_ds_dimensions))
            dbgsql(index_sql)
            self.cursor.execute(index_sql)
        self.conn.commit()

    def get_row_insert_sql(self, row):
        # TODO: switch to bulk insert
        values_clause = ', '.join(['?'] * (1 + len(row)))
        columns = [k for k in row.keys()]
        columns_clause = 'hash, ' + ', '.join(columns)

        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.table_name, columns_clause, values_clause)

        update_clauses = []
        for k in columns:
            if k in self.primary_ds_dimensions:
                continue
            update_clauses.append('%s=excluded.%s' % (k, k))
        if update_clauses:
            update_clause = ' ON CONFLICT(hash) DO UPDATE SET ' + ', '.join(update_clauses)
            sql = sql + update_clause

        hash_value = self.get_row_hash(row)
        values = [hash_value]
        for value in row.values():
            # XXX Hack: sqlite cant handle Decimal values.
            if isinstance(value, decimal.Decimal):
                value = float(value)
            values.append(value)
        return sql, values

    def load_table(self):
        for qr in self.ds_query_results:
            for row in qr.data:
                insert_sql, values = self.get_row_insert_sql(row)
                self.cursor.execute(insert_sql, values)
            self.conn.commit()

    def select_all(self):
        qr = self.cursor.execute('SELECT * FROM %s' % self.table_name)
        return [OrderedDict(row) for row in qr.fetchall()]

    def get_final_select_sql(self, columns, dimension_aliases):
        columns_clause = ', '.join(columns)
        order_clause = '1'
        if dimension_aliases:
            order_clause = ', '.join(['%s ASC' % d for d in dimension_aliases])
        sql = 'SELECT %s FROM %s GROUP BY hash ORDER BY %s' % (columns_clause, self.table_name, order_clause)
        dbgsql(sql)
        return sql

    def apply_row_filters_to_df(self, df, row_filters, facts, dimensions):
        filter_parts = []
        for row_filter in row_filters:
            field, op, value = row_filter
            assert (field in facts) or (field in dimensions),\
                'Row filter field "%s" is not in result table' % field
            assert op in ROW_FILTER_OPS, 'Invalid row filter operation: %s' % op
            filter_parts.append('(%s %s %s)' % (field, op, value))
        return df.query(' and '.join(filter_parts))

    def get_multi_rollup_df(self, df, rollup, dimensions, aggrs, wavg, wavgs):
        # TODO: signature of this is a bit odd
        # TODO: test weighted averages
        # https://stackoverflow.com/questions/36489576/why-does-concatenation-of-dataframes-get-exponentially-slower
        level_aggrs = [df]

        for level in range(rollup):
            if (level+1) == len(dimensions):
                # Unnecessary to rollup at the most granular level
                break

            grouped = df.groupby(level=list(range(0,level+1)))
            level_aggr = grouped.agg(aggrs)
            for fact_name, weighting_fact in wavgs:
                level_aggr[fact_name] = wavg(fact_name, weighting_fact)

            # for the remaining levels, set index cols to ROLLUP_INDEX_LABEL
            if level != (len(dimensions) - 1):
                new_index_dims = []
                for dim in dimensions[level+1:]:
                    level_aggr[dim] = ROLLUP_INDEX_LABEL
                    new_index_dims.append(dim)
                level_aggr = level_aggr.set_index(new_index_dims, append=True)

            level_aggrs.append(level_aggr)

        df = pd.concat(level_aggrs, sort=False, copy=False)
        df.sort_index(inplace=True)
        return df

    def get_multi_rollup_df_alternate(self, df, rollup, dimensions, aggrs, wavg, wavgs):
        # Keeping this function around until I have time to test both further and see which
        # scales better.
        # TODO: signature of this is a bit odd
        # TODO: test weighted averages

        level_aggrs = []
        for level in range(rollup):
            grouped = df.groupby(level=list(range(0,level+1)))
            level_aggr = grouped.agg(aggrs)
            for fact_name, weighting_fact in wavgs:
                level_aggr[fact_name] = wavg(fact_name, weighting_fact)
            level_aggrs.append(level_aggr)

        last_index = None
        rows = []

        def to_dict(tuple_row):
            row = dict()
            for i, name in enumerate(df.index.names):
                row[name] = tuple_row.Index[i]
            for col in df.columns:
                row[col] = getattr(tuple_row, col)
            return row

        def get_rollup_row(last_index, level):
            rollup_rows = []
            rollup_row_key = last_index[:level+1]

            if len(rollup_row_key) > 1:
                # Hack necessary to get a dataframe back
                rollup_df = level_aggrs[level].loc[[rollup_row_key], :]
            else:
                rollup_df = level_aggrs[level].loc[rollup_row_key, :]
            assert isinstance(rollup_df, pd.DataFrame), 'Rollup row type expected DataFrame, got %s' % type(rollup_df)
            new_index_dims = []
            for dim in dimensions[level+1:]:
                rollup_df[dim] = ROLLUP_INDEX_LABEL
                new_index_dims.append(dim)
            rollup_df = rollup_df.set_index(new_index_dims, append=True)
            for rollup_row in rollup_df.itertuples():
                rollup_rows.append(to_dict(rollup_row))
            assert len(rollup_rows) == 1, 'Unexpected number of rollup rows:\n%s' % rollup_rows
            return rollup_rows[0]

        # Note: to_records is faster but not a generator
        for row in df.itertuples():
            index = row.Index
            row = to_dict(row)

            if not last_index:
                last_index = index
                rows.append(row)
                continue

            rollup_rows = []
            for level in range(rollup):
                if index[level] != last_index[level]:
                    rollup_row = get_rollup_row(last_index, level)
                    rollup_rows.append(rollup_row)

            for rollup_row in reversed(rollup_rows):
                rows.append(rollup_row)

            last_index = index
            rows.append(row)

        # Add in rollup for last row
        for level in range(rollup):
            rows.append(get_rollup_row(last_index, level))

        df = pd.DataFrame.from_records(rows, index=df.index.names)
        return df

    def apply_rollup_to_df(self, df, rollup, facts, dimensions):
        aggrs = {}
        wavgs = []

        assert dimensions, 'Can not rollup without dimensions'

        def wavg(avg_name, weight_name):
            d = df[avg_name]
            w = df[weight_name]
            try:
                return (d * w).sum() / w.sum()
            except ZeroDivisionError:
                return d.mean() # Return mean if there are no weights

        for fact_name in facts:
            fact = self.warehouse.get_fact(fact_name)
            if fact.weighting_fact:
                wavgs.append((fact.name, fact.weighting_fact))
                continue
            else:
                aggr_func = PANDAS_ROLLUP_AGGR_TRANSLATION.get(fact.aggregation, fact.aggregation)
            aggrs[fact.name] = aggr_func

        aggr = df.agg(aggrs)
        for fact_name, weighting_fact in wavgs:
            aggr[fact_name] = wavg(fact_name, weighting_fact)

        apply_totals = True
        if rollup != ROLLUP_TOTALS:
            df = self.get_multi_rollup_df(df, rollup, dimensions, aggrs, wavg, wavgs)
            if rollup != len(dimensions):
                apply_totals = False

        if apply_totals:
            totals_rollup_index = (ROLLUP_INDEX_LABEL,) * len(dimensions) if len(dimensions) > 1 else ROLLUP_INDEX_LABEL
            with pd.option_context('mode.chained_assignment', None):
                df.at[totals_rollup_index, :] = aggr

        return df

    def apply_technicals(self, df, technicals, rounding):
        for fact, tech in technicals.items():
            rolling = df[fact].rolling(tech.window, min_periods=tech.min_periods, center=tech.center)
            if tech.type == TechnicalTypes.MA:
                df[fact] = rolling.mean()
            elif tech.type == TechnicalTypes.SUM:
                df[fact] = rolling.sum()
            elif tech.type == TechnicalTypes.BOLL:
                ma = rolling.mean()
                std = rolling.std()
                upper = fact + '_upper'
                lower = fact + '_lower'
                if fact in rounding:
                    # This adds some extra columns for the bounds, so we use the same rounding
                    # as the root fact if applicable.
                    df[upper] = round(ma + 2*std, rounding[fact])
                    df[lower] = round(ma - 2*std, rounding[fact])
                else:
                    df[upper] = ma + 2*std
                    df[lower] = ma - 2*std
            else:
                assert False, 'Invalid technical type: %s' % tech.type
        return df

    def get_final_result(self, facts, dimensions, row_filters, rollup):
        columns = []
        dimension_aliases = []

        for dim_name in dimensions:
            dim_def = self.warehouse.get_dimension(dim_name)
            columns.append('%s as %s' % (dim_def.get_final_select_clause(self.warehouse), dim_def.name))
            dimension_aliases.append(dim_def.name)

        technicals = {}
        rounding = {}
        for fact_name in facts:
            fact_def = self.warehouse.get_fact(fact_name)
            if fact_def.technical:
                technicals[fact_def.name] = fact_def.technical
            if fact_def.rounding is not None:
                rounding[fact_def.name] = fact_def.rounding
            columns.append('%s as %s' % (fact_def.get_final_select_clause(self.warehouse), fact_def.name))

        sql = self.get_final_select_sql(columns, dimension_aliases)

        df = pd.read_sql(sql, self.conn, index_col=dimension_aliases or None)

        if row_filters:
            df = self.apply_row_filters_to_df(df, row_filters, facts, dimensions)

        if technicals:
            df = self.apply_technicals(df, technicals, rounding)

        if rollup:
            df = self.apply_rollup_to_df(df, rollup, facts, dimensions)

        if rounding:
            df = df.round(rounding)

        return df

    def clean_up(self):
        drop_sql = 'DROP TABLE IF EXISTS %s ' % self.table_name
        self.cursor.execute(drop_sql)
        self.conn.commit()
        self.conn.close()

class Report:
    @initializer
    def __init__(self, warehouse, facts=None, dimensions=None, criteria=None, row_filters=None, rollup=None):
        self.facts = self.facts or []
        self.dimensions = self.dimensions or []
        assert self.facts or self.dimensions, 'One of facts or dimensions must be specified for Report'
        self.criteria = self.criteria or []
        self.row_filters = self.row_filters or []
        if rollup is not None:
            assert dimensions, 'Must specify dimensions in order to use rollup'
            if rollup != ROLLUP_TOTALS:
                assert is_int(rollup) and (0 < int(rollup) <= len(dimensions)), 'Invalid rollup value: %s' % rollup
                self.rollup = int(rollup)

        self.ds_facts = OrderedSet()
        self.ds_dimensions = OrderedSet()

        for fact_name in self.facts:
            self.add_ds_fields(fact_name, FieldTypes.FACT)

        for dim_name in self.dimensions:
            self.add_ds_fields(dim_name, FieldTypes.DIMENSION)

        self.queries = self.build_ds_queries()
        self.combined_query = None

    def add_ds_fields(self, field_name, field_type):
        if field_type == FieldTypes.FACT:
            field = self.warehouse.get_fact(field_name)
        elif field_type == FieldTypes.DIMENSION:
            field = self.warehouse.get_dimension(field_name)
        else:
            assert False, 'Invalid field_type: %s' % field_type

        formula_fields, _ = field.get_formula_fields(self.warehouse) or ([field_name], None)

        if field_type == FieldTypes.FACT and field.weighting_fact:
            assert field.weighting_fact in self.warehouse.facts, \
                'Could not find weighting fact %s in warehouse' % field.weighting_fact
            self.ds_facts.add(field.weighting_fact)

        for formula_field in formula_fields:
            if formula_field in self.warehouse.facts:
                self.ds_facts.add(formula_field)
            elif formula_field in self.warehouse.dimensions:
                self.ds_dimensions.add(formula_field)
            else:
                assert False, 'Could not find field %s in warehouse' % formula_field

    def execute_ds_query(self, query):
        # TODOs:
        # Add straight joins? Optimize indexes?
        # Explain query?
        # MySQL: SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
        #  finally: SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ

        start = time.time()
        conn = query.get_conn()
        try:
            result = conn.execute(query.select)
            data = result.fetchall()
        except:
            error('Exception during query:')
            dbgsql(query.select)
            raise
        finally:
            conn.close()
        end = time.time()
        dbg('Got %d rows in %.3fs' % (len(data), end - start))
        return DataSourceQueryResult(query, data)

    def execute_ds_queries(self, queries):
        # TODO: execute datasource queries in parallel threads
        results = []
        for query in queries:
            result = self.execute_ds_query(query)
            results.append(result)
        return results

    def execute(self):
        ds_query_results = self.execute_ds_queries(self.queries)
        cr = self.create_combined_result(ds_query_results)
        try:
            final_result = cr.get_final_result(self.facts, self.dimensions, self.row_filters, self.rollup)
            dbg(final_result)
            self.result = ReportResult(final_result)
            return self.result
        finally:
            cr.clean_up()

    def get_grain(self):
        # TODO: may need to adjust this to support partition dimensions
        if not (self.ds_dimensions or self.criteria):
            return None
        grain = set()
        if self.ds_dimensions:
            grain = grain | set(self.ds_dimensions)
        if self.criteria:
            grain = grain | set([x[0] for x in self.criteria])
        return grain

    def build_ds_queries(self):
        grain = self.get_grain()
        queries = []

        def fact_covered_in_queries(fact):
            for query in queries:
                if query.covers_fact(fact):
                    query.add_fact(fact)
                    return query
            return False

        for fact in self.ds_facts:
            existing_query = fact_covered_in_queries(fact)
            if existing_query:
                # TODO: we could do a single consolidation at the end instead
                # and that might get more optimal results
                dbg('Fact %s is covered by existing query' % fact)
                continue

            table_set = self.warehouse.get_fact_table_set(fact, grain)
            query = DataSourceQuery(self.warehouse, [fact], self.ds_dimensions, self.criteria, table_set)
            queries.append(query)

        if not self.ds_facts:
            dbg('No facts requested, getting dimension table sets')
            table_set = self.warehouse.get_dimension_table_set(grain)
            query = DataSourceQuery(self.warehouse, None, self.ds_dimensions, self.criteria, table_set)
            queries.append(query)

        for query in queries:
            dbgsql(sqla_compile(query.select))

        return queries

    def create_combined_result(self, ds_query_results):
        return SQLiteMemoryCombinedResult(self.warehouse, ds_query_results, self.ds_dimensions)

class ReportResult:
    @initializer
    def __init__(self, df):
        return df

    def get_rollup_mask(self):
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Index.isin.html
        mask = None
        for i, level in enumerate(self.df.index.levels):
            if mask is None:
                mask = self.df.index.isin([ROLLUP_INDEX_LABEL], i)
            else:
                mask = (mask | self.df.index.isin([ROLLUP_INDEX_LABEL], i))
        return mask

    def rollup_rows(self):
        return self.df.loc[self.get_rollup_mask()]

    def non_rollup_rows(self):
        return self.df.loc[~self.get_rollup_mask()]
