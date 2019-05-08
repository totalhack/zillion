from collections import defaultdict, OrderedDict
import random
from sqlite3 import connect, Row
import time

import pandas as pd
import networkx as nx
from orderedset import OrderedSet
import sqlalchemy as sa

from sqlaw.configs import (ColumnInfoSchema,
                           TableInfoSchema,
                           FactConfigSchema,
                           DimensionConfigSchema)
from sqlaw.core import (NUMERIC_SA_TYPES,
                        INTEGER_SA_TYPES,
                        FLOAT_SA_TYPES,
                        ROW_FILTER_OPS,
                        AggregationTypes,
                        TableTypes)
from sqlaw.sql_utils import (infer_aggregation_and_rounding,
                             aggregation_to_sqla_func,
                             type_string_to_sa_type,
                             is_probably_fact,
                             sqla_compile,
                             column_fullname,
                             get_sqla_clause,
                             to_sqlite_type,
                             sqlite_safe_name)
from sqlaw.utils import (dbg,
                         warn,
                         st,
                         initializer,
                         orderedsetify,
                         get_string_format_args,
                         PrintMixin,
                         MappingMixin)

DEFAULT_IFNULL_VALUE = '--'
ROLLUP_INDEX_LABEL = '::'
MAX_FORMULA_DEPTH = 3

PANDAS_AGGR_TRANSLATION = {
    AggregationTypes.AVG: 'mean'
}

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
    repr_attrs = ['ds_name', 'join_list', 'grain', 'target_facts']

    @initializer
    def __init__(self, ds_name, fact_table, join_list, grain, target_facts):
        pass

    def get_covered_facts(self, warehouse):
        covered_facts = get_table_facts(warehouse, self.fact_table)
        return covered_facts

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

class Field(PrintMixin):
    repr_attrs = ['name']
    ifnull_value = DEFAULT_IFNULL_VALUE

    @initializer
    def __init__(self, name, type, **kwargs):
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
        formula = column.sqlaw.fields[self.name].get('formula', None) if column.sqlaw.fields[self.name] else None
        if not formula:
            return sa.func.ifnull(column, self.ifnull_value).label(self.name)
        return sa.func.ifnull(sa.text(formula), self.ifnull_value).label(self.name)

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
    def __init__(self, name, type, aggregation=AggregationTypes.SUM, rounding=None, **kwargs):
        super(Fact, self).__init__(name, type, aggregation=aggregation, rounding=rounding, **kwargs)

    def get_ds_expression(self, column):
        # TODO: support weighted averages
        formula = column.sqlaw.fields[self.name].get('formula', None) if column.sqlaw.fields[self.name] else None
        use_column = column
        if formula:
            # TODO: detect if rounding or aggregation is already applied?
            use_column = sa.text(formula)

        rounding = self.rounding or 0
        aggr = aggregation_to_sqla_func(self.aggregation)

        if self.aggregation in [AggregationTypes.COUNT, AggregationTypes.COUNT_DISTINCT]:
            if rounding:
                warn('Ignoring rounding for count field: %s' % self.name)
            return aggr(sa.func.ifnull(use_column, self.ifnull_value)).label(self.name)

        return sa.func.round(aggr(sa.func.ifnull(use_column, self.ifnull_value)), rounding).label(self.name)

class FormulaFact(Fact):
    repr_atts = ['name', 'formula']

    def __init__(self, name, formula, aggregation=AggregationTypes.SUM, rounding=None):
        super(FormulaFact, self).__init__(name, None, aggregation=aggregation, rounding=rounding, formula=formula)
        # TODO: ensure the params are valid fields as objects are formed

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
        if self.rounding:
            # XXX: needs a better home? This doesnt know what dialect to compile for.
            # TODO: The 1.0 is a hack specific to sqlite. Also needs a new home.
            format_args = {k:('(1.0*%s)' % k) for k in formula_fields}
            clause = sqla_compile(sa.func.round(sa.text(raw_formula.format(**format_args)), self.rounding))
        else:
            format_args = {k:k for k in formula_fields}
            clause = raw_formula.format(**format_args)

        return clause

def create_fact(fact_def):
    if fact_def['formula']:
        fact = FormulaFact(fact_def['name'], fact_def['formula'],
                           aggregation=fact_def['aggregation'],
                           rounding=fact_def['rounding'])
    else:
        fact = Fact(fact_def['name'], fact_def['type'],
                    aggregation=fact_def['aggregation'],
                    rounding=fact_def['rounding'])
    return fact

class Dimension(Field):
    pass

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
                sqlaw_info['fields'] = sqlaw_info.get('fields', {column_fullname(column): None})

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

    def get_field(self, field):
        if field in self.facts:
            return self.facts[field]
        if field in self.dimensions:
            return self.dimensions[field]
        assert False, 'Field %s does not exist' % field

    def get_fact(self, name):
        assert name in self.facts, 'Invalid fact name: %s' % name
        return self.facts[name]

    def get_dimension(self, name):
        assert name in self.dimensions, 'Invalid dimensions name: %s' % name
        return self.dimensions[name]

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
        dbg('found %d datasource tables with fact' % len(ds_tables))
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

    def find_joins_to_dimension(self, ds_name, fact_table, dimension):
        ds_graph = self.ds_graphs[ds_name]
        joins = []

        for column in self.dimensions[dimension].get_columns(ds_name):
            table = column.table
            paths = nx.all_simple_paths(ds_graph, fact_table.fullname, table.fullname)
            if not paths:
                continue
            for path in paths:
                field_map = {dimension:column}
                join_list = joins_from_path(ds_graph, path, field_map=field_map)
                joins.append(join_list)

        dbg('Found joins to dim %s for table %s:' % (dimension, fact_table.fullname))
        dbg(joins)
        return joins

    def get_possible_joins(self, ds_name, fact_table, grain):
        assert fact_table.fullname in self.fact_tables[ds_name],\
            'Could not find fact table %s in datasource %s' % (fact_table.fullname, ds_name)

        if not grain:
            dbg('No grain specified, ignoring joins')
            return None

        possible_dim_joins = {}
        for dimension in grain:
            dim_joins = self.find_joins_to_dimension(ds_name, fact_table, dimension)
            if not dim_joins:
                dbg('table %s can not satisfy dimension %s' % (fact_table.fullname, dimension))
                return None

            possible_dim_joins[dimension] = dim_joins

        possible_joins = self.consolidate_field_joins(possible_dim_joins)
        dbg('possible joins:')
        dbg(possible_joins)
        return possible_joins

    def find_possible_table_sets(self, ds_name, ds_tables_with_fact, fact, grain):
        table_sets = []
        for fact_table in ds_tables_with_fact:
            if (not grain) or grain.issubset(get_table_fields(fact_table)):
                table_set = TableSet(ds_name, fact_table, None, grain, set([fact]))
                table_sets.append(table_set)
                continue

            joins = self.get_possible_joins(ds_name, fact_table, grain)
            if not joins:
                dbg('table %s can not join at grain %s' % (fact_table.fullname, grain))
                continue
            dbg('adding %d possible join(s) to table %s' % (len(joins), fact_table.fullname))
            for join_list, covered_dims in joins.items():
                table_set = TableSet(ds_name, fact_table, join_list, grain, set([fact]))
                table_sets.append(table_set)
        dbg(table_sets)
        return table_sets

    def get_ds_table_sets(self, ds_fact_tables, fact, grain):
        '''Returns all table sets that can satisfy grain in each datasource'''
        ds_table_sets = {}
        for ds_name, ds_tables_with_fact in ds_fact_tables.items():
            possible_table_sets = self.find_possible_table_sets(ds_name, ds_tables_with_fact, fact, grain)
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
                    sqlaw_info['fields'] = sqlaw_info.get('fields', [column_fullname(column)])
                    column.info['sqlaw'] = ColumnInfo.create(sqlaw_info)

    def build_report(self, facts, dimensions=None, criteria=None, row_filters=None, rollup=None):
        return Report(self, facts, dimensions=dimensions, criteria=criteria, row_filters=row_filters, rollup=rollup)

    def report(self, facts, dimensions=None, criteria=None, row_filters=None, rollup=None):
        report = self.build_report(facts,
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
        self.facts = orderedsetify(facts)
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
            elif field in self.warehouse.table_field_map[ts.ds_name][ts.fact_table.fullname]:
                column = self.column_for_field(field, table=ts.fact_table)
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
            return ts.fact_table

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

    def add_fact(self, fact):
        assert self.covers_fact(fact), 'Fact %s can not be covered by query' % fact
        # TODO: improve the way we maintain targeted facts/dims
        self.table_set.target_facts.add(fact)
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
        # XXX: should we limit length of particular primary dims if they are long strings?
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
        create_sql = 'CREATE TEMP TABLE %s (\n' % self.table_name
        column_clauses = ['hash BIGINT NOT NULL PRIMARY KEY']

        for field_name, field in self.ds_dimensions.items():
            type_str = str(to_sqlite_type(field.type))
            clause = '%s %s NOT NULL' % (sqlite_safe_name(field_name), type_str)
            column_clauses.append(clause)

        for field_name, field in self.ds_facts.items():
            type_str = str(to_sqlite_type(field.type))
            clause = '%s %s DEFAULT NULL' % (sqlite_safe_name(field_name), type_str)
            column_clauses.append(clause)

        create_sql += ',\n'.join(column_clauses)
        create_sql += '\n) WITHOUT ROWID'
        dbg(create_sql)
        self.cursor.execute(create_sql)
        if self.primary_ds_dimensions:
            index_sql = 'CREATE INDEX idx_dims ON %s (%s)' % (self.table_name, ', '.join(self.primary_ds_dimensions))
            dbg(index_sql)
            self.cursor.execute(index_sql)
        self.conn.commit()

    def get_row_insert_sql(self, row):
        # TODO: switch to bulk insert
        values_clause = ', '.join(['?'] * (1 + len(row)))
        # TODO: dont allow unsafe chars in fields names in the first place?
        columns = [sqlite_safe_name(k) for k in row.keys()]
        columns_clause = 'hash, ' + ', '.join(columns)
        update_clauses = []
        for k in columns:
            if k in self.primary_ds_dimensions:
                continue
            update_clauses.append('%s=excluded.%s' % (k, k))
        update_clause = ', '.join(update_clauses)

        sql = ('INSERT INTO %s (%s) VALUES (%s) '
               'ON CONFLICT(hash) DO UPDATE SET %s' %
               (self.table_name, columns_clause, values_clause, update_clause))
        hash_value = self.get_row_hash(row)
        values = [hash_value]
        values.extend(row.values())
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

    def get_final_result(self, facts, dimensions, row_filters, rollup):
        # TODO: support multi-rollup
        # TODO: support weighted average
        # - this would require keeping that column in the result

        fields = dimensions + facts
        columns = []

        for dim_name in dimensions:
            dim_def = self.warehouse.get_dimension(dim_name)
            columns.append('%s as %s' % (dim_def.get_final_select_clause(self.warehouse), dim_def.name))

        for fact_name in facts:
            fact_def = self.warehouse.get_fact(fact_name)
            columns.append('%s as %s' % (fact_def.get_final_select_clause(self.warehouse), fact_def.name))

        columns_clause = ', '.join(columns)
        order_clause = '1'
        if self.primary_ds_dimensions:
            order_clause = ', '.join(['%s ASC' % d for d in self.primary_ds_dimensions])
        sql = 'SELECT %s FROM %s ORDER BY %s' % (columns_clause, self.table_name, order_clause)
        index_col = dimensions or None
        df = pd.read_sql(sql, self.conn, index_col=index_col)

        if row_filters:
            filter_parts = []
            for row_filter in row_filters:
                field, op, value = row_filter
                assert (field in facts) or (field in dimensions),\
                    'Row filter field "%s" is not in result table' % field
                assert op in ROW_FILTER_OPS, 'Invalid row filter operation: %s' % op
                filter_parts.append('(%s %s %s)' % (field, op, value))
            df = df.query(' and '.join(filter_parts))

        if rollup:
            aggrs = {}
            for fact_name in facts:
                fact = self.warehouse.get_fact(fact_name)
                aggr_type = PANDAS_AGGR_TRANSLATION.get(fact.aggregation, fact.aggregation)
                fact_name = sqlite_safe_name(fact_name)
                aggrs[fact_name] = aggr_type

            aggr = df.agg(aggrs)
            rollup_index = (ROLLUP_INDEX_LABEL,) * len(dimensions)
            with pd.option_context('mode.chained_assignment', None):
                df.loc[rollup_index, :] = aggr

        return df

    def clean_up(self):
        drop_sql = 'DROP TABLE IF EXISTS %s ' % self.table_name
        self.cursor.execute(drop_sql)
        self.conn.commit()
        self.conn.close()

class Report:
    @initializer
    def __init__(self, warehouse, facts, dimensions=None, criteria=None, row_filters=None, rollup=None):
        self.dimensions = self.dimensions or []
        self.criteria = self.criteria or []
        self.row_filters = self.row_filters or []
        if rollup:
            assert dimensions, 'Must specify dimensions in order to use rollup'

        self.ds_facts = OrderedSet()
        self.ds_dimensions = OrderedSet()

        for fact_name in self.facts:
            fact = warehouse.get_fact(fact_name)
            formula_fields, _ = fact.get_formula_fields(self.warehouse) or ([fact_name], None)
            for field in formula_fields:
                if field in warehouse.facts:
                    self.ds_facts.add(field)
                elif field in warehouse.dimensions:
                    self.ds_dimensions.add(field)
                else:
                    assert False, 'Could not find field %s in warehouse' % field

        for dim in dimensions:
            self.ds_dimensions.add(dim)

        self.queries = self.build_ds_queries()
        self.combined_query = None

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
                    return True
            return False

        for fact in self.ds_facts:
            if fact_covered_in_queries(fact):
                # TODO: we could do a single consolidation at the end instead
                # and that might get more optimal results
                dbg('Fact %s is covered by existing query' % fact)
                continue

            table_set = self.warehouse.get_fact_table_set(fact, grain)
            query = DataSourceQuery(self.warehouse, [fact], self.ds_dimensions, self.criteria, table_set)
            dbg(sqla_compile(query.select))
            queries.append(query)
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
