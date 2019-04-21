import ast
from collections import defaultdict
import random

import networkx as nx
from orderedset import OrderedSet
import sqlalchemy as sa

from sqlaw.configs import ColumnInfoSchema, TableInfoSchema
from sqlaw.core import NUMERIC_SA_TYPES, INTEGER_SA_TYPES, FLOAT_SA_TYPES
from sqlaw.utils import (dbg,
                         warn,
                         st,
                         initializer,
                         PrintMixin,
                         MappingMixin)

DIGIT_THRESHOLD_FOR_AVG_AGGR = 1

# Aggregation Types
# TODO: make class for this, put it in core, add support for "count"?
SUM = 'sum'
AVG = 'avg'

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

class TableSet(PrintMixin):
    repr_attrs = ['ds_name', 'join_list', 'grain', 'target_facts']

    @initializer
    def __init__(self, ds_name, fact_table, join_list, grain, target_facts):
        pass

    def get_covered_facts(self, warehouse):
        covered_facts = get_table_facts(warehouse, fact_table)
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

def get_aggregation_and_rounding(column):
    if type(column.type) in INTEGER_SA_TYPES:
        return SUM, 0
    if type(column.type) in FLOAT_SA_TYPES:
        rounding = column.type.scale
        precision = column.type.precision
        whole_digits = precision - rounding
        if whole_digits <= DIGIT_THRESHOLD_FOR_AVG_AGGR:
            aggregation = AVG
        else:
            aggregation = SUM
        return aggregation, rounding
    assert False, 'Column %s is not a numeric type' % column

def is_probably_fact(column):
    if type(column.type) not in NUMERIC_SA_TYPES:
        return False
    if column.primary_key:
        return False
    if column.name.endswith('_id') or column.name.endswith('Id') or column.name == 'id':
        return False
    return True

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

def get_sqla_condition(criteria_row):
    st()
    pass

def add_where(select, criteria):
    # TODO: should this be module-level method?
    sqla_conditions = []
    for row in criteria:
        sqla_conditions.append(get_sqla_condition(row))
    for condition in sqla_conditions:
        select = select.where(condition)
    return select

def add_group_by(select, dimensions):
    for i, dim in enumerate(dimensions):
        select = select.group_by(sa.text(str(i)))
    return select

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

def column_fullname(column):
    return '%s.%s' % (column.table.fullname, column.name)

class Field(PrintMixin):
    repr_attrs = ['name']
    ifnull_value = '--'

    @initializer
    def __init__(self, name, type, **kwargs):
        self.column_map = defaultdict(list)
        if isinstance(type, str):
            self.type = Field.type_string_to_sa_type(type)

    def add_column(self, ds_name, column):
        current_cols = [column_fullname(col) for col in self.column_map[ds_name]]
        fullname = column_fullname(column)
        if fullname in current_cols:
            warn('Column %s.%s is already mapped to field %s' % (ds_name, fullname, self.name))
            return
        self.column_map[ds_name].append(column)

    def get_columns(self, ds_name):
        return self.column_map[ds_name]

    def get_expression(self, column):
        # XXX: should we do ifnull here?
        return sa.func.ifnull(column_fullname(column), self.ifnull_value).label(self.name)

    @classmethod
    def type_string_to_sa_type(cls, type_string):
        parts = type_string.split('(')
        type_args = []
        if len(parts) > 1:
            assert len(parts) == 2, 'Unable to parse type string: %s' % type_string
            type_args = ast.literal_eval(parts[1].rstrip(')') + ',')
        type_name = parts[0]
        type_cls = getattr(sa, type_name, None)
        assert type_cls, 'Could not find matching type for %s' % type_name
        return type_cls(*type_args)

    # https://stackoverflow.com/questions/2909106/whats-a-correct-and-good-way-to-implement-hash
    def __key(self):
        return self.name

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.__key() == other.__key()


class Fact(Field):
    def __init__(self, name, type, aggregation=SUM, rounding=None):
        super(Fact, self).__init__(name, type, aggregation=aggregation, rounding=rounding)

    def get_expression(self, column):
        # TODO: support weighted averages
        rounding = self.rounding or 0
        aggr = sa.func.sum
        if self.aggregation == 'avg':
            aggr = sa.func.avg
        # XXX: should we do ifnull here?
        return aggr(sa.func.round(sa.func.ifnull(column_fullname(column), self.ifnull_value), rounding)).label(self.name)

class CombinedFact(PrintMixin):
    repr_atts = ['name', 'formula']

    @initializer
    def __init__(self, name, formula, aggregation=SUM, rounding=None):
        pass

class Dimension(Field):
    pass

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
                sqlaw_info['fields'] = sqlaw_info.get('fields', [column_fullname(column)])

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

        if table.sqlaw.type == 'fact':
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
        if table.sqlaw.type == 'fact':
            self.add_fact_table(ds_name, table)
        elif table.sqlaw.type == 'dimension':
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
            aggregation, rounding = get_aggregation_and_rounding(column)
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
                fact = Fact(fact_def['name'], fact_def['type'],
                            aggregation=fact_def['aggregation'],
                            rounding=fact_def['rounding'])
            else:
                assert isinstance(fact_def, Fact),\
                    'Fact definition must be a dict-like object or a Fact object'
                fact = fact_def
            self.add_fact(fact)

        for dim_def in config.get('dimensions', []):
            if isinstance(dim_def, dict):
                dim = Dimension(dim_def['name'], dim_def['type'])
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

class Query:
    @initializer
    def __init__(self, warehouse, table_set, facts, dimensions, criteria):
        self.select = self.build_select()

    def build_select(self):
        # https://docs.sqlalchemy.org/en/latest/core/selectable.html
        select = sa.select()

        for dimension in self.dimensions:
            select.column(self.get_field_expression(dimension))

        for fact in self.facts:
            select.column(self.get_field_expression(fact))

        join = self.get_join(select)
        select = add_where(select, self.criteria)
        select = add_group_by(select, self.dimensions)
        select = select.select_from(join)
        return select

    def column_from_field(self, table, field):
        ts = self.table_set
        columns = self.warehouse.table_field_map[ts.ds_name][table.fullname][field]
        # TODO: add check for this earlier?
        assert len(columns) == 1, 'Multiple columns for same field in single table not supported yet'
        column = columns[0]
        return column

    def get_field_expression(self, field):
        ts = self.table_set
        if field in ts.join_list.field_map:
            column = ts.join_list.field_map[field]
        elif field in self.warehouse.table_field_map[ts.ds_name][ts.fact_table.fullname]:
            column = self.column_from_field(ts.fact_table, field)
        else:
            assert False, 'Could not determine column for field %s' % field

        field_obj = self.warehouse.get_field(field)
        return field_obj.get_expression(column)

    def get_join(self, select):
        ts = self.table_set
        sqla_join = None
        last_table = None

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
                    last_column = self.column_from_field(last_table, field)
                    column = self.column_from_field(table, field)
                    conditions.append(column==last_column)
                sqla_join = sqla_join.outerjoin(table, *tuple(conditions))
                last_table = table

        return sqla_join

    def covers_fact(self, fact):
        if fact in self.table_set.get_covered_facts(warehouse):
            return True
        return False

    def add_fact(self, fact):
        assert self.covers_fact(fact), 'Fact %s can not be covered by query' % fact
        st()
        self.table_set.target_facts.add(fact)
        #self.select = self.select(self.get_select_column(fact))

class CombinedQuery:
    pass

class Report:
    @initializer
    def __init__(self, warehouse, facts, dimensions=None, criteria=None, row_filters=None, rollup=None):
        self.queries = self.build_queries()
        self.combined_query = None

    def execute_query(self, query):
        return []

    def execute_queries(self, queries):
        # TODO: execute queries in parallel threads
        results = []
        for query in queries:
            results.append(self.execute_query(query))
        return results

    def execute(self):
        query_results = self.execute_queries(self.queries)
        query_result_table = self.create_query_result_table(query_results)
        self.combined_query = self.build_combined_query(query_result_table)
        combined_results = self.execute_query(self.combined_query)
        # TODO: do we need to apply row filters in additional step?
        self.result = ReportResult(combined_results)
        return self.result

    def get_grain(self):
        # TODO: may need to adjust this to support partition dimensions
        if not (self.dimensions or self.criteria):
            return None
        grain = set(self.dimensions)
        if self.criteria:
            grain = grain | set([x[0] for x in self.criteria])
        return grain

    def build_queries(self):
        grain = self.get_grain()
        queries = []

        def fact_covered_in_queries(fact):
            for query in queries:
                if query.covers_fact(fact):
                    query.add_fact(fact)
                    return True
            return False

        for fact in self.facts:
            if fact_covered_in_queries(fact):
                # TODO: we could do a single consolidation at the end instead
                # and that might get more optimal results
                dbg('Fact %s is covered by existing query' % fact)
                continue

            table_set = self.warehouse.get_fact_table_set(fact, grain)
            query = Query(self.warehouse, table_set, [fact], self.dimensions, self.criteria)
            queries.append(query)
        return queries

    def create_query_table(self, query_results):
        # TODO: create a reference to a temporary result table
        pass

    def build_combined_query(self, query_table):
        pass

class ReportResult:
    @initializer
    def __init__(self, rows):
        return rows
