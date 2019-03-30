from collections import defaultdict
import random

import networkx as nx
import sqlalchemy as sa

from sqlaw.configs import ColumnInfoSchema, TableInfoSchema
from sqlaw.core import NUMERIC_SA_TYPES
from sqlaw.utils import (dbg,
                         warn,
                         st,
                         initializer,
                         PrintMixin,
                         MappingMixin)

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
    repr_attrs = ['fieldname', 'type', 'active']
    schema = ColumnInfoSchema

class TableSet(PrintMixin):
    repr_attrs = ['ds_name', 'join', 'grain', 'target_facts']

    @initializer
    def __init__(self, ds_name, fact_table, join, grain, target_facts):
        self.covered_facts = get_table_facts(fact_table)

class NeighborTable(PrintMixin):
    repr_attrs = ['table_name', 'join_fields']

    @initializer
    def __init__(self, table, join_fields):
        self.table_name = table.fullname

class JoinInfo(PrintMixin):
    repr_attrs = ['table_names']

    @initializer
    def __init__(self, table_names, join_fields):
        pass

class JoinList(PrintMixin):
    repr_attrs = ['table_names']

    @initializer
    def __init__(self, joins):
        self.table_names = set()
        for join in self.joins:
            for table_name in join.table_names:
                self.table_names.add(table_name)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return self.table_names == other.table_names

def is_probably_fact(column):
    if type(column.type) not in NUMERIC_SA_TYPES:
        return False
    if column.primary_key:
        return False
    if column.name.endswith('_id') or column.name.endswith('Id') or column.name == 'id':
        return False
    return True

def joins_from_path(graph, path):
    joins = []
    for i, node in enumerate(path):
        if i == (len(path) - 1):
            break
        start, end = path[i], path[i+1]
        edge = graph.edges[start, end]
        join_info = JoinInfo([start, end], edge['join_fields'])
        joins.append(join_info)
    return JoinList(joins)

def get_primary_key_fields(primary_key):
    return set([x.info['sqlaw']['fieldname'] for x in primary_key])

def get_table_fields(table):
    return set([x.info['sqlaw']['fieldname'] for x in table.c])

def get_table_facts(table):
    return set([x.info['sqlaw']['fieldname'] for x in table.c if x.info['sqlaw'].type == 'fact'])

def get_table_dimensions(table):
    return set([x.info['sqlaw']['fieldname'] for x in table.c if x.info['sqlaw'].type == 'dimension'])

def column_fullname(column):
    return '%s.%s' % (column.table.fullname, column.name)

class Warehouse:
    @initializer
    def __init__(self, ds_map, ds_priority=None):
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
        self.ds_graphs = {}
        for ds_name, metadata in ds_map.items():
            self.ensure_metadata_info(metadata)
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
                continue
            table.info['sqlaw'] = TableInfo.create(sqlaw_info)
            for column in table.c:
                sqlaw_info = column.info.get('sqlaw', None)
                if not sqlaw_info:
                    if not table.info['sqlaw'].autocolumns:
                        continue
                    else:
                        sqlaw_info = {}
                sqlaw_info['fieldname'] = sqlaw_info.get('fieldname', column_fullname(column))
                column.info['sqlaw'] = ColumnInfo.create(sqlaw_info)

    def find_neighbor_tables(self, ds_name, table):
        neighbor_tables = []
        pk_fields = get_primary_key_fields(table.primary_key)
        fields = get_table_fields(table)

        if table.info['sqlaw'].type == 'fact':
            # Find dimension tables whose primary key is contained in the fact table
            dim_tables = self.dimension_tables.get(ds_name, [])
            for dim_table in self.dimension_tables[ds_name].values():
                dt_pk_fields = get_primary_key_fields(dim_table.primary_key)
                can_join = True
                for field in dt_pk_fields:
                    if field not in fields:
                        can_join = False
                        break
                if can_join:
                    neighbor_tables.append(NeighborTable(dim_table, dt_pk_fields))

        # Add parent table if present
        parent_name = table.info['sqlaw'].parent
        if parent_name:
            parent = self.tables[ds_name][parent_name]
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
        if not ('sqlaw' in table.info):
            return
        self.tables[ds_name][table.fullname] = table
        table_info = table.info['sqlaw']
        if table_info.type == 'fact':
            self.add_fact_table(ds_name, table)
        elif table_info.type == 'dimension':
            self.add_dimension_table(ds_name, table)
        else:
            assert False, 'Invalid table type: %s' % table_info.type

    def add_fact_table(self, ds_name, table):
        self.fact_tables[ds_name][table.fullname] = table
        for column in table.c:
            if not ('sqlaw' in column.info):
                continue
            column_info = column.info['sqlaw']
            if column_info.type == 'fact' or (column_info.type == 'auto' and is_probably_fact(column)):
                column_info.type = 'fact'
                self.add_fact(ds_name, column)
            elif column_info.type == 'dimension' or column_info.type == 'auto':
                column_info.type = 'dimension'
                self.add_dimension(ds_name, column)

    def get_ds_tables_with_fact(self, fact):
        ds_tables = defaultdict(list)
        ds_fact_columns = self.facts[fact]
        for ds_name, columns in ds_fact_columns.items():
            for column in columns:
                ds_tables[ds_name].append(column.table)
        dbg('found %d datasource tables with fact' % len(ds_tables))
        return ds_tables

    def add_dimension_table(self, ds_name, table):
        self.dimension_tables[ds_name][table.fullname] = table
        for column in table.c:
            if not ('sqlaw' in column.info):
                continue
            self.add_dimension(ds_name, column)

    def add_fact(self, ds_name, column):
        fieldname = column.info['sqlaw']['fieldname']
        self.facts[fieldname].setdefault(ds_name, []).append(column)

    def add_dimension(self, ds_name, column):
        fieldname = column.info['sqlaw']['fieldname']
        self.dimensions[fieldname].setdefault(ds_name, []).append(column)

    def get_tables_with_dimension(self, ds_name, dimension):
        return [x.table for x in self.dimensions[dimension][ds_name]]

    def find_joins_to_dimension(self, ds_name, fact_table, dimension):
        ds_graph = self.ds_graphs[ds_name]
        joins = []

        for table in self.get_tables_with_dimension(ds_name, dimension):
            paths = nx.all_simple_paths(ds_graph, fact_table.fullname, table.fullname)
            if not paths:
                continue
            paths = [x for x in paths]
            for path in paths:
                join_list = joins_from_path(ds_graph, path)
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
            dbg('adding %d possible joins to table %s' % (len(joins), fact_table.fullname))
            for join, covered_dims in joins.items():
                table_set = TableSet(ds_name, fact_table, join, grain, set([fact]))
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
        for join in join_fields.keys():
            for other_join in join_fields.keys():
                if join == other_join or other_join in joins_to_delete:
                    continue
                if join.table_names.issubset(other_join.table_names):
                    joins_to_delete.append(join)
                    fields = join_fields[join]
                    dbg('Fields %s satisfied by other join %s' % (fields, other_join.table_names))
                    join_fields[other_join] = join_fields[other_join].union(fields)

        for join in joins_to_delete:
            del join_fields[join]

        return join_fields

    @classmethod
    def apply_config(cls, config, ds_map):
        '''
        This will update or add sqlaw info to the schema item info dict if it
        appears in the datasource config
        '''
        for ds_name, metadata in ds_map.items():
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
                    sqlaw_info['fieldname'] = sqlaw_info.get('fieldname', column_fullname(column))
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
    def __init__(self, table_set):
        st()
        # XXX build the components of the sqla select
        pass

    def covers_fact(self, fact):
        if fact in self.table_set.covered_facts:
            return True
        return False

    def add_fact(self, fact):
        assert self.covers_fact(fact), 'Fact %s can not be covered by query' % fact
        self.table_set.target_facts.add(fact)
        # XXX and then add it to the query itself somehow

class CombinedQuery(Query):
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
        for fact in self.facts:
            skip = False
            for query in queries:
                if query.covers_fact(fact):
                    query.add_fact(fact)
                    skip = True
                    break
            if skip:
                # TODO: we could do a single consolidation at the end instead
                # and that might get more optimal results
                dbg('Fact %s is covered by existing query' % fact)
                continue

            table_set = self.warehouse.get_fact_table_set(fact, grain)
            query = Query(table_set)
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
