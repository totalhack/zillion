from collections import defaultdict, OrderedDict
import copy
import datetime
import os
import random
from urllib.parse import urlparse, urlunparse, parse_qs

import pandas as pd
import networkx as nx
from orderedset import OrderedSet
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from tlbx import (
    PrintMixin,
    dbg,
    pf,
    st,
    format_msg,
    rmfile,
    initializer,
    open_filepath_or_buffer,
    get_string_format_args,
)

from zillion.configs import (
    DATASOURCE_ALLOWABLE_CHARS,
    DATASOURCE_ALLOWABLE_CHARS_STR,
    TableInfo,
    ColumnInfo,
    DataSourceConfigSchema,
    TableConfigSchema,
    MetricConfigSchema,
    DimensionConfigSchema,
    default_field_name,
    is_valid_field_name,
    zillion_config,
    ADHOC_TABLE_CONFIG_PARAMS,
    EXCLUDE,
)
from zillion.core import TableTypes, ADHOC_URL
from zillion.field import (
    Field,
    Metric,
    Dimension,
    FormulaMetric,
    create_metric,
    create_dimension,
    get_table_metrics,
    get_table_dimensions,
    get_table_fields,
    get_table_field_column,
    get_dialect_type_conversions,
    FieldManagerMixin,
)
from zillion.sql_utils import (
    column_fullname,
    infer_aggregation_and_rounding,
    is_probably_metric,
    get_schemas,
    filter_dialect_schemas,
)


class TableSet(PrintMixin):
    repr_attrs = ["datasource", "join", "grain", "target_fields"]

    @initializer
    def __init__(self, datasource, ds_table, join, grain, target_fields):
        self.adhoc_datasources = []
        if isinstance(self.datasource, AdHocDataSource):
            self.adhoc_datasources = [self.datasource]

    def get_covered_metrics(self, wh):
        # Note: we pass in WH instead of DS here since the metric/dim may only
        # be defined at the WH level even if it exists in the DS
        covered_metrics = get_table_metrics(
            wh, self.ds_table, adhoc_fms=self.adhoc_datasources
        )
        return covered_metrics

    def get_covered_fields(self):
        covered_fields = get_table_fields(
            self.ds_table, adhoc_fms=self.adhoc_datasources
        )
        return covered_fields

    def __len__(self):
        if not self.join:
            return 1
        return len(self.join.table_names)


class JoinPart(PrintMixin):
    repr_attrs = ["datasource", "table_names", "join_fields"]

    @initializer
    def __init__(self, datasource, table_names, join_fields):
        pass


class Join(PrintMixin):
    """
    Join is a group of join parts that would be used together.
    field_map represents the requested fields this join list is meant to satisfy
    """

    repr_attrs = ["datasource", "table_names", "field_map"]

    @initializer
    def __init__(self, join_parts, field_map):
        self.datasource = None
        self.table_names = OrderedSet()
        for join_part in self.join_parts:
            if not self.datasource:
                self.datasource = join_part.datasource
            else:
                assert join_part.datasource.name == self.datasource.name, (
                    "Can not form %s using join_parts from different datasources"
                    % self.__class__
                )
            for table_name in join_part.table_names:
                self.table_names.add(table_name)

    def __key(self):
        return tuple(self.table_names)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.__key() == other.__key()

    def __len__(self):
        return len(self.table_names)

    def get_covered_fields(self):
        """Generate a list of all possible fields this can cover"""
        fields = set()
        for table_name in self.table_names:
            table = self.datasource.metadata.tables[table_name]
            covered_fields = get_table_fields(table)
            fields = fields | covered_fields
        return fields

    def add_field(self, field):
        assert field not in self.field_map, "Field %s is already in field map" % field
        for table_name in self.table_names:
            table = self.datasource.metadata.tables[table_name]
            covered_fields = get_table_fields(table)
            if field in covered_fields:
                column = get_table_field_column(table, field)
                self.field_map[field] = column
                return
        assert False, "Field %s is not in any join tables: %s" % (
            field,
            self.table_names,
        )

    def add_fields(self, fields):
        for field in fields:
            if field not in self.field_map:
                self.add_field(field)


def joins_from_path(ds, path, field_map=None):
    join_parts = []
    if len(path) == 1:
        # A placeholder join that is really just a single table
        join_part = JoinPart(ds, path, None)
        join_parts.append(join_part)
    else:
        for i, node in enumerate(path):
            if i == (len(path) - 1):
                break
            start, end = path[i], path[i + 1]
            edge = ds.graph.edges[start, end]
            join_part = JoinPart(ds, [start, end], edge["join_fields"])
            join_parts.append(join_part)
    return Join(join_parts, field_map=field_map)


class NeighborTable(PrintMixin):
    repr_attrs = ["table_name", "join_fields"]

    @initializer
    def __init__(self, table, join_fields):
        self.table_name = table.fullname


class DataSource(FieldManagerMixin, PrintMixin):
    repr_attrs = ["name"]

    def __init__(
        self,
        name,
        metadata=None,
        config=None,
        reflect=False,
        skip_conversion_fields=False,
    ):
        self.name = DataSource.check_or_create_name(name)
        self._metrics = {}
        self._dimensions = {}
        self.graph = None

        config = config or {}
        if config:
            config = DataSourceConfigSchema().load(config)

        url = config.get("url", None)

        ds_config_context = zillion_config.get("DATASOURCE_CONTEXTS", {}).get(
            self.name, {}
        )
        if url and get_string_format_args(url):
            url = url.format(**ds_config_context)

        assert url != ADHOC_URL, "Unsupported datasource URL: '%s'" % url
        assert metadata or url, "You must pass metadata or config->url"
        assert not (
            url and metadata
        ), "Only one of metadata or config->url may be specified"

        if url:
            self.check_url(url)
            self.metadata = sa.MetaData()
            self.metadata.bind = sa.create_engine(url)
        else:
            assert isinstance(metadata, sa.MetaData), (
                "Invalid MetaData object: %s" % metadata
            )
            self.metadata = metadata
            assert (
                self.metadata.bind
            ), "MetaData object must have a bind (engine) attribute specified"

        self.reflect = reflect
        if reflect:
            self.reflect_metadata()

        self.apply_config(config, skip_conversion_fields=skip_conversion_fields)

    @classmethod
    def check_or_create_name(cls, name):
        if not name:
            datestr = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            name = "zillion_ds_%s_%s" % (datestr, random.randint(0, 1e9))
            return name
        assert set(name) <= DATASOURCE_ALLOWABLE_CHARS, (
            'DataSource name "%s" has invalid characters. Allowed: %s'
            % (name, DATASOURCE_ALLOWABLE_CHARS_STR)
        )
        return name

    @classmethod
    def from_config(cls, name, config):
        return cls(name, reflect=True, config=config)

    @property
    def metric_tables(self):
        return {
            table_name: table
            for table_name, table in self.metadata.tables.items()
            if table.zillion and table.zillion.type == TableTypes.METRIC
        }

    @property
    def dimension_tables(self):
        return {
            table_name: table
            for table_name, table in self.metadata.tables.items()
            if table.zillion and table.zillion.type == TableTypes.DIMENSION
        }

    def get_dialect_name(self):
        return self.metadata.bind.dialect.name

    def check_url(self, url):
        url = make_url(url)
        if url.get_dialect().name == "sqlite":
            assert os.path.isfile(url.database), (
                "SQLite DB does not exist: %s" % url.database
            )

    def reflect_metadata(self):
        dialect = self.get_dialect_name()
        schemas = get_schemas(self.metadata.bind)
        schemas = filter_dialect_schemas(schemas, dialect)
        for schema in schemas:
            self.metadata.reflect(schema=schema, views=True)

    def get_params(self):
        # TODO: does this need to store more information, entire config?
        return dict(
            name=self.name, url=str(self.metadata.bind.url), reflect=self.reflect
        )

    def print_info(self):
        print("---- Datasource %s" % self.name)
        print("metrics:")
        self.print_metrics(indent=2)
        print("dimensions:")
        self.print_dimensions(indent=2)

        print()
        for table in self.metadata.tables.values():
            print(format_msg("table: %s" % table.fullname, label=None))
            zillion_info = table.info.get("zillion", None)
            if not zillion_info:
                print(format_msg("table has no zillion info", label=None, indent=2))
                continue

            for column in table.c:
                print(format_msg("column: %s" % column.name, label=None, indent=2))
                zillion_info = column.info.get("zillion", None)
                if not zillion_info:
                    print(
                        format_msg("column has no zillion info", label=None, indent=4)
                    )
                    continue

                print(format_msg(column.info["zillion"], label=None, indent=4))

    def apply_table_configs(self, table_configs):
        """Take configs and apply them to the table/column metadata"""

        for table in self.metadata.tables.values():
            if table.fullname not in table_configs:
                continue

            table_config = table_configs[table.fullname]
            for param in ADHOC_TABLE_CONFIG_PARAMS:
                assert not table_config.get(param, None), (
                    "AdHoc table config param '%s' passed to non-adhoc datasource"
                    % param
                )

            table_info = TableInfo.schema_load(table_config, unknown=EXCLUDE)

            zillion_info = table.info.get("zillion", {})
            # Config takes precedence over values on table objects
            zillion_info.update(table_info)
            table.info["zillion"] = TableInfo.create(zillion_info)

            column_configs = table_config.get("columns", None)
            if not column_configs:
                continue

            for column in table.columns:
                if column.name not in column_configs:
                    continue

                column_config = column_configs[column.name]
                zillion_info = column.info.get("zillion", {})
                # Config takes precedence over values on column objects
                zillion_info.update(column_config)

                if table.info["zillion"].use_full_column_names:
                    field_name = default_field_name(column)
                else:
                    field_name = column.name
                is_valid_field_name(field_name)

                zillion_info["fields"] = zillion_info.get("fields", [field_name])
                column.info["zillion"] = ColumnInfo.create(zillion_info)

    def ensure_metadata_info(self):
        """Ensure that all zillion info are of proper type"""
        for table in self.metadata.tables.values():
            zillion_info = table.info.get("zillion", None)
            if not zillion_info:
                setattr(table, "zillion", None)
                continue

            table.info["zillion"] = TableInfo.create(zillion_info)
            setattr(table, "zillion", table.info["zillion"])

            column_count = 0

            for column in table.c:
                zillion_info = column.info.get("zillion", None) or {}
                if not zillion_info:
                    if not table.zillion.create_fields:
                        assert not column.primary_key, (
                            "Primary key column %s must have zillion info defined"
                            % column_fullname(column)
                        )
                        # If create_fields IS set the zillion info would
                        # automatically get created on the column and fields
                        # would automatically be created from the columns.
                        # Since it is NOT set, we just set the attribute to
                        # None and move on.
                        setattr(column, "zillion", None)
                        continue

                if table.zillion.use_full_column_names:
                    field_name = default_field_name(column)
                else:
                    field_name = column.name
                is_valid_field_name(field_name)

                zillion_info["fields"] = zillion_info.get("fields", [field_name])
                if column.primary_key:
                    assert zillion_info["fields"], (
                        "Primary key column %s must have fields defined and one must be a valid dimension"
                        % column_fullname(column)
                    )
                column.info["zillion"] = ColumnInfo.create(zillion_info)
                setattr(column, "zillion", column.info["zillion"])
                column_count += 1

            assert column_count, (
                "Table %s has no columns with zillion info defined" % table.fullname
            )

    def add_conversion_fields(self):
        for table in self.metadata.tables.values():
            if not table.zillion:
                continue

            table_fields = get_table_fields(table)
            types_converted = set()

            for column in table.c:
                if not column.zillion:
                    continue

                if not column.zillion.allow_type_conversions:
                    # TODO: could allow specifying certain allowable conversions
                    # instead of just on/off switch
                    continue

                convs = get_dialect_type_conversions(self.get_dialect_name(), column)
                if convs:
                    assert not type(column.type) in types_converted, (
                        "Table %s has multiple columns of same type allowing conversions"
                        % table.fullname
                    )
                    types_converted.add(type(column.type))

                for field_def, ds_formula in convs:
                    field_name = field_def.name
                    field_def = field_def.copy()

                    if column.zillion.type_conversion_prefix:
                        field_name = column.zillion.type_conversion_prefix + field_name
                        is_valid_field_name(field_name)
                        field_def.name = field_name

                    if field_name in table_fields:
                        dbg(
                            "Skipping conversion field %s for column %s, already in table"
                            % (field_name, column_fullname(column))
                        )
                        continue
                    dbg(
                        "Adding conversion field %s for column %s"
                        % (field_name, column_fullname(column))
                    )
                    column.zillion.add_field(
                        dict(name=field_name, ds_formula=ds_formula)
                    )

                    if not self.has_field(field_name):
                        if isinstance(field_def, Dimension):
                            self.add_dimension(field_def)
                        else:
                            self.add_metric(field_def)

    def add_metric_column(self, column, field):
        if not self.has_metric(field):
            dbg(
                "Adding metric %s from column %s.%s"
                % (field, self.name, column_fullname(column))
            )
            aggregation, rounding = infer_aggregation_and_rounding(column)
            metric = Metric(
                field, column.type, aggregation=aggregation, rounding=rounding
            )
            self.add_metric(metric)

    def add_dimension_column(self, column, field):
        if not self.has_dimension(field):
            dbg(
                "Adding dimension %s from column %s.%s"
                % (field, self.name, column_fullname(column))
            )
            dimension = Dimension(field, column.type)
            self.add_dimension(dimension)

    def add_metric_table_fields(self, table):
        for column in table.c:
            if not column.zillion:
                continue

            if not column.zillion.active:
                continue

            for field, field_def in column.zillion.get_fields().items():
                if self.has_field(field):
                    continue

                if not table.zillion.create_fields:
                    # If create_fields is False we do not automatically create fields
                    # from columns. The field would have to be explicitly defined
                    # in the metrics/dimensions of the datasource.
                    continue

                formula = (
                    field_def.get("ds_formula", None)
                    if isinstance(field_def, dict)
                    else None
                )
                if is_probably_metric(column, formula=formula):
                    self.add_metric_column(column, field)
                else:
                    self.add_dimension_column(column, field)

    def add_dimension_table_fields(self, table):
        for column in table.c:
            if not column.zillion:
                continue

            if not column.zillion.active:
                continue

            for field in column.zillion.get_field_names():
                if self.has_metric(field):
                    assert False, "Dimension table has metric field: %s" % field

                if self.has_dimension(field):
                    continue

                if not table.zillion.create_fields:
                    # If create_fields is False we do not automatically create fields
                    # from columns. The field would have to be explicitly defined
                    # in the metrics/dimensions of the datasource.
                    continue

                self.add_dimension_column(column, field)

    def populate_fields(self, config):
        self.populate_global_fields(config, force=True)

        for table in self.metadata.tables.values():
            if not table.zillion:
                continue
            if table.zillion.type == TableTypes.METRIC:
                self.add_metric_table_fields(table)
            elif table.zillion.type == TableTypes.DIMENSION:
                self.add_dimension_table_fields(table)
            else:
                assert False, "Invalid table type: %s" % table.zillion.type

    def find_neighbor_tables(self, table):
        neighbor_tables = []
        fields = get_table_fields(table)

        if table.zillion.type == TableTypes.METRIC:
            # Find dimension tables whose primary key is contained in the metric table
            for dim_table in self.dimension_tables.values():
                dt_pk_fields = dim_table.zillion.primary_key
                can_join = True
                for field in dt_pk_fields:
                    if field not in fields:
                        can_join = False
                        break
                if can_join:
                    neighbor_tables.append(NeighborTable(dim_table, dt_pk_fields))

        # Add parent table if present
        parent_name = table.zillion.parent
        if parent_name:
            parent = self.metadata.tables[parent_name]
            pk_fields = parent.zillion.primary_key
            for pk_field in pk_fields:
                assert pk_field in fields, (
                    "Table %s is parent of %s but primary key %s is not in both"
                    % (parent.fullname, table.fullname, pk_fields)
                )
            neighbor_tables.append(NeighborTable(parent, pk_fields))
        return neighbor_tables

    def build_graph(self):
        graph = nx.DiGraph()
        self.graph = graph
        for table in self.metadata.tables.values():
            if not table.zillion:
                continue

            self.graph.add_node(table.fullname)
            neighbors = self.find_neighbor_tables(table)
            for neighbor in neighbors:
                self.graph.add_node(neighbor.table.fullname)
                self.graph.add_edge(
                    table.fullname,
                    neighbor.table.fullname,
                    join_fields=neighbor.join_fields,
                )

    def apply_config(self, config, skip_conversion_fields=False):
        if config.get("tables", None):
            self.apply_table_configs(config["tables"])

        self.ensure_metadata_info()

        if not skip_conversion_fields:
            self.add_conversion_fields()

        self.populate_fields(config)

        self.build_graph()

    def has_table(self, table):
        return table.fullname in self.metadata.tables

    def get_table(self, fullname):
        return self.metadata.tables[fullname]

    def get_tables_with_field(self, field_name, table_type=None):
        tables = []
        for table in self.metadata.tables.values():
            if not table.zillion:
                continue
            if table_type and table.zillion.type != table_type:
                continue
            if field_name in get_table_fields(table):
                tables.append(table)
        return tables

    def get_metric_tables_with_metric(self, metric_name):
        return self.get_tables_with_field(metric_name, table_type=TableTypes.METRIC)

    def get_dim_tables_with_dim(self, dim_name):
        return self.get_tables_with_field(dim_name, table_type=TableTypes.DIMENSION)

    def get_columns_with_field(self, field_name):
        columns = []
        for table in self.metadata.tables.values():
            if not table.zillion:
                continue
            for col in table.c:
                if not (getattr(col, "zillion", None) and col.zillion.active):
                    continue
                if field_name in col.zillion.get_field_names():
                    columns.append(col)
        return columns

    def invert_field_joins(self, field_joins):
        """Take a map of fields to relevant joins and invert it"""
        join_fields = defaultdict(set)
        for field, joins in field_joins.items():
            for join in joins:
                if join in join_fields:
                    dbg("join %s already used, adding field %s" % (join, field))
                join_fields[join].add(field)
        return join_fields

    def populate_max_join_field_coverage(self, join_fields, grain):
        for join, covered_fields in join_fields.items():
            for field in grain:
                if field in covered_fields:
                    continue
                all_covered_fields = join.get_covered_fields()
                if field in all_covered_fields:
                    covered_fields.add(field)

    def eliminate_redundant_joins(self, sorted_join_fields):
        joins_to_delete = set()
        for join, covered_fields in sorted_join_fields:
            if join in joins_to_delete:
                continue

            dbg(
                "Finding redundant joins for %s / %s"
                % (join.table_names, covered_fields)
            )
            for other_join, other_covered_fields in sorted_join_fields:
                if join == other_join or join in joins_to_delete:
                    continue

                is_superset = join.table_names.issubset(other_join.table_names)
                has_unique_fields = other_covered_fields - covered_fields
                if is_superset and not has_unique_fields:
                    dbg(
                        "Removing redundant join %s / %s"
                        % (other_join.table_names, other_covered_fields)
                    )
                    joins_to_delete.add(other_join)

        sorted_join_fields = [
            (join, fields)
            for join, fields in sorted_join_fields
            if join not in joins_to_delete
        ]
        return sorted_join_fields

    def find_join_combinations(self, sorted_join_fields, grain):
        candidates = []
        for join_combo in powerset(sorted_join_fields):
            if not join_combo:
                continue

            covered = set()
            has_subsets = False
            for join, covered_dims in join_combo:
                covered |= covered_dims
                # If any of the other joins are a subset of this join we ignore it.
                # The powerset has every combination so it will eventually hit the case
                # where there are only distinct joins.
                for other_join, other_covered_dims in join_combo:
                    if join == other_join:
                        continue
                    if other_join.table_names.issubset(join.table_names):
                        has_subsets = True
                        break
                if has_subsets:
                    break

            if has_subsets:
                continue

            if len(covered) == len(grain):
                # This combination of joins covers the entire grain. Add it as a candidate if
                # there isn't an existing candidate that is a a subset of these joins
                skip = False
                joins = set([x[0] for x in join_combo])
                for other_join_combo in candidates:
                    other_joins = set([x[0] for x in other_join_combo])
                    if other_joins.issubset(joins):
                        skip = True
                if skip:
                    dbg("Skipping subset join list combination")
                    continue
                candidates.append(join_combo)

        return candidates

    def choose_best_join_combination(self, candidates):
        ordered = sorted(
            candidates, key=lambda x: len(iter_or([y[0].table_names for y in x]))
        )
        chosen = ordered[0]
        join_fields = {}
        for join, covered_fields in chosen:
            join_fields[join] = covered_fields
            join.add_fields(covered_fields)
        return join_fields

    def consolidate_field_joins(self, grain, field_joins):
        """This takes a mapping of fields to joins that satisfy that field
        and returns a minimized map of joins to fields satisfied by that join.
        """

        # Some preliminary shuffling of the inputs to support later logic
        join_fields = self.invert_field_joins(field_joins)
        self.populate_max_join_field_coverage(join_fields, grain)

        # Sort by number of dims covered desc, number of tables involved asc
        sorted_join_fields = sorted(
            join_fields.items(), key=lambda kv: (len(kv[1]), -len(kv[0])), reverse=True
        )

        if len(sorted_join_fields[0][1]) == len(grain):
            # Single join covers entire grain. It should be ~optimal based on sorting.
            join = sorted_join_fields[0][0]
            covered_fields = sorted_join_fields[0][1]
            join.add_fields(covered_fields)
            return {join: covered_fields}

        sorted_join_fields = self.eliminate_redundant_joins(sorted_join_fields)
        candidates = self.find_join_combinations(sorted_join_fields, grain)
        join_fields = self.choose_best_join_combination(candidates)
        return join_fields

    def find_joins_to_dimension(self, table, dimension):
        joins = []

        dim_columns = self.get_columns_with_field(dimension)
        dim_column_table_map = {c.table.fullname: c for c in dim_columns}

        for column in dim_columns:
            if column.table == table:
                paths = [[table.fullname]]
            else:
                # TODO: consider caching with larger graphs, or precomputing
                paths = nx.all_simple_paths(
                    self.graph, table.fullname, column.table.fullname
                )

            if not paths:
                continue

            for path in paths:
                # For each path, if this dim can be found earlier in the path then
                # reference it in the earlier (child) table
                field_map = None
                for table_name in path:
                    if table_name in dim_column_table_map:
                        field_map = {dimension: dim_column_table_map[table_name]}
                        break

                assert field_map, "Could not map dimension %s to column" % dimension
                join = joins_from_path(self, path, field_map=field_map)
                joins.append(join)

        dbg("Found joins to dim %s for table %s:" % (dimension, table.fullname))
        dbg(joins)
        return joins

    def get_possible_joins(self, table, grain):
        """This takes a given table (usually a metric table) and tries to find one or
        more joins to each dimension of the grain. It's possible some of these
        joins satisfy other parts of the grain too which leaves room for
        consolidation, but it's also possible to have it generate independent,
        non-overlapping joins to meet the grain.
        """
        assert self.has_table(table), "Could not find table %s in datasource %s" % (
            table.fullname,
            self.name,
        )

        if not grain:
            dbg("No grain specified, ignoring joins")
            return None

        possible_dim_joins = {}
        for dimension in grain:
            dim_joins = self.find_joins_to_dimension(table, dimension)
            if not dim_joins:
                dbg(
                    "table %s can not satisfy dimension %s"
                    % (table.fullname, dimension)
                )
                return None

            possible_dim_joins[dimension] = dim_joins

        possible_joins = self.consolidate_field_joins(grain, possible_dim_joins)
        dbg("possible joins:")
        dbg(possible_joins)
        return possible_joins

    def find_possible_table_sets(self, ds_tables_with_field, field, grain):
        table_sets = []
        for field_table in ds_tables_with_field:
            if (not grain) or grain.issubset(get_table_fields(field_table)):
                table_set = TableSet(self, field_table, None, grain, set([field]))
                table_sets.append(table_set)
                dbg("full grain (%s) covered in %s" % (grain, field_table.fullname))
                continue

            joins = self.get_possible_joins(field_table, grain)
            if not joins:
                dbg("table %s can not join at grain %s" % (field_table.fullname, grain))
                continue

            dbg(
                "adding %d possible join(s) to table %s"
                % (len(joins), field_table.fullname)
            )
            for join, covered_dims in joins.items():
                table_set = TableSet(self, field_table, join, grain, set([field]))
                table_sets.append(table_set)

        return table_sets


class AdHocDataTable(PrintMixin):
    repr_attrs = ["name", "primary_key", "table_config"]

    @initializer
    def __init__(
        self,
        name,
        data,
        table_type,
        columns=None,
        primary_key=None,
        parent=None,
        schema=None,
        **kwargs
    ):
        self.df_kwargs = kwargs or {}
        self.table_config = TableConfigSchema().load(
            dict(
                type=table_type,
                columns=self.columns,
                create_fields=True,
                parent=parent,
                primary_key=primary_key,
            )
        )

    @property
    def fullname(self):
        if self.schema:
            return "%s.%s" % (self.schema, self.name)
        return self.name

    def get_dataframe(self):
        if isinstance(self.data, pd.DataFrame):
            return self.data

        kwargs = self.df_kwargs.copy()
        if self.columns:
            kwargs["columns"] = self.columns
        return pd.DataFrame.from_records(self.data, self.primary_key, **kwargs)

    def table_exists(self, engine):
        qr = engine.execute(
            "SELECT name FROM sqlite_master " "WHERE type='table' AND name=?",
            (self.name),
        )
        result = qr.fetchone()
        if result:
            return True
        return False

    def to_sql(self, engine, if_exists="fail", method="multi", chunksize=int(1e3)):
        if if_exists == "ignore":
            if self.table_exists(engine):
                return
            # Pandas doesn't actually have an "ignore" option, but switching
            # to fail will work because the table *should* not exist.
            if_exists = "fail"

        df = self.get_dataframe()

        # Note: this hits limits in allowed sqlite params if chunks are too large
        df.to_sql(
            self.name,
            engine,
            if_exists=if_exists,
            method=method,
            chunksize=chunksize,
            schema=self.schema,
        )


class SQLiteDataTable(AdHocDataTable):
    def get_dataframe(self):
        raise NotImplementedError

    def to_sql(self, engine, **kwargs):
        assert self.table_exists(engine), "SQLiteDataTable table does not exist"


class CSVDataTable(AdHocDataTable):
    def get_dataframe(self):
        return pd.read_csv(
            self.data,
            index_col=self.primary_key,
            usecols=list(self.columns.keys()) if self.columns else None,
            **self.df_kwargs
        )


class ExcelDataTable(AdHocDataTable):
    def get_dataframe(self):
        df = pd.read_excel(
            self.data,
            usecols=list(self.columns.keys()) if self.columns else None,
            **self.df_kwargs
        )
        if self.primary_key and df.index.names != self.primary_key:
            df = df.set_index(self.primary_key)
        return df


class JSONDataTable(AdHocDataTable):
    def get_dataframe(self, orient="table"):
        df = pd.read_json(self.data, orient=orient, **self.df_kwargs)
        if self.primary_key and df.index.names != self.primary_key:
            df = df.set_index(self.primary_key)
        return df


class HTMLDataTable(AdHocDataTable):
    def get_dataframe(self):
        # Expects this format by default:
        # df.reset_index().to_html("dma_zip.html", index=False)
        dfs = pd.read_html(self.data, **self.df_kwargs)
        assert dfs, "No html table found"
        assert len(dfs) == 1, "More than one html table found"
        df = dfs[0]
        if self.primary_key and df.index.names != self.primary_key:
            df = df.set_index(self.primary_key)
        return df


class GoogleSheetsDataTable(AdHocDataTable):
    def get_dataframe(self):
        parsed = urlparse(self.data)
        params = parse_qs(parsed.query)
        if params.get("format", None) == ["csv"]:
            cls = CSVDataTable
        elif parsed.path.endswith("/edit"):
            parsed = parsed._replace(
                path=parsed.path.replace("/edit", "/export"), query="format=csv"
            )
            url = urlunparse(parsed)
        else:
            raise Exception("Unsupported google docs URL: %s" % url)

        return pd.read_csv(
            url,
            index_col=self.primary_key,
            usecols=list(self.columns.keys()) if self.columns else None,
            **self.df_kwargs
        )


class AdHocDataSource(DataSource):
    def __init__(
        self, datatables, name=None, config=None, coerce_float=True, if_exists="fail"
    ):
        config = config or dict(tables={})
        ds_name = self.check_or_create_name(name)

        conn_url = self.get_datasource_url(ds_name)
        engine = sa.create_engine(conn_url, echo=False)

        for dt in datatables:
            dt.to_sql(engine, if_exists=if_exists)
            config.setdefault("tables", {})[dt.fullname] = dt.table_config

        metadata = sa.MetaData()
        metadata.bind = engine

        super(AdHocDataSource, self).__init__(
            ds_name, metadata=metadata, config=config, reflect=True
        )

    @classmethod
    def from_config(cls, name, config, if_exists="fail"):
        for table_name, table_config in config["tables"].items():
            assert table_config.get(
                "url", None
            ), "All tables in an adhoc datasource config must have a url"

        ds_config_context = zillion_config.get("DATASOURCE_CONTEXTS", {}).get(name, {})

        datatables = []
        for table_name, table_config in config["tables"].items():
            cfg = table_config.copy()
            schema = None

            if get_string_format_args(cfg["url"]):
                cfg["url"] = cfg["url"].format(**ds_config_context)

            if "." in table_name:
                parts = table_name.split(".")
                # This is also checked in config, should never happen
                assert len(parts) == 2, "Invalid table name: %s" % table_name
                schema, table_name = parts

            dt = datatable_from_config(table_name, cfg, schema=schema)
            datatables.append(dt)

        return cls(datatables, name=name, if_exists=if_exists)

    @classmethod
    def get_datasource_filename(cls, ds_name):
        dir_name = zillion_config["ADHOC_DATASOURCE_DIRECTORY"]
        return "%s/%s.db" % (dir_name, ds_name)

    @classmethod
    def get_datasource_url(cls, ds_name):
        return "sqlite:///%s" % cls.get_datasource_filename(ds_name)

    def clean_up(self):
        filename = self.get_datasource_filename(self.name)
        dbg("Removing %s" % filename)
        rmfile(filename)


def datasource_from_config(name, config, if_exists="fail"):
    if config.get("url", None) == ADHOC_URL:
        return AdHocDataSource.from_config(name, config, if_exists=if_exists)
    return DataSource.from_config(name, config)


def datatable_from_config(name, config, schema=None, **kwargs):
    assert config.get(
        "create_fields", True
    ), "AdHocDataTables must have create_fields=True"

    url = config["url"]
    if url.endswith("csv"):
        cls = CSVDataTable
    elif url.endswith("xlsx") or url.endswith("xls"):
        cls = ExcelDataTable
    elif url.endswith("json"):
        cls = JSONDataTable
    elif url.endswith("html"):
        cls = HTMLDataTable
    elif "docs.google.com" in url:
        cls = GoogleSheetsDataTable

    kwargs.update(config.get("adhoc_table_options", {}))

    return cls(
        name,
        url,
        config["type"],
        config.get("columns", None),
        primary_key=config.get("primary_key", None),
        parent=config.get("parent", None),
        schema=schema,
        **kwargs
    )
