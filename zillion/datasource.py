from collections import defaultdict
import datetime
import os
import random
from urllib.parse import urlparse, urlunparse, parse_qs

import pandas as pd
import networkx as nx
from orderedset import OrderedSet
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from zillion.configs import (
    DATASOURCE_ALLOWABLE_CHARS,
    DATASOURCE_ALLOWABLE_CHARS_STR,
    TableInfo,
    ColumnInfo,
    DataSourceConfigSchema,
    TableConfigSchema,
    default_field_name,
    is_valid_field_name,
    is_active,
    zillion_config,
    ADHOC_TABLE_CONFIG_PARAMS,
    EXCLUDE,
)
from zillion.core import *
from zillion.field import (
    Metric,
    Dimension,
    get_table_metrics,
    get_table_fields,
    get_table_field_column,
    get_dialect_type_conversions,
    table_field_allows_grain,
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
    """A set of tables in a datasource that can meet a grain and provide
    target fields.

    Parameters
    ----------
    datasource : DataSource
        The DataSource containing all tables
    ds_table : Table
        A table containing a desired metric or dimension
    join : Join
        A join to related tables that satisfies the grain and provides
        the target fields
    grain : list of str
        A list of dimensions that must be supported by the join
    target_fields : list of str
        A list of fields being targeted

    """

    repr_attrs = ["datasource", "join", "grain", "target_fields"]

    @initializer
    def __init__(self, datasource, ds_table, join, grain, target_fields):
        self._adhoc_datasources = []
        if isinstance(self.datasource, AdHocDataSource):
            self._adhoc_datasources = [self.datasource]

    def get_covered_metrics(self, wh):
        """Get a list of metrics covered by this table set

        Parameters
        ----------
        wh : Warehouse
           The warehouse to use as a reference for metric fields

        Returns
        -------
        list of str
            A list of metric names covered in this TableSet

        """
        covered_metrics = get_table_metrics(
            wh, self.ds_table, adhoc_fms=self._adhoc_datasources
        )
        return covered_metrics

    def get_covered_fields(self):
        """Get a list of all covered fields in this table set"""
        return get_table_fields(self.ds_table)

    def __len__(self):
        if not self.join:
            return 1
        return len(self.join.table_names)


class JoinPart(PrintMixin):
    """A part of a join that defines a join between two particular tables"""

    repr_attrs = ["datasource", "table_names", "join_fields"]

    @initializer
    def __init__(self, datasource, table_names, join_fields):
        pass


class Join(PrintMixin):
    """Represents a join (potentially multi-part) that will be part of a query"""

    repr_attrs = ["datasource", "table_names", "field_map"]

    @initializer
    def __init__(self, join_parts, field_map):
        """Initialize the Join from the given join parts

        Parameters
        ----------
        join_parts : list of JoinParts
            A list of JoinParts that will make up a single Join
        field_map : dict
            The requested fields this join is meant to satisfy

        """
        self.datasource = None
        self.table_names = OrderedSet()
        for join_part in self.join_parts:
            if not self.datasource:
                self.datasource = join_part.datasource
            else:
                raiseifnot(
                    join_part.datasource.name == self.datasource.name,
                    (
                        "Can not form %s using join_parts from different datasources"
                        % self.__class__
                    ),
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
        """Denote that this field is covered in this join"""
        raiseif(field in self.field_map, "Field %s is already in field map" % field)
        for table_name in self.table_names:
            table = self.datasource.metadata.tables[table_name]
            covered_fields = get_table_fields(table)
            if field in covered_fields:
                column = get_table_field_column(table, field)
                self.field_map[field] = column
                return
        raise ZillionException(
            "Field %s is not in any join tables: %s" % (field, self.table_names)
        )

    def add_fields(self, fields):
        """Add multiple fields as covered in this join"""
        for field in fields:
            if field not in self.field_map:
                self.add_field(field)

    def join_parts_for_table(self, table_name):
        """Get a list of JoinParts that reference a particular table"""
        return [jp for jp in self.join_parts if table_name in jp.table_names]

    def join_fields_for_table(self, table_name):
        """Get a list of join fields for a particular table in the join"""
        result = set()
        for jp in self.join_parts_for_table(table_name):
            if jp.join_fields:
                result |= set(jp.join_fields)
        return result


def join_from_path(ds, path, field_map=None):
    """Given a path in the datasource graph, get the corresponding Join

    Parameters
    ----------
    ds : DataSource
        The datasource for the join
    path : list of str
        A list of tables that form a join path
    field_map : dict, optional
        Passed through to Join init

    Returns
    -------
    Join
        A Join between all tables in the path

    """
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
            edge = ds._graph.edges[start, end]
            join_part = JoinPart(ds, [start, end], edge["join_fields"])
            join_parts.append(join_part)
    return Join(join_parts, field_map=field_map)


class NeighborTable(PrintMixin):
    """Represents a neighboring node in the datasource graph"""

    repr_attrs = ["table_name", "join_fields"]

    @initializer
    def __init__(self, table, join_fields):
        self.table_name = table.fullname


class DataSource(FieldManagerMixin, PrintMixin):
    """A component of a warehouse that houses one or more related tables

    Parameters
    ----------
    name : str
        The name of the datasource
    metadata : SQLAlchemy metadata, optional
        A SQLAlchemy metadata object that may have zillion configuration
        information defined in the table and column `info.zillion`
        attribute
    config : dict, optional
        A datasource config that will be applied to the metadata's zillion
        info
    reflect : bool, optional
        If true, use SQLAlchemy to reflect the database
    skip_conversion_fields : bool, optional
        Don't add any conversion fields when applying a config

    """

    repr_attrs = ["name"]

    def __init__(
        self,
        name,
        metadata=None,
        config=None,
        reflect=False,
        skip_conversion_fields=False,
    ):
        self.name = DataSource._check_or_create_name(name)
        self._metrics = {}
        self._dimensions = {}
        self._graph = None

        config = config or {}
        if config:
            config = DataSourceConfigSchema().load(config)

        url = config.get("url", None)

        ds_config_context = zillion_config.get("DATASOURCE_CONTEXTS", {}).get(
            self.name, {}
        )
        if url and get_string_format_args(url):
            url = url.format(**ds_config_context)

        raiseif(url == ADHOC_DS_URL, "Unsupported datasource URL: '%s'" % url)
        raiseifnot(metadata or url, "You must pass metadata or config->url")
        raiseif(
            url and metadata, "Only one of metadata or config->url may be specified"
        )

        if url:
            self._check_url(url)
            self.metadata = sa.MetaData()
            self.metadata.bind = sa.create_engine(url)
        else:
            raiseifnot(
                isinstance(metadata, sa.MetaData),
                "Invalid MetaData object: %s" % metadata,
            )
            self.metadata = metadata
            raiseifnot(
                self.metadata.bind,
                "MetaData object must have a bind (engine) attribute specified",
            )

        self._reflect = reflect
        if reflect:
            self._reflect_metadata()

        self.apply_config(config, skip_conversion_fields=skip_conversion_fields)

    @property
    def metric_tables(self):
        """A mapping of metric table names to table objects"""
        return {
            table_name: table
            for table_name, table in self.metadata.tables.items()
            if is_active(table) and table.zillion.type == TableTypes.METRIC
        }

    @property
    def dimension_tables(self):
        """A dimension of metric table names to table objects"""
        return {
            table_name: table
            for table_name, table in self.metadata.tables.items()
            if is_active(table) and table.zillion.type == TableTypes.DIMENSION
        }

    def has_table(self, table):
        """Check whether the table is in this datasource's metadata

        Parameters
        ----------
        table : SQLAlchemy Table
            A SQLAlchemy table

        Returns
        -------
        bool
            True if the table's fullname is in the metadata.tables map

        """
        if isinstance(table, str):
            name = table
        else:
            name = table.fullname
        return name in self.metadata.tables and is_active(self.metadata.tables[name])

    def get_table(self, fullname):
        """Get the table object from the datasource's metadata

        Parameters
        ----------
        fullname : str
            The full name of the table

        Returns
        -------
        Table
            The SQLAlchemy table object from the metadata

        """
        table = self.metadata.tables[fullname]
        if not is_active(table):
            raise ZillionException("Table %s is not active" % fullname)
        return table

    def get_tables_with_field(self, field_name, table_type=None):
        """Get a list of Tables that have a field

        Parameters
        ----------
        field_name : str
            The name of the field to check for
        table_type : str, optional
            Check only this TableType

        Returns
        -------
        list
            A list of Table objects

        """
        tables = []
        for table in self.metadata.tables.values():
            if not is_active(table):
                continue
            if table_type and table.zillion.type != table_type:
                continue
            if field_name in get_table_fields(table):
                tables.append(table)
        return tables

    def get_metric_tables_with_metric(self, metric_name):
        """Get a list of metric table objects with the given metric"""
        return self.get_tables_with_field(metric_name, table_type=TableTypes.METRIC)

    def get_dim_tables_with_dim(self, dim_name):
        """Get a list of dimension table objects with the given dimension"""
        return self.get_tables_with_field(dim_name, table_type=TableTypes.DIMENSION)

    def get_columns_with_field(self, field_name):
        """Get a list of column objects that support a field"""
        columns = []
        for table in self.metadata.tables.values():
            if not is_active(table):
                continue

            for col in table.c:
                if not is_active(col):
                    continue
                if field_name in col.zillion.get_field_names():
                    columns.append(col)
        return columns

    def apply_config(self, config, skip_conversion_fields=False):
        """Apply a datasource config to this datasource's metadata.
        This will also ensure zillion info is present on the metadata,
        populate global fields, and rebuild the datasource graph.

        Parameters
        ----------
        config : dict
            The datasource config to apply
        skip_conversion_fields : bool, optional
            Don't add any conversion fields when applying the config

        """
        if config.get("tables", None):
            self._apply_table_configs(config["tables"])

        self._ensure_metadata_info()

        if not skip_conversion_fields:
            self._add_conversion_fields()

        self._populate_fields(config)

        self._build_graph()

    def find_neighbor_tables(self, table):
        """Find tables that can be joined to or are parents of the given table

        Parameters
        ----------
        table : SQLAlchemy Table
            The table to find neighbors for

        Returns
        -------
        list
            A list of NeighborTables

        """
        neighbor_tables = []
        fields = get_table_fields(table)

        if table.zillion.type == TableTypes.METRIC:
            # Find dimension tables whose primary key is contained in the
            # metric table
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
                raiseifnot(
                    pk_field in fields,
                    (
                        "Table %s is parent of %s but primary key %s is not in both"
                        % (parent.fullname, table.fullname, pk_fields)
                    ),
                )
            neighbor_tables.append(NeighborTable(parent, pk_fields))
        return neighbor_tables

    def find_descendent_tables(self, table):
        """Find graph descendents of the table"""
        return nx.descendants(self._graph, table.fullname)

    def get_possible_joins(self, table, grain):
        """This takes a given table (usually a metric table) and tries to find
        one or more joins to each dimension of the grain. It's possible some
        of these joins satisfy other parts of the grain too which leaves room
        for consolidation, but it's also possible to have it generate
        independent, non-overlapping joins to meet the grain.

        Parameters
        ----------
        table : SQLAlchemy Table
            Table to analyze for joins to grain
        grain : iterable
            An iterable of dimension names that the given table must join to

        Returns
        -------
        dict
            A mapping of dimension -> dimension joins
        """
        raiseifnot(
            self.has_table(table),
            "Could not find table %s in datasource %s" % (table.fullname, self.name),
        )

        if not grain:
            dbg("No grain specified, ignoring joins")
            return None

        possible_dim_joins = {}
        for dimension in grain:
            dim_joins = self._find_joins_to_dimension(table, dimension)
            if not dim_joins:
                dbg(
                    "table %s can not satisfy dimension %s"
                    % (table.fullname, dimension)
                )
                return None

            possible_dim_joins[dimension] = dim_joins

        possible_joins = self._consolidate_field_joins(grain, possible_dim_joins)
        dbg("possible joins:")
        dbg(possible_joins)
        return possible_joins

    def find_possible_table_sets(
        self, ds_tables_with_field, field, grain, dimension_grain
    ):
        """Find table sets that meet the grain

        Parameters
        ----------
        ds_tables_with_field : list of tables
            A list of datasource tables that have the target field
        field : str
            The target field we are trying to cover
        grain : iterable
            The grain the table set must support
        dimension_grain
            The subset of the grain that are requested dimensions

        Returns
        -------
        list
            A list of TableSets

        """
        table_sets = []
        for field_table in ds_tables_with_field:
            if not table_field_allows_grain(field_table, field, dimension_grain):
                continue

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

    def get_dialect_name(self):
        """Get the name of the SQLAlchemy metadata dialect"""
        return self.metadata.bind.dialect.name

    def get_params(self):
        """Get a simple dict representation of the datasource params. This is
        currently not sufficient to completely rebuild the datasource.

        """
        # TODO: does this need to store entire config?
        # TODO: is the metadata URL exposing sensitive info?
        return dict(
            name=self.name, url=str(self.metadata.bind.url), reflect=self._reflect
        )

    def print_info(self):
        """Print the structure of the datasource"""
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

    def _check_url(self, url):
        """Check validity of the metadata URL"""
        url = make_url(url)
        if url.get_dialect().name == "sqlite":
            raiseifnot(
                os.path.isfile(url.database),
                "SQLite DB does not exist: %s" % url.database,
            )

    def _reflect_metadata(self):
        """Reflect the metadata object from the connection"""
        dialect = self.get_dialect_name()
        schemas = get_schemas(self.metadata.bind)
        schemas = filter_dialect_schemas(schemas, dialect)
        for schema in schemas:
            self.metadata.reflect(schema=schema, views=True)

    def _apply_table_configs(self, table_configs):
        """Take configs and apply them to the table/column metadata"""

        for table in self.metadata.tables.values():
            if table.fullname not in table_configs:
                continue

            table_config = table_configs[table.fullname]
            for param in ADHOC_TABLE_CONFIG_PARAMS:
                raiseif(
                    table_config.get(param, None),
                    (
                        "AdHoc table config param '%s' passed to non-adhoc datasource"
                        % param
                    ),
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

    def _ensure_metadata_info(self):
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
                        raiseif(
                            column.primary_key,
                            (
                                "Primary key column %s must have zillion info defined"
                                % column_fullname(column)
                            ),
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
                    raiseifnot(
                        zillion_info["fields"],
                        (
                            "Primary key column %s must have fields defined and"
                            "one must be a valid dimension"
                        )
                        % column_fullname(column),
                    )
                column.info["zillion"] = ColumnInfo.create(zillion_info)
                setattr(column, "zillion", column.info["zillion"])
                column_count += 1

            raiseifnot(
                column_count,
                "Table %s has no columns with zillion info defined" % table.fullname,
            )

    def _add_conversion_fields(self):
        """Add conversion fields where they are supported"""
        for table in self.metadata.tables.values():
            if not is_active(table):
                continue

            table_fields = get_table_fields(table)
            types_converted = set()

            for column in table.c:
                if not is_active(column):
                    continue

                if not column.zillion.allow_type_conversions:
                    continue

                convs = get_dialect_type_conversions(self.get_dialect_name(), column)
                if convs:
                    raiseif(
                        type(column.type) in types_converted,
                        (
                            "Table %s has multiple columns of same type allowing conversions"
                            % table.fullname
                        ),
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

    def _add_metric_column(self, column, field):
        """Add a metric to the datasource from a column"""
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

    def _add_dimension_column(self, column, field):
        """Add a dimension to the datasource from a column"""
        if not self.has_dimension(field):
            dbg(
                "Adding dimension %s from column %s.%s"
                % (field, self.name, column_fullname(column))
            )
            dimension = Dimension(field, column.type)
            self.add_dimension(dimension)

    def _add_metric_table_fields(self, table):
        """Populate fields from a metric table"""
        for column in table.c:
            if not is_active(column):
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
                    self._add_metric_column(column, field)
                else:
                    self._add_dimension_column(column, field)

    def _add_dimension_table_fields(self, table):
        """Populate fields from a dimension table"""
        for column in table.c:
            if not is_active(column):
                continue

            for field in column.zillion.get_field_names():
                if self.has_metric(field):
                    raise ZillionException(
                        "Dimension table has metric field: %s" % field
                    )

                if self.has_dimension(field):
                    continue

                if not table.zillion.create_fields:
                    # If create_fields is False we do not automatically create fields
                    # from columns. The field would have to be explicitly defined
                    # in the metrics/dimensions of the datasource.
                    continue

                self._add_dimension_column(column, field)

    def _populate_fields(self, config):
        """Populate fields from a datasource config"""
        self._populate_global_fields(config, force=True)

        for table in self.metadata.tables.values():
            if not is_active(table):
                continue
            if table.zillion.type == TableTypes.METRIC:
                self._add_metric_table_fields(table)
            elif table.zillion.type == TableTypes.DIMENSION:
                self._add_dimension_table_fields(table)
            else:
                raise ZillionException("Invalid table type: %s" % table.zillion.type)

    def _build_graph(self):
        """Build a directional graph representing the relationships between
        tables in the datasource"""

        graph = nx.DiGraph()
        self._graph = graph
        for table in self.metadata.tables.values():
            if not is_active(table):
                continue

            self._graph.add_node(table.fullname)
            neighbors = self.find_neighbor_tables(table)
            for neighbor in neighbors:
                self._graph.add_node(neighbor.table.fullname)
                self._graph.add_edge(
                    table.fullname,
                    neighbor.table.fullname,
                    join_fields=neighbor.join_fields,
                )

    def _invert_field_joins(self, field_joins):
        """Take a map of fields to relevant joins and invert it"""
        join_fields = defaultdict(set)
        for field, joins in field_joins.items():
            for join in joins:
                if join in join_fields:
                    dbg("join %s already used, adding field %s" % (join, field))
                join_fields[join].add(field)
        return join_fields

    def _populate_max_join_field_coverage(self, join_fields, grain):
        """Populate the relevant grain fields a join can cover"""
        for join, covered_fields in join_fields.items():
            for field in grain:
                if field in covered_fields:
                    continue
                all_covered_fields = join.get_covered_fields()
                if field in all_covered_fields:
                    covered_fields.add(field)

    def _eliminate_redundant_joins(self, sorted_join_fields):
        """Eliminate joins that aren't providing unique fields or are
        just table supersets of other joins"""
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

    def _find_join_combinations(self, sorted_join_fields, grain):
        """Find candidate joins that cover the entire grain"""
        candidates = []
        for join_combo in powerset(sorted_join_fields):
            if not join_combo:
                continue

            covered = set()
            has_subsets = False
            for join, covered_dims in join_combo:
                covered |= covered_dims
                # If any of the other joins are a subset of this join we
                # ignore it. The powerset has every combination so it will
                # eventually hit the case where there are only distinct joins.
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
                # This combination of joins covers the entire grain. Add it as
                # a candidate if there isn't an existing candidate that is a a
                # subset of these joins
                skip = False
                joins = {x[0] for x in join_combo}
                for other_join_combo in candidates:
                    other_joins = {x[0] for x in other_join_combo}
                    if other_joins.issubset(joins):
                        skip = True
                if skip:
                    dbg("Skipping subset join list combination")
                    continue
                candidates.append(join_combo)

        return candidates

    def _choose_best_join_combination(self, candidates):
        """Choose the best join combination from the candidates. Currently
        "best" is just defined as the candidate with the fewest tables."""
        ordered = sorted(
            candidates, key=lambda x: len(iter_or([y[0].table_names for y in x]))
        )
        chosen = ordered[0]
        join_fields = {}
        for join, covered_fields in chosen:
            join_fields[join] = covered_fields
            join.add_fields(covered_fields)
        return join_fields

    def _consolidate_field_joins(self, grain, field_joins):
        """This takes a mapping of fields to joins that satisfy each field and
        returns a minimized map of joins to fields satisfied by that join.
        """

        # Some preliminary shuffling of the inputs to support later logic
        join_fields = self._invert_field_joins(field_joins)
        self._populate_max_join_field_coverage(join_fields, grain)

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

        sorted_join_fields = self._eliminate_redundant_joins(sorted_join_fields)
        candidates = self._find_join_combinations(sorted_join_fields, grain)
        join_fields = self._choose_best_join_combination(candidates)
        return join_fields

    def _find_joins_to_dimension(self, table, dimension):
        """Find joins to a dimension from a table"""
        joins = []

        dim_columns = self.get_columns_with_field(dimension)
        dim_column_table_map = {c.table.fullname: c for c in dim_columns}

        for column in dim_columns:
            if column.table == table:
                paths = [[table.fullname]]
            else:
                paths = nx.all_simple_paths(
                    self._graph, table.fullname, column.table.fullname
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

                raiseifnot(
                    field_map, "Could not map dimension %s to column" % dimension
                )
                join = join_from_path(self, path, field_map=field_map)
                if table.zillion.incomplete_dimensions:
                    join_fields = join.join_fields_for_table(table.fullname)
                    if set(table.zillion.incomplete_dimensions) & join_fields:
                        dbg(
                            "Skipping table %s join due to incomplete dimensions"
                            % table.fullname
                        )
                        continue
                joins.append(join)

        dbg("Found joins to dim %s for table %s:" % (dimension, table.fullname))
        dbg(joins)
        return joins

    @classmethod
    def from_config(cls, name, config):
        """Create a DataSource from a datasource config"""
        return cls(name, reflect=True, config=config)

    @classmethod
    def _check_or_create_name(cls, name):
        """Validate the datasource name or create one if necessary"""
        if not name:
            datestr = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            name = "zillion_ds_%s_%s" % (datestr, random.randint(0, 1e9))
            return name
        raiseifnot(
            set(name) <= DATASOURCE_ALLOWABLE_CHARS,
            (
                'DataSource name "%s" has invalid characters. Allowed: %s'
                % (name, DATASOURCE_ALLOWABLE_CHARS_STR)
            ),
        )
        return name


class AdHocDataTable(PrintMixin):
    """Create an adhoc (temporary) representation of a table for use in an
    adhoc datasource"""

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
        """Initializes the datatable by parsing its config, but does not
        actually add it to a particular sqlite DB yet. It is assumed the
        AdHocDataSource will do that later.

        Parameters
        ----------
        name : str
            The name of the table
        data : iterable or DataFrame
            The data to create the adhoc table from
        table_type : str
            Specify the TableType
        columns : dict, optional
            Column configuration for the table
        primary_key : list of str, optional
            A list of fields that make up the primary key of the table
        parent : str, optional
            A reference to a parent table in the same datasource
        schema : str, optional
            The schema in which the table resides
        **kwargs
            Keyword arguments passed to pandas.DataFrame.from_records if
            a DataFrame is created from iterable data

        """
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
        """Table full name"""
        if self.schema:
            return "%s.%s" % (self.schema, self.name)
        return self.name

    def get_dataframe(self):
        """Get the DataFrame representation of the data"""
        if isinstance(self.data, pd.DataFrame):
            return self.data

        kwargs = self.df_kwargs.copy()
        if self.columns:
            kwargs["columns"] = self.columns
        return pd.DataFrame.from_records(self.data, self.primary_key, **kwargs)

    def table_exists(self, engine):
        """Determine if this table exists in the given sqlite connection"""
        qr = engine.execute(
            "SELECT name FROM sqlite_master " "WHERE type='table' AND name=?",
            (self.name),
        )
        result = qr.fetchone()
        if result:
            return True
        return False

    def to_sql(self, engine, if_exists="fail", method="multi", chunksize=int(1e3)):
        """Use pandas.DataFrame.to_sql to push the adhoc table data to a SQL database.

        Parameters
        ----------
        engine : SQLAlchemy connection engine
            The engine used to connect to the database
        if_exists : str, optional
            Passed through to to_sql. An additional option of "ignore" is
            supported which first checks if the table exists and if so takes
            no action.
        method : str, optional
            Passed through to to_sql
        chunksize : type, optional
            Passed through to to_sql

        """
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
    """AdHocDataTable from an existing sqlite database"""

    def get_dataframe(self):
        raise NotImplementedError

    def to_sql(self, engine, **kwargs):
        raiseifnot(self.table_exists(engine), "SQLiteDataTable table does not exist")


class CSVDataTable(AdHocDataTable):
    """AdHocDataTable from a JSON file using pandas.read_csv"""

    def get_dataframe(self):
        return pd.read_csv(
            self.data,
            index_col=self.primary_key,
            usecols=list(self.columns.keys()) if self.columns else None,
            **self.df_kwargs
        )


class ExcelDataTable(AdHocDataTable):
    """AdHocDataTable from a JSON file using pandas.read_excel"""

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
    """AdHocDataTable from a JSON file using pandas.read_json"""

    def get_dataframe(self, orient="table"):
        df = pd.read_json(self.data, orient=orient, **self.df_kwargs)
        if self.primary_key and df.index.names != self.primary_key:
            df = df.set_index(self.primary_key)
        return df


class HTMLDataTable(AdHocDataTable):
    """AdHocDataTable from an html table using pandas.read_html. By default
    it expects a table in the same format as produced by:
    `df.reset_index().to_html("table.html", index=False)`

    """

    def get_dataframe(self):
        dfs = pd.read_html(self.data, **self.df_kwargs)
        raiseifnot(dfs, "No html table found")
        raiseifnot(len(dfs) == 1, "More than one html table found")
        df = dfs[0]
        if self.primary_key and df.index.names != self.primary_key:
            df = df.set_index(self.primary_key)
        return df


class GoogleSheetsDataTable(AdHocDataTable):
    """AdHocDataTable from a google sheet. Parsed as a CSVDataTable."""

    def get_dataframe(self):
        parsed = urlparse(self.data)
        params = parse_qs(parsed.query)
        if params.get("format", None) == ["csv"]:
            pass
        elif parsed.path.endswith("/edit"):
            parsed = parsed._replace(
                path=parsed.path.replace("/edit", "/export"), query="format=csv"
            )
            url = urlunparse(parsed)
        else:
            raise ZillionException("Unsupported google docs URL: %s" % url)

        return pd.read_csv(
            url,
            index_col=self.primary_key,
            usecols=list(self.columns.keys()) if self.columns else None,
            **self.df_kwargs
        )


class AdHocDataSource(DataSource):
    """Create an adhoc (temporary) datasource from a set of adhoc data
    tables. The main use case for this is when temporarily augmenting a
    report or set of reports with some temporary data that is not meant to
    be permanently added to the warehouse.

    Parameters
    ----------
    datatables : list of AdHocDataTables
        The AdHocDataTables that make up the datasource
    name : str, optional
        A name for the datasources. If missing a unique name will be provided.
    config : dict, optional
        A datasource config that describes this datasources.
    if_exists : str, optional
        Passed through to the AdHocDataTable's to_sql method

    """

    def __init__(self, datatables, name=None, config=None, if_exists="fail"):
        config = config or dict(tables={})
        ds_name = self._check_or_create_name(name)

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
        """Create an AdHocDataSource from a datasource config. All tables
        in the config must have a URL pointing to a data resource.

        """
        for table_name, table_config in config["tables"].items():
            raiseifnot(
                table_config.get("url", None),
                "All tables in an adhoc datasource config must have a url",
            )

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
                raiseifnot(len(parts) == 2, "Invalid table name: %s" % table_name)
                schema, table_name = parts

            dt = datatable_from_config(table_name, cfg, schema=schema)
            datatables.append(dt)

        return cls(datatables, name=name, if_exists=if_exists)

    @classmethod
    def get_datasource_filename(cls, ds_name):
        """Get the filename where the adhoc datasource will be located"""
        dir_name = zillion_config["ADHOC_DATASOURCE_DIRECTORY"]
        return "%s/%s.db" % (dir_name, ds_name)

    @classmethod
    def get_datasource_url(cls, ds_name):
        """Get a connection URL for the datasource"""
        return "sqlite:///%s" % cls.get_datasource_filename(ds_name)

    def clean_up(self):
        """Clean up by removing created resources"""
        filename = self.get_datasource_filename(self.name)
        dbg("Removing %s" % filename)
        rmfile(filename)


def datasource_from_config(name, config, if_exists="fail"):
    """Factory to create a datasource from a config.

    Parameters
    ----------
    name : str
        The name of the datasource
    config : dict
        The datasource config
    if_exists : str, optional
        If an adhoc datasource is being created pass this value
        through. Otherwise it is ignored.

    Returns
    -------
    DataSource
        The datasource created from the config

    """
    if config.get("url", None) == ADHOC_DS_URL:
        return AdHocDataSource.from_config(name, config, if_exists=if_exists)
    return DataSource.from_config(name, config)


def datatable_from_config(name, config, schema=None, **kwargs):
    """Factory to create an AdHocDataTable from a given config. The type of
    the AdHocDataTable created will be inferred from the config["url"] param.

    Parameters
    ----------
    name : str
        The name of the table
    config : dict
        The configuration of the table
    schema : str, optional
        The schema in which the table resides
    **kwargs
        Passed to init of the particular AdHocDataTable class

    Returns
    -------
    AdHocDataTable
        Return the created AdHocDataTable (subclass)

    """
    raiseifnot(
        config.get("create_fields", True),
        "AdHocDataTables must have create_fields=True",
    )

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
