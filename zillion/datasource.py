from collections import defaultdict
import datetime
import os
import random
from urllib.parse import urlparse, urlunparse, parse_qs

import pandas as pd
from pandas.io.sql import SQLTable, pandasSQL_builder
import networkx as nx
from ordered_set import OrderedSet
import sqlalchemy as sa

from zillion.configs import (
    load_datasource_config,
    DATASOURCE_NAME_ALLOWED_CHARS,
    DATASOURCE_NAME_ALLOWED_CHARS_STR,
    TableInfo,
    ColumnInfo,
    DataSourceConnectSchema,
    TableConfigSchema,
    default_field_name,
    default_field_display_name,
    table_safe_name,
    is_valid_field_name,
    is_valid_datasource_criteria_conversions,
    is_active,
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
from zillion.nlp import get_nlp_table_info
from zillion.sql_utils import (
    column_fullname,
    infer_aggregation_and_rounding,
    is_probably_metric,
    get_schema_and_table_name,
    get_schemas,
    filter_dialect_schemas,
    check_metadata_url,
    type_string_to_sa_type,
)

DEFAULT_DB_ENGINE_POOL_SIZE = 10
DEFAULT_DB_ENGINE_MAX_OVERFLOW = 20


def entity_name_from_file(filename):
    return os.path.basename(filename.split("?")[0]).split(".")[0]


def get_ds_config_context(name):
    """Helper to get datasource context from the zillion config"""
    return zillion_config.get("DATASOURCE_CONTEXTS", {}).get(name, {})


def populate_url_context(url, ds_name):
    """Helper to do variable replacement in URLs"""
    ds_config_context = get_ds_config_context(ds_name)
    if get_string_format_args(url):
        url = url.format(**ds_config_context)
    return url


def get_engine_extra_kwargs(url):
    """Get the extra kwargs for the SQLAlchemy engine based on the URL"""
    if "sqlite" in url:
        return {}

    return {
        "pool_size": zillion_config.get(
            "DB_ENGINE_POOL_SIZE", DEFAULT_DB_ENGINE_POOL_SIZE
        ),
        "max_overflow": zillion_config.get(
            "DB_ENGINE_MAX_OVERFLOW", DEFAULT_DB_ENGINE_MAX_OVERFLOW
        ),
    }


def connect_url_to_metadata(url, ds_name=None):
    """Create a bound SQLAlchemy MetaData object from a database URL. The
    ds_name param is used to determine datasource config context for variable
    substitution."""
    if ds_name:
        url = populate_url_context(url, ds_name)
    check_metadata_url(url)
    metadata = sa.MetaData()
    metadata.bind = sa.create_engine(
        url,
        pool_pre_ping=True,
        **get_engine_extra_kwargs(url),
    )
    return metadata


def parse_replace_after(replace_after):
    """Parse a case-insensitive interval string of the format "number interval".
    The number may be a float, and the inverval options are: seconds, minutes,
    hours, days, weeks.
    """
    parts = replace_after.lower().strip().split()
    raiseifnot(
        len(parts) == 2,
        "Invalid replace_after format: '%s' expected: 'number interval'",
    )

    multipliers = dict(
        seconds=1, minutes=60, hours=60 * 60, days=24 * 60 * 60, weeks=7 * 24 * 60 * 60
    )

    try:
        num = float(parts[0])
    except ValueError:
        raise ZillionException("Invalid replace_after numeric value: %s" % parts[0])

    try:
        multiplier = multipliers[parts[1]]
    except KeyError:
        raise ZillionException("Invalid replace_after interval value: %s" % parts[1])

    return num * multiplier


def data_url_to_metadata(
    data_url,
    ds_name,
    if_exists=IfFileExistsModes.FAIL,
    replace_after=DEFAULT_REPLACE_AFTER,
):
    """Create a bound SQLAlchemy MetaData object from a data URL. The ds_name
    param is used to determine datasource config context for variable
    substitution."""
    dbfile = get_adhoc_datasource_filename(ds_name)

    skip = False
    if os.path.isfile(dbfile):
        raiseif(if_exists == IfFileExistsModes.FAIL, "File %s already exists" % dbfile)
        if if_exists == IfFileExistsModes.IGNORE:
            skip = True
        if if_exists == IfFileExistsModes.REPLACE_AFTER:
            seconds = parse_replace_after(replace_after)
            if get_time_since_modified(dbfile) < seconds:
                skip = True

    if not skip:
        data_url = populate_url_context(data_url, ds_name)
        f = download_file(data_url, outfile=dbfile)

    connect_url = get_adhoc_datasource_url(ds_name)
    engine = sa.create_engine(
        connect_url,
        echo=False,
        pool_pre_ping=True,
        **get_engine_extra_kwargs(connect_url),
    )
    metadata = sa.MetaData()
    metadata.bind = engine
    return metadata


def metadata_from_connect(connect, ds_name):
    """Create a bound SQLAlchemy MetaData object from a "connect" param. The
    connect value may be a connection string or a DataSourceConnectSchema dict.
    See the DataSourceConnectSchema docs for more details on that format."""
    if isinstance(connect, str):
        return connect_url_to_metadata(connect, ds_name=ds_name)

    schema = DataSourceConnectSchema()
    connect = schema.load(connect)
    func = import_object(connect["func"])
    params = connect.get("params", {})
    result = func(ds_name, **params)
    raiseifnot(
        isinstance(result, sa.MetaData),
        "Connect function did not return a MetaData object: %s" % result,
    )
    raiseifnot(
        result.is_bound(),
        "Connect function did not return a bound MetaData object: %s" % result,
    )
    return result


def reflect_metadata(metadata, reflect_only=None):
    """Reflect the metadata object from the connection. If reflect_only is
    passed, reflect only the tables specified in that list"""

    raiseifnot(metadata.is_bound(), "MetaData must be bound to an engine")
    only_schema_tables = defaultdict(list)
    only_tables = []

    if reflect_only:
        for table_name in reflect_only:
            schema, table_name = get_schema_and_table_name(table_name)
            if schema:
                only_schema_tables[schema].append(table_name)
            else:
                only_tables.append(table_name)

    dialect = metadata.bind.dialect.name
    schemas = get_schemas(metadata.bind)
    schemas = filter_dialect_schemas(schemas, dialect)

    for schema in schemas:
        only = only_schema_tables.get(schema, []) or []
        if only_tables:
            only.extend(only_tables)
        metadata.reflect(schema=schema, views=True, only=only or None)


def get_adhoc_datasource_filename(ds_name):
    """Get the filename where the adhoc datasource will be located"""
    dir_name = zillion_config["ADHOC_DATASOURCE_DIRECTORY"]
    return "%s/%s.db" % (dir_name, ds_name)


def get_adhoc_datasource_url(ds_name):
    """Get a connection URL for the datasource"""
    return "sqlite:///%s" % get_adhoc_datasource_filename(ds_name)


def url_connect(
    ds_name,
    connect_url=None,
    data_url=None,
    if_exists=IfFileExistsModes.FAIL,
    replace_after=DEFAULT_REPLACE_AFTER,
):
    """A URL-based datasource connector. This is meant to be used as the "func"
    value of a DataSourceConnectSchema. Only one of connect_url or data_url may
    be specified.

    **Parameters:**

    * **ds_name** - (*str*) The name of the datasource to get a connection for
    * **connect_url** - (*str, optional*) If a connect_url is passed, it will
    create a bound MetaData object from that connection string.
    * **data_url** - (*str, optional*) If a data_url is passed, it will first
    download that data (or make sure it is already downloaded) and then create a
    connection to that data file, which is assumed to be a SQLite database. The
    name of the database file will be based on the name of the datasource passed
    in.
    * **if_exists** - (*str, optional*) If a data_url is in use, this will
    control handling of existing data under the same filename. If "fail", an
    exception will be raised if the file already exists. If "ignore", it will
    skip downloading the file if it exists. If "replace", it will create or
    replace the file. If "replace_after" it will check the age of the file and
    replace it after the age interval provided in the `replace_after` param.
    * **replace_after** - (*str, optional*) If `if_exists` is "replace_after, use
    this value to determine the age threshold. The format is "number interval",
    where the number may be an int or float and the interval options are: seconds,
    minutes, hours, days, weeks. Note that this only occurs when `url_connect` is
    called, which is typically on datasource init; it does not replace itself
    periodically while the datasource is instantiated.

    """
    raiseif(connect_url and data_url, "Only one of connect_url or data_url may be set")
    raiseifnot(connect_url or data_url, "One of connect_url or data_url must be set")
    if if_exists and data_url:
        raiseifnot(
            if_exists in IfFileExistsModes, "Invalid if_exists value: %s" % if_exists
        )

    if data_url:
        return data_url_to_metadata(
            data_url, ds_name, if_exists=if_exists, replace_after=replace_after
        )

    return connect_url_to_metadata(connect_url, ds_name=ds_name)


class TableSet(PrintMixin):
    """A set of tables in a datasource that can meet a grain and provide target
    fields.

    **Parameters:**

    * **datasource** - (*DataSource*) The DataSource containing all tables
    * **ds_table** - (*Table*) A table containing a desired metric or dimension
    * **join** - (*Join*) A join to related tables that satisfies the grain and
    provides the target fields
    * **grain** - (*list of str*) A list of dimensions that must be supported by
    the join
    * **target_fields** - (*list of str*) A list of fields being targeted

    """

    repr_attrs = ["datasource", "join", "grain", "target_fields"]

    @initializer
    def __init__(self, datasource, ds_table, join, grain, target_fields):
        pass

    def get_covered_metrics(self, wh):
        """Get a list of metrics covered by this table set

        **Parameters:**

        * **wh** - (*Warehouse*) The warehouse to use as a reference for metric
        fields

        **Returns:**

        (*list of str*) - A list of metric names covered in this TableSet

        """
        adhoc_dses = []
        if self.datasource.name not in wh.datasource_names:
            adhoc_dses = [self.datasource]
        covered_metrics = get_table_metrics(wh, self.ds_table, adhoc_fms=adhoc_dses)
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
    """Represents a join (potentially multi-part) that will be part of a
    query

    **Parameters:**

    * **join_parts** - (*list of JoinParts*) A list of JoinParts that will
    make up a single Join
    * **field_map** - (*dict*) The requested fields this join is meant to
    satisfy

    """

    repr_attrs = ["datasource", "table_names", "field_map"]

    @initializer
    def __init__(self, join_parts, field_map):
        """Initialize the Join from the given join parts"""
        self.datasource = None
        self.table_names = OrderedSet()
        for join_part in self.join_parts:
            self.add_join_part_tables(join_part)

    def __key(self):
        return tuple(self.table_names)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.__key() == other.__key()

    def __len__(self):
        return len(self.table_names)

    def add_join_part_tables(self, join_part):
        """Add tables from join parts to the table list"""
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

    @classmethod
    def combine(cls, join1, join2):
        """Create a new Join object that combines the parts and fields of the given joins"""
        raiseifnot(
            join1.datasource == join2.datasource,
            "Can not combine joins from different datasources",
        )
        new_join = cls(
            [x for x in join1.join_parts], {k: v for k, v in join1.field_map.items()}
        )

        for join_part in join2.join_parts:
            new_join.add_join_part_tables(join_part)
            new_join.join_parts.append(join_part)

        for field in join2.field_map:
            if field not in new_join.field_map:
                new_join.add_field(field)

        return new_join


def join_from_path(ds, path, field_map=None):
    """Given a path in the datasource graph, get the corresponding Join

    **Parameters:**

    * **ds** - (*DataSource*) The datasource for the join
    * **path** - (*list of str*) A list of tables that form a join path
    * **field_map** - (*dict, optional*) Passed through to Join init

    **Returns:**

    (*Join*) - A Join between all tables in the path

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

    **Parameters:**

    * **name** - (*str*) The name of the datasource
    * **metadata** - (*SQLAlchemy metadata, optional*) A SQLAlchemy metadata
    object that may have zillion configuration information defined in the table
    and column `info.zillion` attribute
    * **config** - (*dict, str, or buffer, optional*) A dict adhering to the
    DataSourceConfigSchema or a file location to load the config from
    * **nlp** - (*bool, optional*) If true, allow NLP analysis when creating
    fields

    """

    repr_attrs = ["name"]

    def __init__(self, name, metadata=None, config=None, nlp=False):
        self.name = self._check_or_create_name(name)
        self._metrics = {}
        self._dimensions = {}
        self._graph = None
        self.prefix_with = None
        reflect = False

        if config:
            config = load_datasource_config(config)
        else:
            config = {}

        connect = config.get("connect", None)

        raiseifnot(metadata or connect, "You must pass metadata or config->connect")
        raiseif(
            connect and metadata,
            "Only one of metadata or config->connect may be specified",
        )

        if connect:
            self.metadata = metadata_from_connect(connect, self.name)
            reflect = True
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

        self.apply_config(config, reflect=reflect, nlp=nlp)

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

    def has_table(self, table, check_active=True):
        """Check whether the table is in this datasource's metadata

        **Parameters:**

        * **table** - (*SQLAlchemy Table*) A SQLAlchemy table
        * **check_active** - (*bool, optional*) If true, check that the table
        is active

        **Returns:**

        (*bool*) - True if the table's fullname is in the metadata.tables map

        """
        if isinstance(table, str):
            name = table
        else:
            name = table.fullname
        if name not in self.metadata.tables:
            return False
        if not check_active:
            return True
        return is_active(self.metadata.tables[name])

    def get_table(self, fullname, check_active=True):
        """Get the table object from the datasource's metadata

        **Parameters:**

        * **fullname** - (*str*) The full name of the table
        * **check_active** - (*bool, optional*) If true, check that the table


        **Returns:**

        (*Table*) - The SQLAlchemy table object from the metadata

        """
        table = self.metadata.tables[fullname]
        if check_active and not is_active(table):
            raise ZillionException("Table %s is not active" % fullname)
        return table

    def get_tables_with_field(self, field_name, table_type=None):
        """Get a list of Tables that have a field

        **Parameters:**

        * **field_name** - (*str*) The name of the field to check for
        * **table_type** - (*str, optional*) Check only this TableType

        **Returns:**

        (*list*) - A list of Table objects

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

    def apply_config(self, config, reflect=False, nlp=False):
        """Apply a datasource config to this datasource's metadata. This will
        also ensure zillion info is present on the metadata, populate global
        fields, and rebuild the datasource graph.

        **Parameters:**

        * **config** - (*dict*) The datasource config to apply
        * **reflect** - (*bool, optional*) If true, use SQLAlchemy to reflect
        the database. Table-level reflection will also occur if any tables are
        created from data URLs
        * **nlp** - (*bool, optional*) If true, allow NLP analysis when creating
        fields

        """
        raiseifnot(self.metadata, "apply_config called with no datasource metadata")

        reflect_only = None
        adhoc_tables = self._load_adhoc_tables(config)
        if adhoc_tables and not reflect:
            reflect = True
            reflect_only = adhoc_tables

        if reflect:
            reflect_metadata(self.metadata, reflect_only=reflect_only)

        if config.get("tables", None):
            self._apply_table_configs(config["tables"])

        self._ensure_metadata_info()

        if not config.get("skip_conversion_fields", False):
            self._add_conversion_fields()

        self.prefix_with = config.get("prefix_with", None)

        self._populate_fields(config, nlp=nlp)

        self._build_graph()

    def find_neighbor_tables(self, table):
        """Find tables that can be joined to or are parents of the given table

        **Parameters:**

        * **table** - (*SQLAlchemy Table*) The table to find neighbors for

        **Returns:**

        (*list*) - A list of NeighborTables

        """
        neighbor_tables = []
        fields = get_table_fields(table)

        def check_pk_fields(pk_fields, t, ttype):
            for pk_field in pk_fields:
                raiseifnot(
                    pk_field in fields,
                    (
                        "Table %s is %s of %s but primary key %s is not in both"
                        % (t.fullname, ttype, table.fullname, pk_fields)
                    ),
                )

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
            try:
                parent = self.metadata.tables[parent_name]
            except KeyError:
                raise AssertionError(
                    "Parent %s of %s not defined" % (parent_name, table.fullname)
                )

            pk_fields = parent.zillion.primary_key
            check_pk_fields(pk_fields, parent, "parent")
            neighbor_tables.append(NeighborTable(parent, pk_fields))

            if parent.zillion.type == TableTypes.DIMENSION and parent.zillion.siblings:
                # Add parent dimension sibling tables if present
                for sibling_name in parent.zillion.siblings:
                    try:
                        sibling = self.metadata.tables[sibling_name]
                    except KeyError:
                        raise AssertionError(
                            "Sibling %s of %s not defined"
                            % (sibling_name, parent.fullname)
                        )

                    raiseifnot(
                        sibling.zillion.type == TableTypes.DIMENSION,
                        "Sibling tables can only be dimension tables",
                    )
                    pk_fields = sibling.zillion.primary_key
                    check_pk_fields(pk_fields, sibling, "sibling")
                    neighbor_tables.append(NeighborTable(sibling, pk_fields))

        return neighbor_tables

    def find_descendent_tables(self, table):
        """Find graph descendents of the table"""
        return nx.descendants(self._graph, table.fullname)

    def get_possible_joins(self, table, grain):
        """This takes a given table (usually a metric table) and tries to find
        one or more joins to each dimension of the grain. It's possible some of
        these joins satisfy other parts of the grain too which leaves room for
        consolidation, but it's also possible to have it generate independent,
        non-overlapping joins to meet the grain.

        **Parameters:**

        * **table** - (*SQLAlchemy Table*) Table to analyze for joins to grain
        * **grain** - (*iterable*) An iterable of dimension names that the given
        table must join to

        **Returns:**

        (*dict*) - A mapping of dimension -> dimension joins

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

        possible_joins = self._consolidate_field_joins(table, grain, possible_dim_joins)
        dbg("possible joins:")
        dbg(possible_joins)
        return possible_joins

    def find_possible_table_sets(
        self, ds_tables_with_field, field, grain, dimension_grain
    ):
        """Find table sets that meet the grain

        **Parameters:**

        * **ds_tables_with_field** - (*list of tables*) A list of datasource
        tables that have the target field
        * **field** - (*str*) The target field we are trying to cover
        * **grain** - (*iterable*) The grain the table set must support
        * **dimension_grain** - The subset of the grain that are requested
        dimensions

        **Returns:**

        (*list*) - A list of TableSets

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
        currently not sufficient to completely rebuild the datasource."""
        # TODO: does this need to store entire config?
        return dict(name=self.name)

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

    def _load_adhoc_tables(self, config):
        """Extract and init the adhoc tables in the DS config. This will return
        a list of processed adhoc tables by name"""
        ds_config_context = get_ds_config_context(self.name)
        adhoc_tables = []

        for table_name, table_config in config.get("tables", {}).items():
            cfg = table_config.copy()
            data_url = cfg.get("data_url", None)
            if not data_url:
                continue

            adhoc_tables.append(table_name)

            # TODO: we currently ignore this with adhoc tables since we
            # don't want to accidentally leak secrets from the DS context.
            # It's allowed for datasource connect urls since those form the
            # entire datasource so it's possible to apply an appropriate
            # context. Here we probably need a way to specify which context
            # is allowed for adhoc tables within each DS context.
            #
            # if get_string_format_args(cfg["data_url"]):
            #     cfg["data_url"] = cfg["data_url"].format(**ds_config_context)

            schema, table_name = get_schema_and_table_name(table_name)
            dt = datatable_from_config(table_name, cfg, schema=schema)
            dt.to_sql(self.metadata.bind)

        return adhoc_tables

    def _apply_table_configs(self, table_configs):
        """Take configs and apply them to the table/column metadata"""

        for table in self.metadata.tables.values():
            if table.fullname not in table_configs:
                continue

            table_config = table_configs[table.fullname]
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

                for conv in convs:
                    field = conv["field"]
                    ds_formula = conv["ds_formula"]
                    ds_criteria_conversions = conv["ds_criteria_conversions"]
                    is_valid_datasource_criteria_conversions(ds_criteria_conversions)
                    field_def = field.copy()
                    field_name = field_def.name

                    if not field_def.description:
                        field_def.description = "Automatic conversion field"

                    if column.zillion.type_conversion_prefix:
                        field_name = column.zillion.type_conversion_prefix + field_name
                        is_valid_field_name(field_name)
                        field_def.name = field_name
                        field_def.display_name = default_field_display_name(field_name)

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
                        dict(
                            name=field_name,
                            ds_formula=ds_formula,
                            ds_criteria_conversions=ds_criteria_conversions,
                        )
                    )

                    if not self.has_field(field_name):
                        if isinstance(field_def, Dimension):
                            self.add_dimension(field_def)
                        else:
                            self.add_metric(field_def)

    def _add_metric_column(self, column, field, aggregation=None, rounding=None):
        """Add a metric to the datasource from a column"""
        if not self.has_metric(field):
            dbg(
                "Adding metric %s from column %s.%s"
                % (field, self.name, column_fullname(column))
            )
            _aggregation, _rounding = infer_aggregation_and_rounding(column)
            aggregation = aggregation.lower() if aggregation != None else _aggregation
            rounding = int(rounding) if rounding != None else _rounding
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

    def _add_metric_table_fields(self, table, nlp=False):
        """Populate fields from a metric table"""

        nlp_table_info = get_nlp_table_info(table) if nlp else {}

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
                nlp_column_info = nlp_table_info.get(column.name, {})
                if is_probably_metric(
                    column, formula=formula, nlp_column_info=nlp_column_info
                ):
                    self._add_metric_column(
                        column,
                        field,
                        aggregation=nlp_column_info.get("aggregation", None),
                        rounding=nlp_column_info.get("rounding", None),
                    )
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

    def _populate_fields(self, config, nlp=False):
        """Populate fields from a datasource config"""
        self._populate_global_fields(config, force=True)

        for table in self.metadata.tables.values():
            if not is_active(table):
                continue
            if table.zillion.type == TableTypes.METRIC:
                self._add_metric_table_fields(table, nlp=nlp)
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

    def _eliminate_redundant_joins(self, sorted_join_fields, main_table):
        """Eliminate joins that aren't providing unique fields or are just table
        supersets of other joins"""
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
                        % (other_join.table_names, other_covered_fields),
                        indent=4,
                    )
                    joins_to_delete.add(other_join)

                # If this join goes through a table that contains the PK of the
                # main table, remove joins that cover the same grain but don't
                # have that PK. This is assumed to lead to more efficient joins
                # and match the intent of the schema.
                if (
                    other_covered_fields == covered_fields
                    and len(join) > 1
                    and len(other_join) > 1
                ):
                    main_pk = main_table.zillion.primary_key
                    join_table_two_pk = self.get_table(
                        join.table_names[1]
                    ).zillion.primary_key
                    other_table_two_pk = self.get_table(
                        other_join.table_names[1]
                    ).zillion.primary_key
                    if join_table_two_pk == main_pk and other_table_two_pk != main_pk:
                        dbg(
                            "Removing redundant join %s / %s"
                            % (other_join.table_names, other_covered_fields),
                            indent=4,
                        )
                        joins_to_delete.add(other_join)

        dbg(f"Removing {len(joins_to_delete)} redundant joins")
        sorted_join_fields = [
            (join, fields)
            for join, fields in sorted_join_fields
            if join not in joins_to_delete
        ]
        return sorted_join_fields

    def _find_join_combinations(self, sorted_join_fields, grain):
        """Find candidate joins that cover the entire grain"""
        candidates = []
        required_joins = []
        optional_joins = []
        max_joins = zillion_config.get("DATASOURCE_MAX_JOINS", None)
        max_candidates = zillion_config.get("DATASOURCE_MAX_JOIN_CANDIDATES", None)

        # First find joins that provide unique components of the grain
        # as they will be required. Do this by looking at the covered fields
        # of the join to see if any are unique among all joins.
        for join, covered_fields in sorted_join_fields:
            unique_covered = covered_fields.copy()
            for other_join, other_covered_fields in sorted_join_fields:
                if join == other_join:
                    continue
                unique_covered -= other_covered_fields
            if unique_covered:
                required_joins.append((join, covered_fields))
            else:
                optional_joins.append((join, covered_fields))

        dbg(f"{len(sorted_join_fields)} total joins, {len(required_joins)} required")
        optional_powerset = powerset(optional_joins, max_combo_len=max_joins)

        for i, optional_join_combo in enumerate(optional_powerset):
            join_combo = list(optional_join_combo) + required_joins

            if not join_combo:
                continue

            if i > 0 and i % 10000 == 0:
                warn(f"{i} join combos and counting, {len(candidates)} candidates...")

            covered = set()
            has_subsets = False
            for join, covered_dims in join_combo:
                covered |= covered_dims
                # If any of the other joins are a subset of this join we
                # ignore it. The powerset has every combination so it will
                # eventually hit the case where there are only distinct joins.
                for other_join, _ in join_combo:
                    if join == other_join:
                        continue
                    if other_join.table_names.issubset(join.table_names):
                        has_subsets = True
                        break
                if has_subsets:
                    break

            if has_subsets:
                continue

            if len(covered) != len(grain):
                # dbg(f"does not cover grain: {covered} / {grain}")
                continue

            # This combination of joins covers the entire grain. Add it as
            # a candidate if there isn't an existing candidate that is a
            # subset of these joins
            skip = False
            joins = {x[0] for x in join_combo}
            for other_join_combo in candidates:
                other_joins = {x[0] for x in other_join_combo}
                if other_joins.issubset(joins):
                    skip = True
                    break

            if skip:
                # dbg("Skipping subset join list combination")
                continue

            # dbg("Adding join combo")
            candidates.append(join_combo)
            if max_candidates and len(candidates) >= max_candidates:
                warn(f"Hit max join candidates, returning {len(candidates)}")
                break

        return candidates

    def _combine_orthogonal_joins(self, candidates):
        """Combine any joins that orthogonally stem from a common table.

        NOTE: this needs more thorough review, and there may be a better way
        to handle this or combine joins at a different step in the process.
        """

        result = []
        for join_combo in candidates:
            root_joins = {}

            for join, covered_fields in join_combo:
                root_table = join.table_names[0]
                if not root_table in root_joins:
                    root_joins[root_table] = (join, covered_fields)
                else:
                    existing_join, existing_fields = root_joins[root_table]
                    dbg(
                        f"Combining join {join.table_names} into {existing_join.table_names}"
                    )
                    new_join = Join.combine(existing_join, join)
                    new_fields = existing_fields | covered_fields
                    root_joins[root_table] = (new_join, new_fields)

            final_combo = tuple([x for x in root_joins.values()])
            for row in final_combo:
                dbg(f"Final combo join: {row[0].table_names}")
            result.append(final_combo)

        return result

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
            dbg(f"join {join.table_names} covers {covered_fields}")
            join.add_fields(covered_fields)
        return join_fields

    def _consolidate_field_joins(self, table, grain, field_joins):
        """This takes a mapping of fields to joins that satisfy each field and
        returns a minimized map of joins to fields satisfied by that join."""

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

        sorted_join_fields = self._eliminate_redundant_joins(sorted_join_fields, table)
        candidates = self._find_join_combinations(sorted_join_fields, grain)
        candidates = self._combine_orthogonal_joins(candidates)
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
    def from_db_file(
        cls,
        file,
        name=None,
        config=None,
        if_exists=IfFileExistsModes.FAIL,
        replace_after=DEFAULT_REPLACE_AFTER,
        nlp=False,
    ):
        """Create a DataSource from a sqlite db file path or url

        **Parameters:**

        * **file** - (*str*) A file path/url pointing to a SQLite database
        * **name** - (*str, optional*) The name to give the datasource. If blank it
        will be derived from the file name.
        * **config** - (*dict, optional*) A DataSourceConfigSchema dict config.
        Note that the connect param of this config will be overwritten if
        present!
        * **if_exists** - (*str, optional*) Control behavior when the database
        already exists
        * **replace_after** - (*str, optional*) Replace the data file after this
        interval if `if_exists` is "replace_after". See `url_connect` docs for
        more information.
        * **nlp** - (*bool, optional*) If true, allow NLP analysis when creating
        fields

        **Returns:**

        (*DataSource*) - A DataSource created from the db and config

        """
        config = (config or {}).copy()
        connect = config.get("connect", {})
        if connect:
            connect = {}
            warn("Overwriting datasource connect settings for from_db_file()")
        connect["func"] = "zillion.datasource.url_connect"

        if os.path.isfile(file):
            url = "sqlite:///" + os.path.abspath(file)
            connect["params"] = dict(connect_url=url, if_exists=if_exists)
        else:
            connect["params"] = dict(data_url=file, if_exists=if_exists)
            if replace_after:
                connect["params"]["replace_after"] = replace_after

        config["connect"] = connect

        name = name or entity_name_from_file(file)
        return cls(name, config=config, nlp=nlp)

    @classmethod
    def from_datatables(cls, name, datatables, config=None, nlp=False):
        """Create a DataSource from a list of datatables

        **Parameters:**

        * **name** - (*str*) The name to give the datasource
        * **datatables** - (*list of AdHocDataTables*) A list of AdHocDataTables
        to use to create the DataSource
        * **config** - (*dict, optional*) A DataSourceConfigSchema dict config
        * **nlp** - (*bool, optional*) If true, allow NLP analysis when creating
        fields

        **Returns:**

        (*DataSource*) - A DataSource created from the datatables and config

        """
        config = config or dict(tables={})
        ds_name = cls._check_or_create_name(name)

        if config.get("connect", None):
            metadata = metadata_from_connect(config["connect"], name)
            engine = metadata.bind
            del config["connect"]  # will pass metadata directly
        else:
            # No connection URL specified, let's create an adhoc SQLite DB
            conn_url = get_adhoc_datasource_url(ds_name)
            engine = sa.create_engine(
                conn_url,
                echo=False,
                pool_pre_ping=True,
                **get_engine_extra_kwargs(conn_url),
            )
            metadata = sa.MetaData()
            metadata.bind = engine

        for dt in datatables:
            dt.to_sql(engine)
            config.setdefault("tables", {})[dt.fullname] = dt.table_config

        reflect_metadata(metadata)
        return cls(ds_name, metadata=metadata, config=config, nlp=nlp)

    @classmethod
    def from_data_file(
        cls,
        file,
        primary_key,
        ds_name=None,
        schema="main",
        table_name=None,
        # TODO infer table type from columns if not provided
        table_type="metric",
        nlp=False,
        **kwargs,
    ):
        """Create a DataSource from a data file path/url that represents a single table

        **Parameters:**

        * **file** - (*str*) A file path/url pointing to a data file. If remote the
        file will be downloaded to the adhoc data directory.
        * **primary_key** - (*list of str*)  A list of fields representing the
        primary key of the table
        * **ds_name** - (*str, optional*) The name to give the datasource
        * **schema** - (*str, optional*) The schema to create the table in
        * **table_name** - (*str, optional*) The name to give the table. If blank it will be
        derived from the file name.
        * **table_type** - (*str, optional*) Specifies the TableType
        * **kwargs** - (*dict, optional*) Additional keyword arguments to pass to
        the the AdHocDataTable
        * **nlp** - (*bool, optional*) If true, allow NLP analysis when creating
        fields

        **Returns:**

        (*DataSource*) - A DataSource created from the file data and config

        """
        raiseifnot(schema, "Schema must be specified")
        table_name = table_name or table_safe_name(entity_name_from_file(file))
        table_config = dict(
            data_url=file,
            type=table_type,
            primary_key=primary_key,
            **kwargs,
        )
        dt = datatable_from_config(table_name, table_config, schema=schema)
        return cls.from_datatables(ds_name, [dt], nlp=nlp)

    @classmethod
    def _check_or_create_name(cls, name):
        """Validate the datasource name or create one if necessary"""
        if not name:
            datestr = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            name = "zillion_ds_%s_%s" % (datestr, random.randint(0, 1e9))
            return name
        raiseifnot(
            set(name) <= DATASOURCE_NAME_ALLOWED_CHARS,
            (
                'DataSource name "%s" has invalid characters. Allowed: %s'
                % (name, DATASOURCE_NAME_ALLOWED_CHARS_STR)
            ),
        )
        return name


class AdHocDataTable(PrintMixin):
    """Create an adhoc table from raw data that can be added to a DataSource

    **Parameters:**

    * **name** - (*str*) The name of the table
    * **data** - (*iterable, DataFrame, or str*) The data to create the adhoc
    table from
    * **table_type** - (*str*) Specify the TableType
    * **columns** - (*dict, optional*) Column configuration for the table
    * **primary_key** - (*list of str, optional*) A list of fields that make
    up the primary key of the table
    * **parent** - (*str, optional*) A reference to a parent table in the
    same datasource
    * **if_exists** - (*str, optional*) Control behavior when datatables
    already exist in the database
    * **drop_dupes** - (*bool, optional*) Drop duplicate primary key rows when
    loading the table
    * **convert_types** - (*dict, optional*) A mapping of column names to types
    to convert to when loading the table. The types must be strings representing
    valid sqlalchemy types. Ex: {"col1": "date", "col2": "integer"}
    * **fillna_value** - (*str or int, optional*) Fill null values in primary
    key columns with this value before writing to a SQL database.
    * **schema** - (*str, optional*) The schema in which the table resides
    * **kwargs** - Keyword arguments passed to
    pandas.DataFrame.from_records if a DataFrame is created from iterable
    data

    """

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
        if_exists=IfExistsModes.FAIL,
        drop_dupes=False,
        convert_types=None,
        fillna_value="",
        use_full_column_names=False,
        schema=None,
        **kwargs,
    ):
        """Initializes the datatable by parsing its config, but does not
        actually add it to a particular DB yet. It is assumed the DataSource
        will do that later.
        """
        self.df_kwargs = kwargs or {}
        self.fillna_value = fillna_value or ""

        self.primary_key_columns = None
        if primary_key:
            if not columns:
                # Assume specified primary key fields match column names
                self.primary_key_columns = self.primary_key
            else:
                pk_columns = []
                for pk in self.primary_key:
                    found = False
                    for column_name, column_config in columns.items():
                        if pk in column_config.get("fields", []):
                            pk_columns.append(column_name)
                            found = True
                            break
                    if not found:
                        pk_columns.append(pk)
                self.primary_key_columns = pk_columns

        self.table_config = TableConfigSchema().load(
            dict(
                type=table_type,
                columns=self.columns,
                create_fields=True,
                parent=parent,
                if_exists=if_exists,
                drop_dupes=drop_dupes,
                convert_types=convert_types,
                primary_key=primary_key,
                use_full_column_names=use_full_column_names,
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
        return pd.DataFrame.from_records(
            self.data, index=self.primary_key_columns, **kwargs
        )

    def table_exists(self, engine):
        """Determine if this table exists"""
        return engine.has_table(self.name)

    def to_sql(self, engine, method="multi", chunksize=int(1e3)):
        """Use pandas to push the adhoc table data to a SQL database.

        **Parameters:**

        * **engine** - (*SQLAlchemy connection engine*) The engine used to
        connect to the database
        * **method** - (*str, optional*) Passed through to pandas
        * **chunksize** - (*int, optional*) Passed through to pandas

        """
        if_exists = self.if_exists
        if if_exists == IfExistsModes.IGNORE:
            if self.table_exists(engine):
                return
            # Pandas doesn't actually have an "ignore" option, but switching
            # to fail will work because the table *should* not exist.
            if_exists = IfExistsModes.FAIL

        df = self.get_dataframe()

        if self.primary_key_columns:
            if isinstance(df.index, pd.MultiIndex):
                df.index = pd.MultiIndex.from_frame(
                    df.index.to_frame().fillna(self.fillna_value)
                )
            else:
                df.index = df.index.fillna(self.fillna_value)

        if (not df.empty) and self.drop_dupes:
            orig_len = len(df)
            df = df[~df.reset_index().duplicated(subset=self.primary_key).values]
            if len(df) != orig_len:
                warn(
                    "DataFrame size dropped from %s to %s due to duplicates"
                    % (orig_len, len(df))
                )

        dtype = None
        if self.convert_types:
            dtype = {
                k: type_string_to_sa_type(v) for k, v in self.convert_types.items()
            }

        # Note: we are doing this instead of df.to_sql since to_sql doesn't
        # support creating primary keys on table creation via the keys= param.
        # To prevent unnecessary indexes from being created we also reset_index
        # and set index=False.
        table = SQLTable(
            self.name,
            pandasSQL_builder(engine, schema=self.schema),
            frame=df.reset_index(),
            index=False,
            keys=df.index.names,
            if_exists=if_exists,
            schema=self.schema,
            dtype=dtype,
        )
        table.create()
        # Note: this hits limits in allowed sqlite params if chunks are too large
        table.insert(chunksize, method=method)


class SQLiteDataTable(AdHocDataTable):
    """AdHocDataTable from an existing sqlite database on the local filesystem

    Note: the "data" param to AdHocDataTable is ignored. This is simply a
    workaround to get an AdHocDataTable reference for an existing SQLite DB
    without having to recreate anything from data.

    """

    def get_dataframe(self):
        raise NotImplementedError

    def to_sql(self, engine, **kwargs):
        raiseifnot(self.table_exists(engine), "SQLiteDataTable table does not exist")


class CSVDataTable(AdHocDataTable):
    """AdHocDataTable from a JSON file using pandas.read_csv"""

    def get_dataframe(self):
        info("Reading CSV from %s" % self.data)
        return pd.read_csv(
            self.data,
            index_col=self.primary_key_columns,
            usecols=list(self.columns.keys()) if self.columns else None,
            **self.df_kwargs,
        )


class ExcelDataTable(AdHocDataTable):
    """AdHocDataTable from a JSON file using pandas.read_excel"""

    def get_dataframe(self):
        info("Reading Excel File from %s" % self.data)
        df = pd.read_excel(
            self.data,
            usecols=list(self.columns.keys()) if self.columns else None,
            **self.df_kwargs,
        )
        if self.primary_key_columns and df.index.names != self.primary_key_columns:
            df = df.set_index(self.primary_key_columns)
        return df


class JSONDataTable(AdHocDataTable):
    """AdHocDataTable from a JSON file using pandas.read_json"""

    def get_dataframe(self, orient="table"):
        info("Reading JSON from %s" % self.data)
        df = pd.read_json(self.data, orient=orient, **self.df_kwargs)
        if self.primary_key_columns and df.index.names != self.primary_key_columns:
            df = df.set_index(self.primary_key_columns)
        return df


class HTMLDataTable(AdHocDataTable):
    """AdHocDataTable from an html table using pandas.read_html. By default it
    expects a table in the same format as produced by:
    `df.reset_index().to_html("table.html", index=False)`"""

    def get_dataframe(self):
        info("Reading HTML from %s" % self.data)
        dfs = pd.read_html(self.data, **self.df_kwargs)
        raiseifnot(dfs, "No html table found")
        raiseifnot(len(dfs) == 1, "More than one html table found")
        df = dfs[0]
        if self.primary_key_columns and df.index.names != self.primary_key_columns:
            df = df.set_index(self.primary_key_columns)
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

        info("Reading Google Sheets CSV from %s" % url)
        return pd.read_csv(
            url,
            index_col=self.primary_key_columns,
            usecols=list(self.columns.keys()) if self.columns else None,
            **self.df_kwargs,
        )


def datatable_from_config(name, config, schema=None, **kwargs):
    """Factory to create an AdHocDataTable from a given config. The type of the
    AdHocDataTable created will be inferred from the config["url"] param.

    **Parameters:**

    * **name** - (*str*) The name of the table
    * **config** - (*dict*) The configuration of the table
    * **schema** - (*str, optional*) The schema in which the table resides
    * **kwargs** - Passed to init of the particular AdHocDataTable class

    **Returns:**

    (*AdHocDataTable*) - Return the created AdHocDataTable (subclass)

    """
    url = config["data_url"]
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
    else:
        raise AssertionError("Unrecognized data url extension: %s" % url)

    kwargs.update(config.get("adhoc_table_options", {}))

    return cls(
        name,
        url,
        config["type"],
        config.get("columns", None),
        primary_key=config.get("primary_key", None),
        parent=config.get("parent", None),
        if_exists=config.get("if_exists", IfExistsModes.FAIL),
        drop_dupes=config.get("drop_dupes", False),
        convert_types=config.get("convert_types", None),
        fillna_value=config.get("fillna_value", None),
        use_full_column_names=config.get("use_full_column_names", False),
        schema=schema,
        **kwargs,
    )
