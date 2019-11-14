from collections import defaultdict, OrderedDict
import copy
import datetime
import logging
import random
import time

import networkx as nx
from orderedset import OrderedSet
import pandas as pd
import sqlalchemy as sa
from tlbx import (
    dbg,
    warn,
    st,
    rmfile,
    initializer,
    get_string_format_args,
    iter_or,
    powerset,
    PrintMixin,
    MappingMixin,
)

from zillion.configs import (
    AdHocFieldSchema,
    AdHocMetricSchema,
    ColumnInfoSchema,
    TableInfoSchema,
    MetricConfigSchema,
    TechnicalInfoSchema,
    DimensionConfigSchema,
    is_valid_field_name,
    zillion_config,
)
from zillion.core import (
    DATASOURCE_ALLOWABLE_CHARS,
    UnsupportedGrainException,
    InvalidFieldException,
    MaxFormulaDepthException,
    AggregationTypes,
    TableTypes,
    parse_technical_string,
    field_safe_name,
)
from zillion.report import Report
from zillion.sql_utils import (
    infer_aggregation_and_rounding,
    aggregation_to_sqla_func,
    contains_aggregation,
    type_string_to_sa_type,
    is_probably_metric,
    sqla_compile,
    get_dialect_type_conversions,
    column_fullname,
)

if zillion_config["DEBUG"]:
    logging.getLogger().setLevel(logging.DEBUG)

MAX_FORMULA_DEPTH = 3


class DataSource(PrintMixin):
    repr_attrs = ["name"]

    @initializer
    def __init__(self, name, url_or_metadata, reflect=False):
        self.name = DataSource.check_or_create_name(name)

        if isinstance(url_or_metadata, str):
            self.metadata = sa.MetaData()
            self.metadata.bind = sa.create_engine(url_or_metadata)
        else:
            assert isinstance(url_or_metadata, sa.MetaData), (
                "Invalid URL or MetaData object: %s" % url_or_metadata
            )
            self.metadata = url_or_metadata
            assert (
                self.metadata.bind
            ), "MetaData object must have a bind (engine) attribute specified"

        if reflect:
            self.metadata.reflect()

    def get_dialect_name(self):
        return self.metadata.bind.dialect.name

    def get_params(self):
        return dict(
            name=self.name, url=str(self.metadata.bind.url), reflect=self.reflect
        )

    @classmethod
    def check_or_create_name(cls, name):
        if not name:
            datestr = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            name = "zillion_ds_%s_%s" % (datestr, random.randint(0, 1e9))
            return name
        assert set(name) <= DATASOURCE_ALLOWABLE_CHARS, (
            'DataSource name "%s" has invalid characters. Allowed: %s'
            % (name, DATASOURCE_ALLOWABLE_CHARS)
        )
        return name


class AdHocDataTable(PrintMixin):
    repr_attrs = ["name", "type", "primary_key"]

    @initializer
    def __init__(
        self, name, type, primary_key, data, columns=None, parent=None, autocolumns=True
    ):
        self.columns = columns or {}
        self.column_names = self.columns.keys() or None


class AdHocDataSource(DataSource):
    def __init__(self, datatables, name=None, coerce_float=True, if_exists="fail"):
        self.table_configs = {}
        ds_name = self.check_or_create_name(name)
        conn_url = "sqlite:///%s" % self.get_datasource_filename(ds_name)
        engine = sa.create_engine(conn_url, echo=False)
        for dt in datatables:
            df = pd.DataFrame.from_records(
                dt.data,
                dt.primary_key,
                columns=dt.column_names,
                coerce_float=coerce_float,
            )
            size = int(
                1e3
            )  # This hits limits in allowed sqlite params if chunks are too large
            df.to_sql(
                dt.name, engine, if_exists=if_exists, method="multi", chunksize=size
            )
            self.table_configs[dt.name] = dict(
                type=dt.type,
                parent=dt.parent,
                columns=dt.columns,
                autocolumns=dt.autocolumns,
            )
        super(AdHocDataSource, self).__init__(ds_name, conn_url, reflect=True)

    def get_datasource_filename(self, ds_name):
        return "%s/%s.db" % (zillion_config["ADHOC_DATASOURCE_DIRECTORY"], ds_name)

    def clean_up(self):
        filename = self.get_datasource_filename(self.name)
        dbg("Removing %s" % filename)
        rmfile(filename)


class ZillionInfo(MappingMixin):
    schema = None

    @initializer
    def __init__(self, **kwargs):
        assert self.schema, "ZillionInfo subclass must have a schema defined"
        self.schema().load(self)

    @classmethod
    def create(cls, zillion_info):
        if isinstance(zillion_info, cls):
            return zillion_info
        assert isinstance(zillion_info, dict), (
            "Raw info must be a dict: %s" % zillion_info
        )
        zillion_info = cls.schema().load(zillion_info)
        return cls(**zillion_info)


class TableInfo(ZillionInfo, PrintMixin):
    repr_attrs = ["type", "active", "autocolumns", "parent"]
    schema = TableInfoSchema


class ColumnInfo(ZillionInfo, PrintMixin):
    repr_attrs = ["fields", "active"]
    schema = ColumnInfoSchema

    def __init__(self, **kwargs):
        super(ColumnInfo, self).__init__(**kwargs)
        # XXX: field_map could get out of sync if fields is edited.  Is there
        # a way to enforce using add_field, other than storing the values as
        # _fields instead?
        self.field_map = OrderedDict()
        for field in self.fields:
            self.add_field_to_map(field)

    def add_field_to_map(self, field):
        if isinstance(field, str):
            self.field_map[field] = None
        else:
            self.field_map[field["name"]] = field

    def add_field(self, field):
        self.fields.append(field)
        self.add_field_to_map(field)

    def get_field_names(self):
        return self.field_map.keys()


class TableSet(PrintMixin):
    repr_attrs = ["ds_name", "join", "grain", "target_fields"]

    @initializer
    def __init__(self, ds_name, ds_table, join, grain, target_fields):
        pass

    def get_covered_metrics(self, warehouse):
        covered_metrics = get_table_metrics(warehouse, self.ds_table)
        return covered_metrics

    def get_covered_fields(self):
        covered_fields = get_table_fields(self.ds_table)
        return covered_fields

    def __len__(self):
        if not self.join:
            return 1
        return len(self.join.table_names)


class NeighborTable(PrintMixin):
    repr_attrs = ["table_name", "join_fields"]

    @initializer
    def __init__(self, table, join_fields):
        self.table_name = table.fullname


class JoinPart(PrintMixin):
    repr_attrs = ["ds_name", "table_names", "join_fields"]

    @initializer
    def __init__(self, ds_name, table_names, join_fields):
        pass


class Join(PrintMixin):
    """
    Join is a group of join parts that would be used together.
    field_map represents the requested fields this join list is meant to satisfy
    """

    repr_attrs = ["table_names", "field_map"]

    @initializer
    def __init__(self, join_parts, field_map):
        self.table_names = OrderedSet()
        self.ds_name = None
        for join_part in self.join_parts:
            if not self.ds_name:
                self.ds_name = join_part.ds_name
            else:
                assert join_part.ds_name == self.ds_name, (
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

    # TODO: should this perhaps be storing the ds_name in the first place?
    def get_covered_fields(self, warehouse):
        """Generate a list of all possible fields this can cover"""
        fields = set()
        for table_name in self.table_names:
            table = warehouse.tables[self.ds_name][table_name]
            covered_fields = get_table_fields(table)
            fields = fields | covered_fields
        return fields

    def add_field(self, warehouse, field):
        assert field not in self.field_map, "Field %s is already in field map" % field
        for table_name in self.table_names:
            table = warehouse.tables[self.ds_name][table_name]
            covered_fields = get_table_fields(table)
            if field in covered_fields:
                column = get_table_field_column(table, field)
                self.field_map[field] = column
                return
        assert False, "Field %s is not in any join tables: %s" % (
            field,
            self.table_names,
        )

    def add_fields(self, warehouse, fields):
        for field in fields:
            if field not in self.field_map:
                self.add_field(warehouse, field)


def joins_from_path(ds_name, ds_graph, path, field_map=None):
    join_parts = []
    if len(path) == 1:
        # A placeholder join that is really just a single table
        join_part = JoinPart(ds_name, path, None)
        join_parts.append(join_part)
    else:
        for i, node in enumerate(path):
            if i == (len(path) - 1):
                break
            start, end = path[i], path[i + 1]
            edge = ds_graph.edges[start, end]
            join_part = JoinPart(ds_name, [start, end], edge["join_fields"])
            join_parts.append(join_part)
    return Join(join_parts, field_map=field_map)


def get_table_fields(table):
    fields = set()
    for col in table.c:
        for field in col.zillion.get_field_names():
            fields.add(field)
    return fields


def get_table_field_column(table, field_name):
    for col in table.c:
        for field in col.zillion.get_field_names():
            if field == field_name:
                return col
    assert False, "Field %s is not present in table %s" % (field_name, table.fullname)


def get_table_metrics(warehouse, table):
    metrics = set()
    for col in table.c:
        for field in col.zillion.get_field_names():
            if field in warehouse.metrics:
                metrics.add(field)
    return metrics


def get_table_dimensions(warehouse, table):
    dims = set()
    for col in table.c:
        for field in col.zillion.get_field_names():
            if field in warehouse.dimensions:
                dims.add(field)
    return dims


class Technical(MappingMixin, PrintMixin):
    repr_attrs = ["type", "window", "min_periods"]

    @initializer
    def __init__(self, **kwargs):
        pass

    @classmethod
    def create(cls, info):
        if isinstance(info, cls):
            return info
        if isinstance(info, str):
            info = parse_technical_string(info)
        assert isinstance(info, dict), "Raw info must be a dict: %s" % info
        info = TechnicalInfoSchema().load(info)
        return cls(**info)


class Field(PrintMixin):
    repr_attrs = ["name"]
    ifnull_value = zillion_config["IFNULL_PRETTY_VALUE"]

    @initializer
    def __init__(self, name, type, **kwargs):
        is_valid_field_name(name)
        self.column_map = defaultdict(list)
        if isinstance(type, str):
            self.type = type_string_to_sa_type(type)

    def add_column(self, ds, column):
        current_cols = [column_fullname(col) for col in self.column_map[ds.name]]
        fullname = column_fullname(column)
        if fullname in current_cols:
            warn(
                "Column %s.%s is already mapped to field %s"
                % (ds.name, fullname, self.name)
            )
            return
        self.column_map[ds.name].append(column)

    def remove_column(self, ds, column):
        self.column_map[ds.name].remove(column)

    def get_columns(self, ds_name):
        return self.column_map[ds_name]

    def get_column_names(self, ds_name):
        return [column_fullname(x) for x in self.column_map[ds_name]]

    def remove_datasource(self, ds):
        del self.column_map[ds.name]

    def get_formula_fields(self, warehouse, depth=0):
        return []

    def get_ds_expression(self, column):
        ds_formula = (
            column.zillion.field_map[self.name].get("ds_formula", None)
            if column.zillion.field_map[self.name]
            else None
        )
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


class Metric(Field):
    def __init__(
        self,
        name,
        type,
        aggregation=AggregationTypes.SUM,
        rounding=None,
        weighting_metric=None,
        technical=None,
        **kwargs
    ):
        if weighting_metric:
            assert aggregation == AggregationTypes.AVG, (
                'Weighting metrics are only supported for "%s" aggregation type'
                % AggregationTypes.AVG
            )

        if technical:
            technical = Technical.create(technical)

        super(Metric, self).__init__(
            name,
            type,
            aggregation=aggregation,
            rounding=rounding,
            weighting_metric=weighting_metric,
            technical=technical,
            **kwargs
        )

    def add_column(self, ds, column):
        super(Metric, self).add_column(ds, column)
        if self.weighting_metric:
            for col in column.table.c:
                if self.weighting_metric in col.zillion.get_field_names():
                    return
            assert False, (
                'Metric "%s" requires weighting_metric "%s" but it is missing from table for column %s'
                % (self.name, self.weighting_metric, column_fullname(column))
            )

    def get_ds_expression(self, column):
        expr = column
        aggr = aggregation_to_sqla_func(self.aggregation)
        skip_aggr = False

        ds_formula = (
            column.zillion.field_map[self.name].get("ds_formula", None)
            if column.zillion.field_map[self.name]
            else None
        )
        if ds_formula:
            if contains_aggregation(ds_formula):
                warn("Datasource formula contains aggregation, skipping default logic!")
                skip_aggr = True
            expr = sa.literal_column(ds_formula)

        if not skip_aggr:
            if self.aggregation in [
                AggregationTypes.COUNT,
                AggregationTypes.COUNT_DISTINCT,
            ]:
                if self.rounding:
                    warn("Ignoring rounding for count field: %s" % self.name)
                return aggr(expr).label(self.name)

            if self.weighting_metric:
                w_column = get_table_field_column(column.table, self.weighting_metric)
                w_column_name = column_fullname(w_column)
                # NOTE: 1.0 multiplication is a hack to ensure results are not rounded
                # to integer values improperly by some database dialects such as sqlite
                expr = sa.func.sum(
                    sa.text("1.0") * expr * sa.text(w_column_name)
                ) / sa.func.sum(sa.text(w_column_name))
            else:
                expr = aggr(expr)

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
            if isinstance(field, FormulaMetric):
                try:
                    sub_fields, sub_formula = field.get_formula_fields(
                        warehouse, depth=depth + 1
                    )
                except MaxFormulaDepthException:
                    if depth != 0:
                        raise
                    raise MaxFormulaDepthException(
                        "Maximum formula recursion depth exceeded for %s: %s"
                        % (self.name, self.formula)
                    )
                for sub_field in sub_fields:
                    raw_fields.add(sub_field)
                field_formula_map[field_name] = "(" + sub_formula + ")"
            else:
                field_formula_map[field_name] = "{" + field_name + "}"
                raw_fields.add(field_name)

        raw_formula = self.formula.format(**field_formula_map)
        return raw_fields, raw_formula

    def check_formula_fields(self, warehouse):
        fields, _ = self.get_formula_fields(warehouse)
        for field in fields:
            warehouse.get_field(field)

    def get_ds_expression(self, column):
        assert False, "Formula-based Fields do not support get_ds_expression"

    def get_final_select_clause(self, warehouse):
        formula_fields, raw_formula = self.get_formula_fields(warehouse)
        format_args = {k: k for k in formula_fields}
        clause = sa.text(raw_formula.format(**format_args))
        return sqla_compile(clause)


class FormulaMetric(FormulaField):
    repr_atts = ["name", "formula", "technical"]

    def __init__(
        self,
        name,
        formula,
        aggregation=AggregationTypes.SUM,
        rounding=None,
        weighting_metric=None,
        technical=None,
        **kwargs
    ):
        if technical:
            technical = Technical.create(technical)

        super(FormulaMetric, self).__init__(
            name,
            formula,
            aggregation=aggregation,
            rounding=rounding,
            weighting_metric=weighting_metric,
            technical=technical,
            **kwargs
        )

    def get_final_select_clause(self, warehouse):
        formula_fields, raw_formula = self.get_formula_fields(warehouse)
        format_args = {k: k for k in formula_fields}
        clause = sa.text(raw_formula.format(**format_args))
        return sqla_compile(clause)


class AdHocField(FormulaField):
    @classmethod
    def create(cls, obj):
        schema = AdHocFieldSchema()
        field_def = schema.load(obj)
        return cls(field_def["name"], field_def["formula"])


class AdHocMetric(FormulaMetric):
    def __init__(self, name, formula, technical=None, rounding=None):
        super(AdHocMetric, self).__init__(
            name, formula, technical=technical, rounding=rounding
        )

    @classmethod
    def create(cls, obj):
        schema = AdHocMetricSchema()
        field_def = schema.load(obj)
        return cls(
            field_def["name"],
            field_def["formula"],
            technical=field_def["technical"],
            rounding=field_def["rounding"],
        )


class AdHocDimension(AdHocField):
    pass


def create_metric(metric_def):
    if metric_def["formula"]:
        metric = FormulaMetric(
            metric_def["name"],
            metric_def["formula"],
            aggregation=metric_def["aggregation"],
            rounding=metric_def["rounding"],
            weighting_metric=metric_def["weighting_metric"],
            technical=metric_def["technical"],
        )
    else:
        metric = Metric(
            metric_def["name"],
            metric_def["type"],
            aggregation=metric_def["aggregation"],
            rounding=metric_def["rounding"],
            weighting_metric=metric_def["weighting_metric"],
            technical=metric_def["technical"],
        )
    return metric


def create_dimension(dim_def):
    dim = Dimension(dim_def["name"], dim_def["type"])
    return dim


class Warehouse:
    def __init__(self, datasources, config=None, ds_priority=None):
        self.datasources = datasources
        # Note: if no ds_priority is established the datasource chosen may
        # change from report to report if multiple can satisfy a query
        self.ds_priority = ds_priority
        if ds_priority:
            ds_names = {ds.name for ds in datasources}
            assert isinstance(ds_priority, list), (
                "Invalid format for ds_priority, must be list of datesource names: %s"
                % ds_priority
            )
            for ds_name in ds_priority:
                assert ds_name in ds_names, (
                    "Datasource %s is in ds_priority but not in datasource map"
                    % ds_name
                )
        self.tables = defaultdict(dict)
        self.metric_tables = defaultdict(dict)
        self.dimension_tables = defaultdict(dict)
        self.metrics = defaultdict(dict)
        self.dimensions = defaultdict(dict)
        self.table_field_map = defaultdict(dict)
        self.ds_graphs = {}
        self.supported_dimension_cache = {}

        if config:
            self.apply_config(config)

        for ds in datasources:
            self.add_datasource(ds)

    def __repr__(self):
        return "Datasources: %s" % (self.ds_graphs.keys())

    def get_datasource_names(self):
        return self.ds_graphs.keys()

    def get_adhoc_datasources(self):
        adhoc_datasources = []
        for ds in self.datasources:
            if isinstance(ds, AdHocDataSource):
                adhoc_datasources.append(ds)
        return adhoc_datasources

    def get_datasource(self, name):
        for ds in self.datasources:
            if ds.name == name:
                return ds
        assert False, 'Could not find datasource with name "%s"' % name

    def add_datasource_tables(self, ds):
        for table in ds.metadata.tables.values():
            self.add_table(ds, table)

    def remove_datasource_tables(self, ds):
        for table in ds.metadata.tables.values():
            self.remove_table(ds, table)
        del self.tables[ds.name]
        if ds.name in self.metric_tables:
            del self.metric_tables[ds.name]
        if ds.name in self.dimension_tables:
            del self.dimension_tables[ds.name]

    def add_datasource(self, ds):
        dbg("Adding datasource %s" % ds.name)
        self.ensure_metadata_info(ds)
        self.populate_conversion_fields(ds)
        self.populate_table_field_map(ds)
        self.add_datasource_tables(ds)
        self.add_ds_graph(ds)

    def remove_datasource(self, ds):
        dbg("Removing datasource %s" % ds.name)
        self.remove_table_field_map(ds)
        self.remove_datasource_tables(ds)
        self.remove_ds_graph(ds)

    def add_adhoc_datasources(self, adhoc_datasources):
        for adhoc_ds in adhoc_datasources:
            self.apply_adhoc_config(adhoc_ds)
            self.add_datasource(adhoc_ds)
            self.datasources.append(adhoc_ds)

    def remove_adhoc_datasources(self, adhoc_datasources):
        for adhoc_ds in adhoc_datasources:
            self.remove_datasource(adhoc_ds)
            self.datasources.remove(adhoc_ds)
            adhoc_ds.clean_up()

    def ensure_metadata_info(self, ds):
        """Ensure that all zillion info are of proper type"""
        for table in ds.metadata.tables.values():
            zillion_info = table.info.get("zillion", None)
            if not zillion_info:
                setattr(table, "zillion", None)
                continue

            table.info["zillion"] = TableInfo.create(zillion_info)
            setattr(table, "zillion", table.info["zillion"])

            for column in table.c:
                zillion_info = column.info.get("zillion", None)
                if not zillion_info:
                    if not table.info["zillion"].autocolumns:
                        setattr(column, "zillion", None)
                        continue
                    else:
                        zillion_info = {}
                zillion_info["fields"] = zillion_info.get(
                    "fields", [field_safe_name(column_fullname(column))]
                )
                column.info["zillion"] = ColumnInfo.create(zillion_info)
                setattr(column, "zillion", column.info["zillion"])

                if column.primary_key:
                    dim_count = 0
                    for field in column.zillion.get_field_names():
                        if field in self.dimensions or field not in self.metrics:
                            dim_count += 1
                        assert dim_count < 2, (
                            "Primary key column may only map to a single dimension: %s"
                            % column
                        )

    def apply_global_config(self, config):
        formula_metrics = []

        for metric_def in config.get("metrics", []):
            if isinstance(metric_def, dict):
                schema = MetricConfigSchema()
                metric_def = schema.load(metric_def)
                metric = create_metric(metric_def)
            else:
                assert isinstance(
                    metric_def, Metric
                ), "Metric definition must be a dict-like object or a Metric object"
                metric = metric_def

            if isinstance(metric, FormulaMetric):
                # These get added later
                formula_metrics.append(metric)
            else:
                self.add_metric(metric)

        for dim_def in config.get("dimensions", []):
            if isinstance(dim_def, dict):
                schema = DimensionConfigSchema()
                dim_def = schema.load(dim_def)
                dim = create_dimension(dim_def)
            else:
                assert isinstance(
                    dim_def, Dimension
                ), "Dimension definition must be a dict-like object or a Dimension object"
                dim = dim_def
            self.add_dimension(dim)

        # Defer formula metrics so params can be checked against existing fields
        for metric in formula_metrics:
            metric.check_formula_fields(self)
            self.add_metric(metric)

    def apply_datasource_config(self, ds_config, ds):
        for table in ds.metadata.tables.values():
            if table.fullname not in ds_config["tables"]:
                continue

            table_config = copy.deepcopy(ds_config["tables"][table.fullname])
            column_configs = table_config.get("columns", None)
            if "columns" in table_config:
                del table_config["columns"]

            zillion_info = table.info.get("zillion", {})
            # Config takes precendence over values on table objects
            zillion_info.update(table_config)
            table.info["zillion"] = TableInfo.create(zillion_info)

            autocolumns = table.info["zillion"].autocolumns
            if not autocolumns:
                assert column_configs, (
                    "Table %s.%s has autocolumns=False and no column configs"
                    % (ds.name, table.fullname)
                )
            if not column_configs:
                continue

            for column in table.columns:
                if column.name not in column_configs:
                    continue

                column_config = column_configs[column.name]
                zillion_info = column.info.get("zillion", {})
                # Config takes precendence over values on column objects
                zillion_info.update(column_config)
                zillion_info["fields"] = zillion_info.get(
                    "fields", [field_safe_name(column_fullname(column))]
                )
                column.info["zillion"] = ColumnInfo.create(zillion_info)

    def apply_config(self, config):
        """
        This will update or add zillion info to the schema item info dict if it
        appears in the datasource config
        """
        self.apply_global_config(config)

        for ds_name in config.get("datasources", {}):
            ds = self.get_datasource(ds_name)
            ds_config = config["datasources"][ds.name]
            self.apply_datasource_config(ds_config, ds)

    def apply_adhoc_config(self, adhoc_ds):
        ds_config = {"tables": adhoc_ds.table_configs}
        self.apply_datasource_config(ds_config, adhoc_ds)

    def populate_conversion_fields(self, ds):
        for table in ds.metadata.tables.values():
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

                convs = get_dialect_type_conversions(ds.get_dialect_name(), column)
                if convs:
                    assert not type(column.type) in types_converted, (
                        "Table %s has multiple columns of same type allowing conversions"
                        % table.fullname
                    )
                    types_converted.add(type(column.type))

                for field_name, ds_formula in convs:
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

    def populate_table_field_map(self, ds):
        for table in ds.metadata.tables.values():
            if not table.zillion:
                continue

            for column in table.c:
                if not column.zillion:
                    continue

                for field in column.zillion.get_field_names():
                    assert (
                        not self.table_field_map[ds.name]
                        .get(table.fullname, {})
                        .get(field, None)
                    ), "Multiple columns for the same field in a single table not allowed"
                    self.table_field_map[ds.name].setdefault(table.fullname, {})[
                        field
                    ] = column

    def remove_table_field_map(self, ds):
        del self.table_field_map[ds.name]

    def field_exists(self, field):
        if field in self.dimensions or field in self.metrics:
            return True
        return False

    def get_field(self, obj):
        if isinstance(obj, str):
            if obj in self.metrics:
                return self.metrics[obj]
            if obj in self.dimensions:
                return self.dimensions[obj]
            assert False, 'Field "%s" does not exist' % obj

        if isinstance(obj, dict):
            return AdHocField.create(obj)

        raise InvalidFieldException("Invalid field object: %s" % obj)

    def get_metric(self, obj):
        if isinstance(obj, str):
            if obj not in self.metrics:
                raise InvalidFieldException("Invalid metric name: %s" % obj)
            return self.metrics[obj]

        if isinstance(obj, dict):
            metric = AdHocMetric.create(obj)
            assert metric.name not in self.metrics, (
                "AdHocMetric can not use name of an existing metric: %s" % metric.name
            )
            metric.check_formula_fields(self)
            return metric

        raise InvalidFieldException("Invalid metric object: %s" % obj)

    def get_dimension(self, obj):
        if isinstance(obj, str):
            if obj not in self.dimensions:
                raise InvalidFieldException("Invalid dimension name: %s" % obj)
            return self.dimensions[obj]

        if isinstance(obj, dict):
            dim = AdHocDimension.create(obj)
            assert dim.name not in self.dimensions, (
                "AdHocDimension can not use name of an existing dimension: %s"
                % dim.name
            )
            return dim

        raise InvalidFieldException("Invalid metric object: %s" % obj)

    def get_supported_dimensions_for_metric(self, metric, use_cache=True):
        dims = set()
        metric = self.get_metric(metric)

        if use_cache and metric.name in self.supported_dimension_cache:
            return self.supported_dimension_cache[metric]

        ds_names = self.get_datasource_names()

        for ds_name in ds_names:
            ds_tables = self.get_tables_with_metric(ds_name, metric.name)
            ds_graph = self.ds_graphs[ds_name]
            used_tables = set()

            for ds_table in ds_tables:
                if ds_table.fullname not in used_tables:
                    dims |= get_table_dimensions(
                        self, self.tables[ds_name][ds_table.fullname]
                    )
                    used_tables.add(ds_table.fullname)

                desc_tables = nx.descendants(ds_graph, ds_table.fullname)
                for desc_table in desc_tables:
                    if desc_table not in used_tables:
                        dims |= get_table_dimensions(
                            self, self.tables[ds_name][desc_table]
                        )
                        used_tables.add(desc_table)

        self.supported_dimension_cache[metric] = dims
        return dims

    def get_supported_dimensions(self, metrics):
        dims = set()
        for metric in metrics:
            supported_dims = self.get_supported_dimensions_for_metric(metric)
            dims = (dims & supported_dims) if len(dims) else supported_dims
        return dims

    def get_primary_key_fields(self, primary_key):
        pk_fields = set()
        for col in primary_key:
            pk_dims = [
                x
                for x in col.zillion.fields
                if isinstance(x, str) and x in self.dimensions
            ]
            assert len(pk_dims) == 1, (
                "Primary key column has multiple dimensions: %s/%s"
                % (col, col.zillion.fields)
            )
            pk_fields.add(pk_dims[0])
        return pk_fields

    def find_neighbor_tables(self, ds_name, table):
        neighbor_tables = []
        fields = get_table_fields(table)

        if table.zillion.type == TableTypes.METRIC:
            # Find dimension tables whose primary key is contained in the metric table
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
        parent_name = table.zillion.parent
        if parent_name:
            parent = self.tables[ds_name][parent_name]
            pk_fields = self.get_primary_key_fields(parent.primary_key)
            for pk_field in pk_fields:
                assert pk_field in fields, (
                    "Table %s is parent of %s but primary key %s is not in both"
                    % (parent.fullname, table.fullname, pk_fields)
                )
            neighbor_tables.append(NeighborTable(parent, pk_fields))
        return neighbor_tables

    def add_ds_graph(self, ds):
        assert not (ds.name in self.ds_graphs), (
            "Datasource %s already has a graph" % ds.name
        )
        graph = nx.DiGraph()
        self.ds_graphs[ds.name] = graph
        tables = self.tables[ds.name].values()
        for table in tables:
            graph.add_node(table.fullname)
            neighbors = self.find_neighbor_tables(ds.name, table)
            for neighbor in neighbors:
                graph.add_node(neighbor.table.fullname)
                graph.add_edge(
                    table.fullname,
                    neighbor.table.fullname,
                    join_fields=neighbor.join_fields,
                )

    def remove_ds_graph(self, ds):
        del self.ds_graphs[ds.name]

    def add_table(self, ds, table):
        if not table.zillion:
            return
        self.tables[ds.name][table.fullname] = table
        if table.zillion.type == TableTypes.METRIC:
            self.add_metric_table(ds, table)
        elif table.zillion.type == TableTypes.DIMENSION:
            self.add_dimension_table(ds, table)
        else:
            assert False, "Invalid table type: %s" % table.zillion.type

    def remove_table(self, ds, table):
        if table.zillion.type == TableTypes.METRIC:
            self.remove_metric_table(ds, table)
        elif table.zillion.type == TableTypes.DIMENSION:
            self.remove_dimension_table(ds, table)
        else:
            assert False, "Invalid table type: %s" % table.zillion.type
        del self.tables[ds.name][table.fullname]

    def add_metric_table(self, ds, table):
        self.metric_tables[ds.name][table.fullname] = table
        for column in table.c:
            if not column.zillion:
                continue

            if not column.zillion.active:
                continue

            for field in column.zillion.get_field_names():
                if field in self.metrics:
                    self.metrics[field].add_column(ds, column)
                elif field in self.dimensions:
                    self.dimensions[field].add_column(ds, column)
                elif table.zillion.autocolumns:
                    if is_probably_metric(column):
                        self.add_metric_column(ds, column, field)
                    else:
                        self.add_dimension_column(ds, column, field)

    def remove_metric_table(self, ds, table):
        for column in table.c:
            for field in column.zillion.get_field_names():
                if field in self.metrics:
                    self.metrics[field].remove_column(ds, column)
                elif field in self.dimensions:
                    self.dimensions[field].remove_column(ds, column)
        del self.metric_tables[ds.name][table.fullname]

    def get_ds_tables_with_metric(self, metric):
        ds_tables = defaultdict(list)
        ds_metric_columns = self.metrics[metric].column_map
        count = 0
        for ds_name, columns in ds_metric_columns.items():
            for column in columns:
                ds_tables[ds_name].append(column.table)
            count += 1
        dbg(
            "found %d datasources, %d columns for metric %s"
            % (len(ds_tables), count, metric)
        )
        return ds_tables

    def get_ds_dim_tables_with_dim(self, dim):
        ds_tables = defaultdict(list)
        ds_dim_columns = self.dimensions[dim].column_map
        count = 0
        for ds_name, columns in ds_dim_columns.items():
            for column in columns:
                if column.table.zillion.type != TableTypes.DIMENSION:
                    continue
                ds_tables[ds_name].append(column.table)
                count += 1
        dbg(
            "found %d datasources, %d columns for dim %s" % (len(ds_tables), count, dim)
        )
        return ds_tables

    def add_dimension_table(self, ds, table):
        self.dimension_tables[ds.name][table.fullname] = table
        for column in table.c:
            if not column.zillion:
                continue

            if not column.zillion.active:
                continue

            for field in column.zillion.get_field_names():
                if field in self.metrics:
                    assert False, "Dimension table has metric field: %s" % field
                elif field in self.dimensions:
                    self.dimensions[field].add_column(ds, column)
                elif table.zillion.autocolumns:
                    self.add_dimension_column(ds, column, field)

    def add_metric(self, metric):
        if metric.name in self.metrics:
            warn("Metric %s is already in warehouse metrics" % metric.name)
            return
        self.metrics[metric.name] = metric

    def add_metric_column(self, ds, column, field):
        if field not in self.metrics:
            dbg(
                "Adding metric %s from column %s.%s"
                % (field, ds.name, column_fullname(column))
            )
            aggregation, rounding = infer_aggregation_and_rounding(column)
            metric = Metric(
                field, column.type, aggregation=aggregation, rounding=rounding
            )
            self.add_metric(metric)
        self.metrics[field].add_column(ds, column)

    def get_tables_with_metric(self, ds_name, metric_name):
        columns = self.metrics[metric_name].get_columns(ds_name)
        return {x.table for x in columns}

    def add_dimension(self, dimension):
        if dimension.name in self.dimensions:
            warn("Dimension %s is already in warehouse dimensions" % dimension.name)
            return
        self.dimensions[dimension.name] = dimension

    def add_dimension_column(self, ds, column, field):
        if field not in self.dimensions:
            dbg(
                "Adding dimension %s from column %s.%s"
                % (field, ds.name, column_fullname(column))
            )
            dimension = Dimension(field, column.type)
            self.add_dimension(dimension)
        self.dimensions[field].add_column(ds, column)

    def get_tables_with_dimension(self, ds_name, dim_name):
        columns = self.dimensions[dim_name].get_columns(ds_name)
        return {x.table for x in columns}

    def find_joins_to_dimension(self, ds_name, table, dimension):
        ds_graph = self.ds_graphs[ds_name]
        joins = []

        dim_columns = self.dimensions[dimension].get_columns(ds_name)
        dim_column_table_map = {c.table.fullname: c for c in dim_columns}
        for column in dim_columns:
            if column.table == table:
                paths = [[table.fullname]]
            else:
                # TODO: consider caching with larger graphs, or precomputing
                paths = nx.all_simple_paths(
                    ds_graph, table.fullname, column.table.fullname
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
                join = joins_from_path(ds_name, ds_graph, path, field_map=field_map)
                joins.append(join)

        dbg("Found joins to dim %s for table %s:" % (dimension, table.fullname))
        dbg(joins)
        return joins

    def get_possible_joins(self, ds_name, table, grain):
        """This takes a given table (usually a metric table) and tries to find one or
        more joins to each dimension of the grain. It's possible some of these
        joins satisfy other parts of the grain too which leaves room for
        consolidation, but it's also possible to have it generate independent,
        non-overlapping joins to meet the grain.
        """
        assert (
            table.fullname in self.tables[ds_name]
        ), "Could not find table %s in datasource %s" % (table.fullname, ds_name)

        if not grain:
            dbg("No grain specified, ignoring joins")
            return None

        possible_dim_joins = {}
        for dimension in grain:
            dim_joins = self.find_joins_to_dimension(ds_name, table, dimension)
            if not dim_joins:
                dbg(
                    "table %s can not satisfy dimension %s"
                    % (table.fullname, dimension)
                )
                return None

            possible_dim_joins[dimension] = dim_joins

        possible_joins = self.consolidate_field_joins(
            ds_name, grain, possible_dim_joins
        )
        dbg("possible joins:")
        dbg(possible_joins)
        return possible_joins

    def find_possible_table_sets(self, ds_name, ds_tables_with_field, field, grain):
        table_sets = []
        for field_table in ds_tables_with_field:
            if (not grain) or grain.issubset(get_table_fields(field_table)):
                table_set = TableSet(ds_name, field_table, None, grain, set([field]))
                table_sets.append(table_set)
                dbg("full grain (%s) covered in %s" % (grain, field_table.fullname))
                continue

            joins = self.get_possible_joins(ds_name, field_table, grain)
            if not joins:
                dbg("table %s can not join at grain %s" % (field_table.fullname, grain))
                continue

            dbg(
                "adding %d possible join(s) to table %s"
                % (len(joins), field_table.fullname)
            )
            for join, covered_dims in joins.items():
                table_set = TableSet(ds_name, field_table, join, grain, set([field]))
                table_sets.append(table_set)

        return table_sets

    def get_ds_table_sets(self, ds_tables, field, grain):
        """Returns all table sets that can satisfy grain in each datasource"""
        ds_table_sets = {}
        for ds_name, ds_tables_with_field in ds_tables.items():
            possible_table_sets = self.find_possible_table_sets(
                ds_name, ds_tables_with_field, field, grain
            )
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
        warn("No datasource priorities established, picking random option")
        assert ds_names, "No datasource names provided"
        return random.choice(ds_names)

    def choose_best_table_set(self, ds_table_sets):
        ds_name = self.choose_best_data_source(list(ds_table_sets.keys()))
        if len(ds_table_sets[ds_name]) > 1:
            # TODO: establish table set priorities based on expected query performance?
            warn(
                "Picking smallest of %d available table sets"
                % len(ds_table_sets[ds_name])
            )
        return sorted(ds_table_sets[ds_name], key=lambda x: len(x))[0]

    def generate_unsupported_grain_msg(self, grain, metric):
        """
        This assumes you are in a situation where you are sure the metric can not
        meet the grain and want to generate a helpful message pinpointing the
        issue.  If the metric actually supports all dimensions, the conclusion
        is that it just doesn't support them all in a single datasource and
        thus can't meet the grain.
        """
        supported = self.get_supported_dimensions_for_metric(metric)
        unsupported = grain - supported
        if unsupported:
            msg = (
                "metric %s can not meet grain %s due to unsupported dimensions: %s"
                % (metric, grain, unsupported)
            )
        else:
            msg = "metric %s can not meet grain %s in any single datasource" % (
                metric,
                grain,
            )
        return msg

    def get_metric_table_set(self, metric, grain):
        dbg("metric:%s grain:%s" % (metric, grain))
        ds_metric_tables = self.get_ds_tables_with_metric(metric)
        ds_table_sets = self.get_ds_table_sets(ds_metric_tables, metric, grain)
        if not ds_table_sets:
            msg = self.generate_unsupported_grain_msg(grain, metric)
            raise UnsupportedGrainException(msg)
        table_set = self.choose_best_table_set(ds_table_sets)
        return table_set

    def get_dimension_table_set(self, grain):
        """
        This is meant to be used in cases where no metrics are requested. We only
        allow it to look at dim tables since the assumption is joining to a metric
        table to explore dimensions doesn't make sense and would have poor performance.
        """
        dbg("grain:%s" % grain)

        table_set = None
        for dim_name in grain:
            ds_dim_tables = self.get_ds_dim_tables_with_dim(dim_name)
            ds_table_sets = self.get_ds_table_sets(ds_dim_tables, dim_name, grain)
            if not ds_table_sets:
                continue
            table_set = self.choose_best_table_set(ds_table_sets)
            break

        if not table_set:
            raise UnsupportedGrainException(
                "No dimension table set found to meet grain: %s" % grain
            )
        return table_set

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
                all_covered_fields = join.get_covered_fields(self)
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
            join.add_fields(self, covered_fields)
        return join_fields

    def consolidate_field_joins(self, ds_name, grain, field_joins):
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
            join.add_fields(self, covered_fields)
            return {join: covered_fields}

        sorted_join_fields = self.eliminate_redundant_joins(sorted_join_fields)
        candidates = self.find_join_combinations(sorted_join_fields, grain)
        join_fields = self.choose_best_join_combination(candidates)
        return join_fields

    def build_report(
        self,
        metrics=None,
        dimensions=None,
        criteria=None,
        row_filters=None,
        rollup=None,
        pivot=None,
    ):
        return Report(
            self,
            metrics=metrics,
            dimensions=dimensions,
            criteria=criteria,
            row_filters=row_filters,
            rollup=rollup,
            pivot=pivot,
        )

    def load_report(self, report_id):
        report = Report.load(self, report_id)
        return report

    def delete_report(self, report_id):
        report = Report.delete(report_id)
        return report

    # def save_report(self, **kwargs):
    #     report = self.build_report(**kwargs)
    #     report.save()
    #     return report

    def execute(
        self,
        metrics=None,
        dimensions=None,
        criteria=None,
        row_filters=None,
        rollup=None,
        pivot=None,
        adhoc_datasources=None,
    ):
        start = time.time()
        adhoc_datasources = adhoc_datasources or []
        self.add_adhoc_datasources(adhoc_datasources)

        report = self.build_report(
            metrics=metrics,
            dimensions=dimensions,
            criteria=criteria,
            row_filters=row_filters,
            rollup=rollup,
            pivot=pivot,
        )
        result = report.execute()

        self.remove_adhoc_datasources(adhoc_datasources)
        dbg("warehouse report took %.3fs" % (time.time() - start))
        return result

    def execute_id(self, report_id, adhoc_datasources=None):
        start = time.time()
        adhoc_datasources = adhoc_datasources or []
        self.add_adhoc_datasources(adhoc_datasources)

        report = self.load_report(report_id)
        result = report.execute()

        self.remove_adhoc_datasources(adhoc_datasources)
        dbg("warehouse report took %.3fs" % (time.time() - start))
        return result
