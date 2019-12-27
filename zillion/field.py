import sqlalchemy as sa
from tlbx import MappingMixin, PrintMixin, initializer, warn, st, get_string_format_args
from zillion.configs import (
    MetricConfigSchema,
    TechnicalInfoSchema,
    DimensionConfigSchema,
    AdHocMetricSchema,
    AdHocFieldSchema,
    parse_technical_string,
    is_valid_field_name,
    zillion_config,
)
from zillion.core import (
    InvalidFieldException,
    MaxFormulaDepthException,
    AggregationTypes,
    TableTypes,
)
from zillion.sql_utils import (
    aggregation_to_sqla_func,
    contains_aggregation,
    type_string_to_sa_type,
    is_probably_metric,
    sqla_compile,
    column_fullname,
)


MAX_FORMULA_DEPTH = 3


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
        if isinstance(type, str):
            self.type = type_string_to_sa_type(type)

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
            raise MaxFormulaDepthException

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
        # XXX metric = Metric(**metric_def) ?
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


class FieldManagerMixin:
    metrics_attr = "_metrics"
    dimensions_attr = "_dimensions"

    def get_child_field_managers(self):
        return []

    def _directly_has_metric(self, name):
        return name in getattr(self, self.metrics_attr)

    def _directly_has_dimension(self, name):
        return name in getattr(self, self.dimensions_attr)

    def _directly_has_field(self, name):
        return name in getattr(self, self.metrics_attr) or name in getattr(
            self, self.dimensions_attr
        )

    def has_metric(self, name):
        if self._directly_has_metric(name):
            return True
        for fm in self.get_child_field_managers():
            if fm.has_metric(name):
                return True
        return False

    def has_dimension(self, name):
        if self._directly_has_dimension(name):
            return True
        for fm in self.get_child_field_managers():
            if fm.has_dimension(name):
                return True
        return False

    def has_field(self, name):
        if self._directly_has_field(name):
            return True
        for fm in self.get_child_field_managers():
            if fm.has_field(name):
                return True
        return False

    def get_metric(self, obj, adhoc_datasources=None):
        # TODO: handle adhoc DSes?

        if isinstance(obj, str):
            if self._directly_has_metric(obj):
                return getattr(self, self.metrics_attr)[obj]
            elif self.has_metric(obj):
                for fm in self.get_child_field_managers():
                    if fm.has_metric(obj):
                        return fm.get_metric(obj)
            raise InvalidFieldException("Invalid metric name: %s" % obj)

        if isinstance(obj, dict):
            metric = AdHocMetric.create(obj)
            assert not self.has_metric(metric.name), (
                "AdHocMetric can not use name of an existing metric: %s" % metric.name
            )
            metric.check_formula_fields(self)
            return metric

        raise InvalidFieldException("Invalid metric object: %s" % obj)

    def get_dimension(self, obj, adhoc_datasources=None):
        if isinstance(obj, str):
            if self._directly_has_dimension(obj):
                return getattr(self, self.dimensions_attr)[obj]
            elif self.has_dimension(obj):
                for fm in self.get_child_field_managers():
                    if fm.has_dimension(obj):
                        return fm.get_dimension(obj)
            raise InvalidFieldException("Invalid dimension name: %s" % obj)

        if isinstance(obj, dict):
            dim = AdHocDimension.create(obj)
            assert not self.has_dimension(dim.name), (
                "AdHocDimension can not use name of an existing dimension: %s"
                % dim.name
            )
            return dim

        raise InvalidFieldException("Invalid dimension object: %s" % obj)

    def get_field(self, obj):
        if isinstance(obj, str):
            if self.has_metric(obj):
                return self.get_metric(obj)
            if self.has_dimension(obj):
                return self.get_dimension(obj)
            raise InvalidFieldException("Invalid field name: %s" % obj)

        if isinstance(obj, dict):
            field = AdHocField.create(obj)
            assert not self.has_field(field.name), (
                "AdHocField can not use name of an existing field: %s" % field.name
            )
            return field

        raise InvalidFieldException("Invalid field object: %s" % obj)

    def get_metrics(self):
        metrics = {}
        metrics.update(getattr(self, self.metrics_attr))
        for fm in self.get_child_field_managers():
            fm_metrics = fm.get_metrics()
            metrics.update(fm_metrics)
        return metrics

    def get_dimensions(self):
        dimensions = {}
        dimensions.update(getattr(self, self.dimensions_attr))
        for fm in self.get_child_field_managers():
            fm_dimensions = fm.get_dimensions()
            dimensions.update(fm_dimensions)
        return dimensions

    def get_fields(self):
        fields = {}
        fields.update(getattr(self, self.metrics_attr))
        fields.update(getattr(self, self.dimensions_attr))
        for fm in self.get_child_field_managers():
            fm_fields = fm.get_fields()
            fields.update(fm_fields)
        return fields

    def get_metric_names(self):
        return set(self.get_metrics().keys())

    def get_dimension_names(self):
        return set(self.get_dimensions().keys())

    def get_field_names(self):
        return set(self.get_fields().keys())

    def add_metric(self, metric, force=False):
        if (not force) and self.has_metric(metric.name):
            warn("Metric %s already exists on %s" % (metric.name, self))
            return
        getattr(self, self.metrics_attr)[metric.name] = metric

    def add_dimension(self, dimension, force=False):
        if (not force) and self.has_dimension(dimension.name):
            warn("Dimension %s already exists on %s" % (dimension.name, self))
            return
        getattr(self, self.dimensions_attr)[dimension.name] = dimension

    def populate_global_fields(self, config, force=False):
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
                self.add_metric(metric, force=force)

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
            self.add_dimension(dim, force=force)

        # Defer formula metrics so params can be checked against existing fields
        for metric in formula_metrics:
            metric.check_formula_fields(self)
            self.add_metric(metric, force=force)

    def find_field_sources(self, field):
        sources = []
        if self._directly_has_field(field):
            sources.append(self)
        for fm in self.get_child_field_managers():
            if fm._directly_has_field(field):
                sources.append(fm)
        return sources


def get_table_metrics(fm, table):
    metrics = set()
    for col in table.c:
        if not (getattr(col, "zillion", None) and col.zillion.active):
            continue
        for field in col.zillion.get_field_names():
            if fm.has_metric(field):
                metrics.add(field)
    return metrics


def get_table_dimensions(fm, table):
    dims = set()
    for col in table.c:
        if not (getattr(col, "zillion", None) and col.zillion.active):
            continue
        for field in col.zillion.get_field_names():
            if fm.has_dimension(field):
                dims.add(field)
    return dims


def get_table_fields(table):
    fields = set()
    for col in table.c:
        if not (getattr(col, "zillion", None) and col.zillion.active):
            continue
        for field in col.zillion.get_field_names():
            fields.add(field)
    return fields


def get_table_field_column(table, field_name):
    for col in table.c:
        if not (getattr(col, "zillion", None) and col.zillion.active):
            continue
        for field in col.zillion.get_field_names():
            if field == field_name:
                return col
    assert False, "Field %s inactive or not found in table %s" % (
        field_name,
        table.fullname,
    )
