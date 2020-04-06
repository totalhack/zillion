from collections import OrderedDict
import inspect

import sqlalchemy as sa
from tlbx import (
    MappingMixin,
    PrintMixin,
    initializer,
    warn,
    info,
    format_msg,
    st,
    get_string_format_args,
)
from zillion.configs import (
    MetricConfigSchema,
    DimensionConfigSchema,
    AdHocMetricSchema,
    AdHocFieldSchema,
    create_technical,
    is_valid_field_name,
    zillion_config,
)
from zillion.core import (
    InvalidFieldException,
    MaxFormulaDepthException,
    DisallowedSQLException,
    AggregationTypes,
    TableTypes,
    FieldTypes,
)
from zillion.sql_utils import (
    aggregation_to_sqla_func,
    contains_aggregation,
    contains_sql_keywords,
    type_string_to_sa_type,
    is_probably_metric,
    sqla_compile,
    column_fullname,
)


MAX_FORMULA_DEPTH = 3


class Field(PrintMixin):
    repr_attrs = ["name"]
    field_type = None

    @initializer
    def __init__(self, name, type, **kwargs):
        is_valid_field_name(name)
        if isinstance(type, str):
            self.type = type_string_to_sa_type(type)
        if inspect.isclass(type):
            # Assume its a SQLAlchemy class
            self.type = type()

    def copy(self):
        raise NotImplementedError

    def get_formula_fields(self, warehouse, depth=0, adhoc_fms=None):
        return None, None

    def get_ds_expression(self, column, label=True):
        ds_formula = (
            column.zillion.field_map[self.name].get("ds_formula", None)
            if column.zillion.field_map[self.name]
            else None
        )
        if not ds_formula:
            if label:
                return column.label(self.name)
            return column

        if contains_sql_keywords(ds_formula):
            raise DisallowedSQLException(
                "Formula contains disallowed sql: %s" % ds_formula
            )

        if not (ds_formula.startswith("(") and ds_formula.endswith("(")):
            ds_formula = "(" + ds_formula + ")"

        if label:
            return sa.literal_column(ds_formula).label(self.name)
        return sa.literal_column(ds_formula)

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
    field_type = FieldTypes.METRIC

    def __init__(
        self,
        name,
        type,
        aggregation=AggregationTypes.SUM,
        rounding=None,
        weighting_metric=None,
        technical=None,
        required_grain=None,
        **kwargs
    ):
        if weighting_metric:
            assert aggregation == AggregationTypes.AVG, (
                'Weighting metrics are only supported for "%s" aggregation type'
                % AggregationTypes.AVG
            )

        if technical:
            technical = create_technical(technical)

        super(Metric, self).__init__(
            name,
            type,
            aggregation=aggregation,
            rounding=rounding,
            weighting_metric=weighting_metric,
            technical=technical,
            required_grain=required_grain,
            **kwargs
        )

    def copy(self):
        return Metric(
            self.name,
            self.type,
            aggregation=self.aggregation,
            rounding=self.rounding,
            weighting_metric=self.weighting_metric,
            technical=self.technical,
            required_grain=self.required_grain,
        )

    def get_ds_expression(self, column, label=True):
        expr = column
        aggr = aggregation_to_sqla_func(self.aggregation)
        skip_aggr = False

        ds_formula = (
            column.zillion.field_map[self.name].get("ds_formula", None)
            if column.zillion.field_map[self.name]
            else None
        )

        if ds_formula:
            if contains_sql_keywords(ds_formula):
                raise DisallowedSQLException(
                    "Formula contains disallowed sql: %s" % ds_formula
                )
            if contains_aggregation(ds_formula):
                info("Datasource formula contains aggregation, skipping default logic")
                skip_aggr = True
            expr = sa.literal_column(ds_formula)

        if not skip_aggr:
            if self.aggregation in [
                AggregationTypes.COUNT,
                AggregationTypes.COUNT_DISTINCT,
            ]:
                if self.rounding:
                    info("Ignoring rounding for count field: %s" % self.name)
                if label:
                    return aggr(expr).label(self.name)
                return aggr(expr)

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

        if label:
            return expr.label(self.name)
        return expr

    def get_final_select_clause(self, *args, **kwargs):
        return self.name


class Dimension(Field):
    field_type = FieldTypes.DIMENSION

    def copy(self):
        return Dimension(self.name, self.type)


class FormulaField(Field):
    def __init__(self, name, formula, **kwargs):
        super(FormulaField, self).__init__(name, None, formula=formula, **kwargs)

    def get_formula_fields(self, warehouse, depth=0, adhoc_fms=None):
        if depth > MAX_FORMULA_DEPTH:
            raise MaxFormulaDepthException

        raw_formula = self.formula
        raw_fields = set()
        formula_fields = get_string_format_args(self.formula)
        field_formula_map = {}

        for field_name in formula_fields:
            field = warehouse.get_field(field_name, adhoc_fms=adhoc_fms)

            if getattr(field, "technical", None):
                raise InvalidFieldException(
                    "Formula field %s contains field with technical: %s"
                    % (self.name, field.name)
                )

            if isinstance(field, FormulaField):
                try:
                    sub_fields, sub_formula = field.get_formula_fields(
                        warehouse, depth=depth + 1, adhoc_fms=adhoc_fms
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

    def check_formula_fields(self, warehouse, adhoc_fms=None):
        fields, _ = self.get_formula_fields(warehouse, adhoc_fms=adhoc_fms)
        for field in fields:
            warehouse.get_field(field, adhoc_fms=adhoc_fms)

    def get_ds_expression(self, column, label=True):
        assert False, "Formula-based Fields do not support get_ds_expression"

    def get_final_select_clause(self, warehouse, adhoc_fms=None):
        formula_fields, raw_formula = self.get_formula_fields(
            warehouse, adhoc_fms=adhoc_fms
        )
        format_args = {k: k for k in formula_fields}
        formula = raw_formula.format(**format_args)
        if contains_sql_keywords(formula):
            raise DisallowedSQLException(
                "Formula contains disallowed sql: %s" % formula
            )
        return sqla_compile(sa.text(formula))


class FormulaDimension(FormulaField):
    repr_atts = ["name", "formula"]
    field_type = FieldTypes.DIMENSION

    def __init__(self, name, formula, **kwargs):
        # super(FormulaDimension, self).__init__(
        #     name,
        #     formula,
        #     **kwargs
        # )
        raise InvalidFieldException("FormulaDimensions are not currently supported")


class FormulaMetric(FormulaField):
    repr_atts = ["name", "formula", "technical"]
    field_type = FieldTypes.METRIC

    def __init__(
        self,
        name,
        formula,
        aggregation=AggregationTypes.SUM,
        rounding=None,
        weighting_metric=None,
        technical=None,
        required_grain=None,
        **kwargs
    ):
        if technical:
            technical = create_technical(technical)

        super(FormulaMetric, self).__init__(
            name,
            formula,
            aggregation=aggregation,
            rounding=rounding,
            weighting_metric=weighting_metric,
            technical=technical,
            required_grain=required_grain,
            **kwargs
        )


class AdHocField(FormulaField):
    @classmethod
    def create(cls, obj):
        schema = AdHocFieldSchema()
        field_def = schema.load(obj)
        return cls(field_def["name"], field_def["formula"])


class AdHocMetric(FormulaMetric):
    def __init__(
        self, name, formula, technical=None, rounding=None, required_grain=None
    ):
        super(AdHocMetric, self).__init__(
            name,
            formula,
            technical=technical,
            rounding=rounding,
            required_grain=required_grain,
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
            required_grain=field_def["required_grain"],
        )


class AdHocDimension(AdHocField):
    field_type = FieldTypes.DIMENSION


def create_metric(metric_def):
    if metric_def["formula"]:
        metric = FormulaMetric(
            metric_def["name"],
            metric_def["formula"],
            aggregation=metric_def["aggregation"],
            rounding=metric_def["rounding"],
            weighting_metric=metric_def["weighting_metric"],
            technical=metric_def["technical"],
            required_grain=metric_def["required_grain"],
        )
    else:
        metric = Metric(
            metric_def["name"],
            metric_def["type"],
            aggregation=metric_def["aggregation"],
            rounding=metric_def["rounding"],
            weighting_metric=metric_def["weighting_metric"],
            technical=metric_def["technical"],
            required_grain=metric_def["required_grain"],
        )
    return metric


def create_dimension(dim_def):
    if dim_def.get("formula", None):
        # dim = FormulaDimension(dim_def["name"], dim_def["formula"])
        raise InvalidFieldException("FormulaDimensions are not currently supported")
    else:
        dim = Dimension(dim_def["name"], dim_def["type"])
    return dim


class FieldManagerMixin:
    metrics_attr = "_metrics"
    dimensions_attr = "_dimensions"

    def get_child_field_managers(self):
        return []

    def get_field_managers(self, adhoc_fms=None):
        return self.get_child_field_managers() + (adhoc_fms or [])

    def _directly_has_metric(self, name):
        return name in getattr(self, self.metrics_attr)

    def _directly_has_dimension(self, name):
        return name in getattr(self, self.dimensions_attr)

    def _directly_has_field(self, name):
        return name in getattr(self, self.metrics_attr) or name in getattr(
            self, self.dimensions_attr
        )

    def print_metrics(self, indent=None):
        print(format_msg(getattr(self, self.metrics_attr), label=None, indent=indent))

    def print_dimensions(self, indent=None):
        print(
            format_msg(getattr(self, self.dimensions_attr), label=None, indent=indent)
        )

    def has_metric(self, name, adhoc_fms=None):
        if self._directly_has_metric(name):
            return True
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm.has_metric(name):
                return True
        return False

    def has_dimension(self, name, adhoc_fms=None):
        if self._directly_has_dimension(name):
            return True
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm.has_dimension(name):
                return True
        return False

    def has_field(self, name, adhoc_fms=None):
        if self._directly_has_field(name):
            return True
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm.has_field(name):
                return True
        return False

    def get_metric(self, obj, adhoc_fms=None):
        if isinstance(obj, str):
            if self._directly_has_metric(obj):
                return getattr(self, self.metrics_attr)[obj]
            for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
                if fm.has_metric(obj):
                    return fm.get_metric(obj)
            raise InvalidFieldException("Invalid metric name: %s" % obj)

        if isinstance(obj, dict):
            metric = AdHocMetric.create(obj)
            assert not self.has_metric(metric.name, adhoc_fms=adhoc_fms), (
                "AdHocMetric can not use name of an existing metric: %s" % metric.name
            )
            metric.check_formula_fields(self, adhoc_fms=adhoc_fms)
            return metric

        raise InvalidFieldException("Invalid metric object: %s" % obj)

    def get_dimension(self, obj, adhoc_fms=None):
        if isinstance(obj, str):
            if self._directly_has_dimension(obj):
                return getattr(self, self.dimensions_attr)[obj]
            for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
                if fm.has_dimension(obj):
                    return fm.get_dimension(obj)
            raise InvalidFieldException("Invalid dimension name: %s" % obj)

        if isinstance(obj, dict):
            raise InvalidFieldException("AdHocDimensions are not currently supported")
            # dim = AdHocDimension.create(obj)
            # assert not self.has_dimension(dim.name, adhoc_fms=adhoc_fms), (
            #     "AdHocDimension can not use name of an existing dimension: %s"
            #     % dim.name
            # )
            # return dim

        raise InvalidFieldException("Invalid dimension object: %s" % obj)

    def get_field(self, obj, adhoc_fms=None):
        if isinstance(obj, str):
            if self.has_metric(obj, adhoc_fms=adhoc_fms):
                return self.get_metric(obj, adhoc_fms=adhoc_fms)
            if self.has_dimension(obj, adhoc_fms=adhoc_fms):
                return self.get_dimension(obj, adhoc_fms=adhoc_fms)
            raise InvalidFieldException("Invalid field name: %s" % obj)

        # TODO: should this be allowed?
        if isinstance(obj, dict):
            field = AdHocField.create(obj)
            assert not self.has_field(field.name, adhoc_fms=adhoc_fms), (
                "AdHocField can not use name of an existing field: %s" % field.name
            )
            return field

        raise InvalidFieldException("Invalid field object: %s" % obj)

    def get_metrics(self, adhoc_fms=None):
        metrics = {}
        metrics.update(getattr(self, self.metrics_attr))
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm_metrics = fm.get_metrics()
            metrics.update(fm_metrics)
        return metrics

    def get_dimensions(self, adhoc_fms=None):
        dimensions = {}
        dimensions.update(getattr(self, self.dimensions_attr))
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm_dimensions = fm.get_dimensions()
            dimensions.update(fm_dimensions)
        return dimensions

    def get_fields(self, adhoc_fms=None):
        fields = {}
        fields.update(getattr(self, self.metrics_attr))
        fields.update(getattr(self, self.dimensions_attr))
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm_fields = fm.get_fields()
            fields.update(fm_fields)
        return fields

    def get_metric_names(self, adhoc_fms=None):
        return set(self.get_metrics(adhoc_fms=adhoc_fms).keys())

    def get_dimension_names(self, adhoc_fms=None):
        return set(self.get_dimensions(adhoc_fms=adhoc_fms).keys())

    def get_field_names(self, adhoc_fms=None):
        return set(self.get_fields(adhoc_fms=adhoc_fms).keys())

    def add_metric(self, metric, force=False):
        if self.has_dimension(metric.name):
            raise InvalidFieldException(
                "Trying to add metric with same name as a dimension: %s" % metric.name
            )
        if (not force) and self.has_metric(metric.name):
            warn("Metric %s already exists on %s" % (metric.name, self))
            return
        getattr(self, self.metrics_attr)[metric.name] = metric

    def add_dimension(self, dimension, force=False):
        if self.has_metric(dimension.name):
            raise InvalidFieldException(
                "Trying to add dimension with same name as a metric: %s"
                % dimension.name
            )
        if (not force) and self.has_dimension(dimension.name):
            warn("Dimension %s already exists on %s" % (dimension.name, self))
            return
        getattr(self, self.dimensions_attr)[dimension.name] = dimension

    def populate_global_fields(self, config, force=False):
        formula_metrics = []
        formula_dims = []

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
                formula_metrics.append(metric)  # These get added later
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

            if isinstance(dim, FormulaDimension):
                # formula_dims.append(dim) # These get added later
                raise InvalidFieldException(
                    "FormulaDimensions are not currently supported"
                )
            else:
                self.add_dimension(dim, force=force)

        # Defer formulas so params can be checked against existing fields
        for metric in formula_metrics:
            metric.check_formula_fields(self)
            self.add_metric(metric, force=force)

        for dim in formula_dims:
            dim.check_formula_fields(self)
            self.add_dimension(dim, force=force)

    def find_field_sources(self, field, adhoc_fms=None):
        sources = []
        if self._directly_has_field(field):
            sources.append(self)

        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm._directly_has_field(field):
                sources.append(fm)
        return sources


def get_table_metrics(fm, table, adhoc_fms=None):
    metrics = set()
    for col in table.c:
        if not (getattr(col, "zillion", None) and col.zillion.active):
            continue
        for field in col.zillion.get_field_names():
            if fm.has_metric(field, adhoc_fms=adhoc_fms):
                metrics.add(field)
    return metrics


def get_table_dimensions(fm, table, adhoc_fms=None):
    dims = set()
    for col in table.c:
        if not (getattr(col, "zillion", None) and col.zillion.active):
            continue
        for field in col.zillion.get_field_names():
            if fm.has_dimension(field, adhoc_fms=adhoc_fms):
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


def get_conversions_for_type(coltype):
    for basetype, convs in TYPE_ALLOWED_CONVERSIONS.items():
        if issubclass(coltype, basetype):
            return convs
    return None


def get_dialect_type_conversions(dialect, column):
    coltype = type(column.type)
    conv_info = get_conversions_for_type(coltype)
    if not conv_info:
        return []

    results = []
    allowed = conv_info["allowed_conversions"]
    convs = conv_info["dialect_conversions"]

    for field_def in allowed:
        field_name = field_def.name
        conv = convs[dialect].get(field_name, None)
        if not conv:
            continue
        format_args = get_string_format_args(conv)
        assert not any([x != "" for x in format_args]), (
            "Field conversion has non-named format arguments: %s" % conv
        )
        if format_args:
            conv = conv.format(*[column_fullname(column) for i in format_args])
        results.append((field_def, conv))

    return results


DATETIME_CONVERSION_FIELDS = [
    Dimension("year", sa.Integer),
    Dimension("quarter", sa.String(8)),
    Dimension("quarter_of_year", sa.SmallInteger),
    Dimension("month", sa.String(8)),
    Dimension("month_name", sa.String(8)),
    Dimension("month_of_year", sa.SmallInteger),
    Dimension("date", sa.String(10)),
    Dimension("day_name", sa.String(10)),
    Dimension("day_of_week", sa.SmallInteger),
    Dimension("day_of_month", sa.SmallInteger),
    Dimension("day_of_year", sa.SmallInteger),
    Dimension("hour", sa.String(20)),
    Dimension("hour_of_day", sa.SmallInteger),
    Dimension("minute", sa.String(20)),
    Dimension("minute_of_hour", sa.SmallInteger),
    Dimension("datetime", sa.String(20)),
    Dimension("unixtime", sa.BigInteger),
]

DATE_CONVERSION_FIELDS = []
for _dim in DATETIME_CONVERSION_FIELDS:
    if _dim.name == "hour":
        break
    DATE_CONVERSION_FIELDS.append(_dim)

# Somewhat adhering to ISO 8601, but ignoring the "T" between the date/time
# and not including timezone offsets for now because zillion assumes
# everything is in the same timezone (or the datasource formulas take care of
# aligning timezones).
DIALECT_DATE_CONVERSIONS = {
    "sqlite": {
        "year": "cast(strftime('%Y', {}) as integer)",
        "quarter": "strftime('%Y', {}) || '-Q' || ((cast(strftime('%m', {}) as integer) + 2) / 3)",  # 2020-Q1
        "quarter_of_year": "(cast(strftime('%m', {}) as integer) + 2) / 3",
        "month": "strftime('%Y-%m', {})",
        "month_name": (
            "CASE strftime('%m', {}) "
            "WHEN '01' THEN 'January' "
            "WHEN '02' THEN 'February' "
            "WHEN '03' THEN 'March' "
            "WHEN '04' THEN 'April' "
            "WHEN '05' THEN 'May' "
            "WHEN '06' THEN 'June' "
            "WHEN '07' THEN 'July' "
            "WHEN '08' THEN 'August' "
            "WHEN '09' THEN 'September' "
            "WHEN '10' THEN 'October' "
            "WHEN '11' THEN 'November' "
            "WHEN '12' THEN 'December' "
            "ELSE NULL "
            "END"
        ),
        "month_of_year": "cast(strftime('%m', {}) as integer)",
        "date": "strftime('%Y-%m-%d', {})",
        "day_name": (
            "CASE cast(strftime('%w', {}) as integer) "
            "WHEN 0 THEN 'Sunday' "
            "WHEN 1 THEN 'Monday' "
            "WHEN 2 THEN 'Tuesday' "
            "WHEN 3 THEN 'Wednesday' "
            "WHEN 4 THEN 'Thursday' "
            "WHEN 5 THEN 'Friday' "
            "WHEN 6 THEN 'Saturday' "
            "ELSE NULL "
            "END"
        ),
        "day_of_week": "(cast(strftime('%w', {}) as integer) + 6) % 7 + 1",  # Convert to Monday = 1
        "day_of_month": "cast(strftime('%d', {}) as integer)",
        "day_of_year": "cast(strftime('%j', {}) as integer)",
        "hour": "strftime('%Y-%m-%d %H:00:00', {})",
        "hour_of_day": "cast(strftime('%H', {}) as integer)",
        "minute": "strftime('%Y-%m-%d %H:%M:00', {})",
        "minute_of_hour": "cast(strftime('%M', {}) as integer)",
        "datetime": "strftime('%Y-%m-%d %H:%M:%S', {})",
        "unixtime": "cast(strftime('%s', {}) as integer)",
    },
    "mysql": {
        "year": "EXTRACT(YEAR FROM {})",
        "quarter": "CONCAT(YEAR({}), '-Q', QUARTER({}))",
        "quarter_of_year": "EXTRACT(QUARTER FROM {})",
        "month": "DATE_FORMAT({}, '%Y-%m')",
        "month_name": "MONTHNAME({})",
        "month_of_year": "EXTRACT(MONTH FROM {})",
        "date": "DATE_FORMAT({}, '%Y-%m-%d')",
        "day_name": "DAYNAME({})",
        "day_of_week": "WEEKDAY({}) + 1",  # Monday = 1
        "day_of_month": "EXTRACT(DAY FROM {})",
        "day_of_year": "DAYOFYEAR({})",
        "hour": "DATE_FORMAT({}, '%Y-%m-%d %H:00:00')",
        "hour_of_day": "EXTRACT(HOUR FROM {})",
        "minute": "DATE_FORMAT({}, '%Y-%m-%d %H:%i:00')",
        "minute_of_hour": "EXTRACT(MINUTE FROM {})",
        "datetime": "DATE_FORMAT({}, '%Y-%m-%d %H:%i:%S')",
        "unixtime": "UNIX_TIMESTAMP({})",
    },
    "postgresql": {
        "year": "EXTRACT(YEAR FROM {})",
        "quarter": "TO_CHAR({}, 'FMYYYY-\"Q\"Q')",
        "quarter_of_year": "EXTRACT(QUARTER FROM {})",
        "month": "TO_CHAR({}, 'FMYYYY-MM')",
        "month_name": "TO_CHAR({}, 'FMMonth')",
        "month_of_year": "EXTRACT(MONTH FROM {})",
        "date": "TO_CHAR({}, 'FMYYYY-MM-DD')",
        "day_name": "TO_CHAR({}, 'FMDay')",
        "day_of_week": "EXTRACT(ISODOW FROM {})",  # Monday = 1
        "day_of_month": "EXTRACT(DAY FROM {})",
        "day_of_year": "EXTRACT(DOY FROM {})",
        "hour": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:00:00')",
        "hour_of_day": "EXTRACT(HOUR FROM {})",
        "minute": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:MI:00')",
        "minute_of_hour": "EXTRACT(MINUTE FROM {})",
        "datetime": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:MI:SS')",
        "unixtime": "EXTRACT(epoch from {})",
    },
}

TYPE_ALLOWED_CONVERSIONS = {
    sa.DateTime: {
        "allowed_conversions": DATETIME_CONVERSION_FIELDS,
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.DATETIME: {
        "allowed_conversions": DATETIME_CONVERSION_FIELDS,
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.TIMESTAMP: {
        "allowed_conversions": DATETIME_CONVERSION_FIELDS,
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.Date: {
        "allowed_conversions": DATE_CONVERSION_FIELDS,  # DATE_HIERARCHY[0 : DATE_HIERARCHY.index("hour")],
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
    sa.DATE: {
        "allowed_conversions": DATE_CONVERSION_FIELDS,  # DATE_HIERARCHY[0 : DATE_HIERARCHY.index("hour")],
        "dialect_conversions": DIALECT_DATE_CONVERSIONS,
    },
}
