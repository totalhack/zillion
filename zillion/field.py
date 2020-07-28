import sqlalchemy as sa

from zillion.configs import (
    EXCLUDE,
    ConfigMixin,
    FieldConfigSchema,
    FormulaFieldConfigSchema,
    MetricConfigSchema,
    FormulaMetricConfigSchema,
    DimensionConfigSchema,
    AdHocMetricSchema,
    AdHocFieldSchema,
    create_technical,
    is_valid_field_name,
    is_active,
)
from zillion.core import *
from zillion.sql_utils import (
    aggregation_to_sqla_func,
    contains_aggregation,
    contains_sql_keywords,
    type_string_to_sa_type,
    to_generic_sa_type,
    sqla_compile,
    column_fullname,
)


MAX_FORMULA_DEPTH = 3


class Field(ConfigMixin, PrintMixin):
    """Represents the concept a column is capturing, which may be shared by
    columns in other tables or datasources. For example, you may have a several
    columns in your databases/tables that represent the concept of "revenue". In
    other words, a column is like an instance of a Field.
    
    **Parameters:**
    
    * **name** - (*str*) The name of the field
    * **type** - (*str or SQLAlchemy type*) The column type for the field.
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field
    * **kwargs** - Additional attributes stored on the field object
    
    **Attributes:**
    
    * **name** - (*str*) The name of the field
    * **type** - (*str*) A string representing the generic SQLAlchemy type
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field
    * **sa_type** - (*SQLAlchemy type*) If a dialect-specific type object
    is passed in on init it will be coerced to a generic type.
    * **field_type** - (*str*) A valid FieldType string
    * **schema** - (*Marshmallow schema*) A FieldConfigSchema class
    
    """

    repr_attrs = ["name", "type"]
    field_type = None
    schema = FieldConfigSchema

    @initializer
    def __init__(self, name, type, display_name=None, description=None, **kwargs):
        self.sa_type = None
        if isinstance(type, str):
            self.sa_type = type_string_to_sa_type(type)
        elif type:
            self.sa_type = to_generic_sa_type(type)
            self.type = repr(self.sa_type)
        # This will do schema validation
        super().__init__()

    def copy(self):
        """Copy this field"""
        return self.__class__.from_config(self.to_config())

    def get_formula_fields(self, warehouse, depth=0, adhoc_fms=None):
        """Get the fields that are part of this field's formula
        
        **Parameters:**
        
        * **warehouse** - (*Warehouse*) A zillion warehouse that will contain
        all relevant fields
        * **depth** - (*int, optional*) Track the depth of recursion into the
        formula
        * **adhoc_fms** - (*list, optional*) A list of FieldManagers
        
        **Returns:**
        
        (*set, str*) - The set of all base fields involved in the formula
        calculation, as well as an expanded version of the formula. All fields
        in the expanded formula should be raw fields (i.e. not formula
        fields).
        
        """
        return None, None

    def get_ds_expression(self, column, label=True):
        """Get the datasource-level sql expression for this field
        
        **Parameters:**
        
        * **column** - (*Column*) A SQLAlchemy column that supports this field
        * **label** - (*bool, optional*) If true, label the expression with the
        field name
        
        """
        ds_formula = column.zillion.field_ds_formula(self.name)
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
        """The sql clause used when selecting at the combined query layer"""
        return self.name

    # https://stackoverflow.com/questions/2909106/whats-a-correct-and-good-way-to-implement-hash

    def __key(self):
        return self.name

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.__key() == other.__key()


class Metric(Field):
    """Fields that represent values to be measured and possibly broken down
    along Dimensions
    
    **Parameters:**
    
    * **name** - (*str*) The name of the field
    * **type** - (*str or SQLAlchemy type*) The column type for the field
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field
    * **aggregation** - (*str, optional*) The AggregationType to apply to the
    metric
    * **rounding** - (*int, optional*) If specified, the number of decimal
    places to round to
    * **weighting_metric** - (*str, optional*) A reference to a metric to use
    for weighting when aggregating averages
    * **technical** - (*object, optional*) A Technical object or definition used
    to defined a technical computation to be applied to the metric
    * **required_grain** - (*list of str, optional*) If specified, a list of
    dimensions that must be present in the dimension grain of any report that
    aims to include this metric.
    * **kwargs** - kwargs passed to super class
    
    """

    field_type = FieldTypes.METRIC
    schema = MetricConfigSchema

    def __init__(
        self,
        name,
        type,
        display_name=None,
        description=None,
        aggregation=AggregationTypes.SUM,
        rounding=None,
        weighting_metric=None,
        technical=None,
        required_grain=None,
        **kwargs
    ):
        if weighting_metric:
            raiseifnot(
                aggregation == AggregationTypes.MEAN,
                'Weighting metrics are only supported for "%s" aggregation type'
                % AggregationTypes.MEAN,
            )

        if technical:
            technical = create_technical(technical)

        super(Metric, self).__init__(
            name,
            type,
            display_name=display_name,
            description=description,
            aggregation=aggregation,
            rounding=rounding,
            weighting_metric=weighting_metric,
            technical=technical,
            required_grain=required_grain,
            **kwargs
        )

    def get_ds_expression(self, column, label=True):
        """Get the datasource-level sql expression for this metric
        
        **Parameters:**
        
        * **column** - (*Column*) A SQLAlchemy column that supports this metric
        * **label** - (*bool, optional*) If true, label the expression with the
        field name
        
        """
        expr = column
        aggr = aggregation_to_sqla_func(self.aggregation)
        skip_aggr = False

        ds_formula = column.zillion.field_ds_formula(self.name)

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
                expr = sa.func.SUM(
                    sa.text("1.0") * expr * sa.text(w_column_name)
                ) / sa.func.SUM(sa.text(w_column_name))
            else:
                expr = aggr(expr)

        if label:
            return expr.label(self.name)
        return expr

    def get_final_select_clause(self, *args, **kwargs):
        """The sql clause used when selecting at the combined query layer"""
        return self.name


class Dimension(Field):
    """Fields that represent attributes of data that are used for grouping or
    filtering"""

    field_type = FieldTypes.DIMENSION
    schema = DimensionConfigSchema


class FormulaField(Field):
    """A field defined by a formula
    
    **Parameters:**
    
    * **name** - (*str*) The name of the field
    * **formula** - (*str*) The formula used to calculate the field
    * **kwargs** - kwargs passed to the super class
    
    """

    repr_attrs = ["name", "formula"]
    schema = FormulaFieldConfigSchema

    def __init__(self, name, formula, **kwargs):
        super(FormulaField, self).__init__(name, None, formula=formula, **kwargs)

    def get_formula_fields(self, warehouse, depth=0, adhoc_fms=None):
        """Get the fields that are part of this field's formula
        
        **Parameters:**
        
        * **warehouse** - (*Warehouse*) A zillion warehouse that will contain
        all relevant fields
        * **depth** - (*int, optional*) Track the depth of recursion into the
        formula
        * **adhoc_fms** - (*list, optional*) A list of FieldManagers
        
        **Returns:**
        
        (*set, str*) - The set of all base fields involved in the formula
        calculation, as well as an expanded version of the formula. All fields
        in the expanded formula should be raw fields (i.e. not formula
        fields).
        
        """
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

    def get_ds_expression(self, column, label=True):
        """Raise an error if called on FormulaFields"""
        raise ZillionException("Formula-based Fields do not support get_ds_expression")

    def get_final_select_clause(self, warehouse, adhoc_fms=None):
        """Get a SQL select clause for this formula
        
        **Parameters:**
        
        * **warehouse** - (*Warehouse*) A zillion warehouse that will contain
        all relevant fields
        * **adhoc_fms** - (*list, optional*) A list of FieldManagers
        
        **Returns:**
        
        (*SQLAlchemy clause*) - A compiled sqlalchemy clause for the formula
        
        """
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

    def _check_formula_fields(self, warehouse, adhoc_fms=None):
        """Check that all underlying fields exist in the warehouse"""
        fields, _ = self.get_formula_fields(warehouse, adhoc_fms=adhoc_fms)
        if not fields:
            raise InvalidFieldException(
                "No fields found in formula for field:%s formula:%s"
                % (self.name, self.formula)
            )
        for field in fields:
            warehouse.get_field(field, adhoc_fms=adhoc_fms)


class FormulaDimension(FormulaField):
    """A dimension defined by a formula
    
    **Parameters:**
    
    * **name** - (*str*) The name of the dimension
    * **formula** - (*str*) The formula used to calculate the dimension
    * **kwargs** - kwargs passed to super class
    
    """

    field_type = FieldTypes.DIMENSION

    def __init__(self, name, formula, **kwargs):
        # super(FormulaDimension, self).__init__(
        #     name,
        #     formula,
        #     **kwargs
        # )
        raise InvalidFieldException("FormulaDimensions are not currently supported")


class FormulaMetric(FormulaField):
    """A metric defined by a formula
    
    **Parameters:**
    
    * **name** - (*str*) The name of the metric
    * **formula** - (*str*) The formula used to calculate the metric
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field
    * **aggregation** - (*str, optional*) The AggregationType to apply to the
    metric
    * **rounding** - (*int, optional*) If specified, the number of decimal
    places to round to
    * **weighting_metric** - (*str, optional*) A reference to a metric to use
    for weighting when aggregating averages
    * **technical** - (*object, optional*) A Technical object or definition used
    to defined a technical computation to be applied to the metric
    * **required_grain** - (*list of str, optional*) If specified, a list of
    dimensions that must be present in the dimension grain of any report that
    aims to include this metric.
    * **kwargs** - kwargs passed to super class
    
    """

    repr_attrs = ["name", "formula", "aggregation", "technical"]
    field_type = FieldTypes.METRIC
    schema = FormulaMetricConfigSchema

    def __init__(
        self,
        name,
        formula,
        display_name=None,
        description=None,
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
            display_name=display_name,
            description=description,
            aggregation=aggregation,
            rounding=rounding,
            weighting_metric=weighting_metric,
            technical=technical,
            required_grain=required_grain,
            **kwargs
        )


class AdHocField(FormulaField):
    """An AdHoc representation of a field"""

    @classmethod
    def create(cls, obj):
        """Copy this AdHocField"""
        schema = AdHocFieldSchema()
        field_def = schema.load(obj)
        return cls(field_def["name"], field_def["formula"])


class AdHocMetric(FormulaMetric):
    """An AdHoc representation of a Metric

    **Parameters:**

    * **name** - (*str*) The name of the metric
    * **formula** - (*str*) The formula used to calculate the metric
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field
    * **technical** - (*object, optional*) A Technical object or definition
    used to defined a technical computation to be applied to the metric
    * **rounding** - (*int, optional*) If specified, the number of decimal
    places to round to
    * **required_grain** - (*list of str, optional*) If specified, a list of
    dimensions that must be present in the dimension grain of any report
    that aims to include this metric.

    """

    schema = AdHocMetricSchema

    def __init__(
        self,
        name,
        formula,
        display_name=None,
        description=None,
        technical=None,
        rounding=None,
        required_grain=None,
    ):
        """Init an AdHoc representation of a Metric"""
        super(AdHocMetric, self).__init__(
            name,
            formula,
            display_name=display_name,
            description=description,
            technical=technical,
            rounding=rounding,
            required_grain=required_grain,
        )

    @classmethod
    def create(cls, obj):
        """Create an AdHocMetric from an AdHocMetricSchema dict"""
        schema = AdHocMetricSchema()
        field_def = schema.load(obj)
        return cls(
            field_def["name"],
            field_def["formula"],
            display_name=field_def["display_name"],
            description=field_def["description"],
            technical=field_def["technical"],
            rounding=field_def["rounding"],
            required_grain=field_def["required_grain"],
        )


class AdHocDimension(AdHocField):
    """An AdHoc representation of a Dimension"""

    field_type = FieldTypes.DIMENSION


def create_metric(metric_def):
    """Create a Metric object from a dict of params
    
    **Parameters:**
    
    * **metric_def** - (*dict*) A dict of params to init a metric. If a formula
    param is present a FormulaMetric will be created.
    
    """
    if "formula" in metric_def:
        metric = FormulaMetric.from_config(metric_def)
    else:
        metric = Metric.from_config(metric_def)
    return metric


def create_dimension(dim_def):
    """Create a Dimension object from a dict of params
    
    **Parameters:**
    
    * **dim_def** - (*dict*) A dict of params to init a Dimension
    
    """
    if "formula" in dim_def:
        raise InvalidFieldException("FormulaDimensions are not currently supported")
    return Dimension.from_config(dim_def)


class FieldManagerMixin:
    """An interface for managing fields (metrics and dimensions) stored on an
    object.
    
    **Attributes:**
    
    * **metrics_attr** - (*str*) The name of the attribute where metrics are
    stored
    * **dimensions_attr** - (*str*) The name of the attribute where dimensions
    are stored
    
    """

    metrics_attr = "_metrics"
    dimensions_attr = "_dimensions"

    def get_child_field_managers(self):
        """Get a list of child FieldManagers"""
        return []

    def get_field_managers(self, adhoc_fms=None):
        """Get a list of all child FieldManagers including adhoc"""
        return self.get_child_field_managers() + (adhoc_fms or [])

    def get_direct_metrics(self):
        """Get metrics directly stored on this FieldManager"""
        return getattr(self, self.metrics_attr)

    def get_direct_dimensions(self):
        """Get dimensions directly stored on this FieldManager"""
        return getattr(self, self.dimensions_attr)

    def directly_has_metric(self, name):
        """Check if this FieldManager directly stores this metric"""
        return name in getattr(self, self.metrics_attr)

    def directly_has_dimension(self, name):
        """Check if this FieldManager directly stores this dimension"""
        return name in getattr(self, self.dimensions_attr)

    def directly_has_field(self, name):
        """Check if this FieldManager directly stores this field"""
        return name in getattr(self, self.metrics_attr) or name in getattr(
            self, self.dimensions_attr
        )

    def print_metrics(self, indent=None):
        """Print all metrics in this FieldManager"""
        print(format_msg(getattr(self, self.metrics_attr), label=None, indent=indent))

    def print_dimensions(self, indent=None):
        """Print all dimensions in this FieldManager"""
        print(
            format_msg(getattr(self, self.dimensions_attr), label=None, indent=indent)
        )

    def has_metric(self, name, adhoc_fms=None):
        """Check whether a metric is contained in this FieldManager"""
        if self.directly_has_metric(name):
            return True
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm.has_metric(name):
                return True
        return False

    def has_dimension(self, name, adhoc_fms=None):
        """Check whether a dimension is contained in this FieldManager"""
        if self.directly_has_dimension(name):
            return True
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm.has_dimension(name):
                return True
        return False

    def has_field(self, name, adhoc_fms=None):
        """Check whether a field is contained in this FieldManager"""
        if self.directly_has_field(name):
            return True
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm.has_field(name):
                return True
        return False

    def get_metric(self, obj, adhoc_fms=None):
        """Get a reference to a metric on this FieldManager. If the object
        passed is a dict it is expected to defined an AdHocMetric."""
        if isinstance(obj, str):
            if self.directly_has_metric(obj):
                return getattr(self, self.metrics_attr)[obj]
            for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
                if fm.has_metric(obj):
                    return fm.get_metric(obj)
            raise InvalidFieldException("Invalid metric name: %s" % obj)

        if isinstance(obj, dict):
            metric = AdHocMetric.create(obj)
            raiseif(
                self.has_metric(metric.name, adhoc_fms=adhoc_fms),
                "AdHocMetric can not use name of an existing metric: %s" % metric.name,
            )
            metric._check_formula_fields(self, adhoc_fms=adhoc_fms)
            return metric

        raise InvalidFieldException("Invalid metric object: %s" % obj)

    def get_dimension(self, obj, adhoc_fms=None):
        """Get a reference to a dimension on this FieldManager"""
        if isinstance(obj, str):
            if self.directly_has_dimension(obj):
                return getattr(self, self.dimensions_attr)[obj]
            for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
                if fm.has_dimension(obj):
                    return fm.get_dimension(obj)
            raise InvalidFieldException("Invalid dimension name: %s" % obj)

        if isinstance(obj, dict):
            raise InvalidFieldException("AdHocDimensions are not currently supported")
            # dim = AdHocDimension.create(obj)
            # raiseif(
            #     self.has_dimension(dim.name, adhoc_fms=adhoc_fms),
            #     "AdHocDimension can not use name of an existing dimension: %s" % dim.name
            # )
            # return dim

        raise InvalidFieldException("Invalid dimension object: %s" % obj)

    def get_field(self, obj, adhoc_fms=None):
        """Get a refence to a field on this FieldManager"""
        if isinstance(obj, str):
            if self.has_metric(obj, adhoc_fms=adhoc_fms):
                return self.get_metric(obj, adhoc_fms=adhoc_fms)
            if self.has_dimension(obj, adhoc_fms=adhoc_fms):
                return self.get_dimension(obj, adhoc_fms=adhoc_fms)
            raise InvalidFieldException("Invalid field name: %s" % obj)

        if isinstance(obj, dict):
            raise InvalidFieldException("AdHocFields are not currently supported")
            # field = AdHocField.create(obj)
            # raiseif(
            #     self.has_field(field.name, adhoc_fms=adhoc_fms),
            #     "AdHocField can not use name of an existing field: %s" % field.name,
            # )
            # return field

        raise InvalidFieldException("Invalid field object: %s" % obj)

    def get_field_instances(self, field, adhoc_fms=None):
        """Get a dict of FieldManagers (including child and adhoc FMs) that
        support a field"""
        instances = {}

        if self.directly_has_field(field):
            instances[self] = self.get_field(field)

        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm.has_field(field):
                instances.update(fm.get_field_instances(field))

        if not instances:
            raise InvalidFieldException("Invalid field name: %s" % field)
        return instances

    def get_metrics(self, adhoc_fms=None):
        """Get a dict of all metrics supported by this FieldManager"""
        metrics = {}
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm_metrics = fm.get_metrics()
            metrics.update(fm_metrics)
        metrics.update(getattr(self, self.metrics_attr))
        return metrics

    def get_dimensions(self, adhoc_fms=None):
        """Get a dict of all dimensions supported by this FieldManager"""
        dimensions = {}
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm_dimensions = fm.get_dimensions()
            dimensions.update(fm_dimensions)
        dimensions.update(getattr(self, self.dimensions_attr))
        return dimensions

    def get_fields(self, adhoc_fms=None):
        """Get a dict of all fields supported by this FieldManager"""
        fields = {}
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm_fields = fm.get_fields()
            fields.update(fm_fields)
        fields.update(getattr(self, self.metrics_attr))
        fields.update(getattr(self, self.dimensions_attr))
        return fields

    def get_direct_metric_configs(self):
        """Get a dict of metric configs directly supported by this
        FieldManager"""
        return {f.name: f.to_config() for f in self.get_direct_metrics().values()}

    def get_direct_dimension_configs(self):
        """Get a dict of dimension configs directly supported by this
        FieldManager"""
        return {f.name: f.to_config() for f in self.get_direct_dimensions().values()}

    def get_metric_configs(self, adhoc_fms=None):
        """Get a dict of all metric configs supported by this FieldManager"""
        configs = {}
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm_configs = fm.get_metric_configs()
            configs.update(fm_configs)
        configs.update(self.get_direct_metric_configs())
        return configs

    def get_dimension_configs(self, adhoc_fms=None):
        """Get a dict of all dimension configs supported by this FieldManager"""
        configs = {}
        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm_configs = fm.get_dimension_configs()
            configs.update(fm_configs)
        configs.update(self.get_direct_dimension_configs())
        return configs

    def get_metric_names(self, adhoc_fms=None):
        """Get a set of metric names supported by this FieldManager"""
        return set(self.get_metrics(adhoc_fms=adhoc_fms).keys())

    def get_dimension_names(self, adhoc_fms=None):
        """Get a set of dimension names supported by this FieldManager"""
        return set(self.get_dimensions(adhoc_fms=adhoc_fms).keys())

    def get_field_names(self, adhoc_fms=None):
        """Get a set of field names supported by this FieldManager"""
        return set(self.get_fields(adhoc_fms=adhoc_fms).keys())

    def add_metric(self, metric, force=False):
        """Add a reference to a metric to this FieldManager"""
        if self.has_dimension(metric.name):
            raise InvalidFieldException(
                "Trying to add metric with same name as a dimension: %s" % metric.name
            )
        if (not force) and self.has_metric(metric.name):
            warn("Metric %s already exists on %s" % (metric.name, self))
            return
        getattr(self, self.metrics_attr)[metric.name] = metric

    def add_dimension(self, dimension, force=False):
        """Add a reference to a dimension to this FieldManager"""
        if self.has_metric(dimension.name):
            raise InvalidFieldException(
                "Trying to add dimension with same name as a metric: %s"
                % dimension.name
            )
        if (not force) and self.has_dimension(dimension.name):
            warn("Dimension %s already exists on %s" % (dimension.name, self))
            return
        getattr(self, self.dimensions_attr)[dimension.name] = dimension

    def _populate_global_fields(self, config, force=False):
        """Populate fields on this FieldManager from a config
        
        **Parameters:**
        
        * **config** - (*dict*) A config containing lists of metrics and/or
        dimensions to add to this FieldManager
        * **force** - (*bool, optional*) If true, overwrite fields that already
        exist
        
        """
        formula_metrics = []
        formula_dims = []

        for metric_def in config.get("metrics", []):
            if isinstance(metric_def, dict):
                metric = create_metric(metric_def)
            else:
                raiseifnot(
                    isinstance(metric_def, (Metric, FormulaMetric)),
                    "Metric definition must be a dict-like object or a Metric object",
                )
                metric = metric_def

            if isinstance(metric, FormulaMetric):
                formula_metrics.append(metric)  # These get added later
            else:
                self.add_metric(metric, force=force)

        for dim_def in config.get("dimensions", []):
            if isinstance(dim_def, dict):
                dim = create_dimension(dim_def)
            else:
                raiseifnot(
                    isinstance(dim_def, Dimension),
                    "Dimension definition must be a dict-like object or a Dimension object",
                )
                dim = dim_def

            if isinstance(dim, FormulaDimension):
                # formula_dims.append(dim) # These get added later
                raise InvalidFieldException(
                    "FormulaDimensions are not currently supported"
                )
            self.add_dimension(dim, force=force)

        # Defer formulas so params can be checked against existing fields
        for metric in formula_metrics:
            metric._check_formula_fields(self)
            self.add_metric(metric, force=force)

        for dim in formula_dims:
            dim._check_formula_fields(self)
            self.add_dimension(dim, force=force)

    def _find_field_sources(self, field, adhoc_fms=None):
        """Get a list of FieldManagers supporting a field. This will search the
        current FieldManager and all child/adhoc FMs.
        
        **Parameters:**
        
        * **field** - (*str*) The name of a field
        * **adhoc_fms** - (*list, optional*) A list of FieldManagers
        
        **Returns:**
        
        (*list*) - A list of FieldManagers that support the field
        
        """
        sources = []
        if self.directly_has_field(field):
            sources.append(self)

        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            if fm.directly_has_field(field):
                sources.append(fm)
        return sources


def get_table_metrics(fm, table, adhoc_fms=None):
    """Get a list of metrics supported by a table
    
    **Parameters:**
    
    * **fm** - (*FieldManager*) An object supporting the FieldManager interface
    * **table** - (*SQLAlchemy Table*) The table to get a list of supported
    dimensions for
    * **adhoc_fms** - (*list, optional*) AdHoc FieldManagers relevant to this
    request
    
    **Returns:**
    
    (*set*) - A set of metric names
    
    """
    metrics = set()
    for col in table.c:
        if not is_active(col):
            continue
        for field in col.zillion.get_field_names():
            if fm.has_metric(field, adhoc_fms=adhoc_fms):
                metrics.add(field)
    return metrics


def get_table_dimensions(fm, table, adhoc_fms=None):
    """Get a list of dimensions supported by a table
    
    **Parameters:**
    
    * **fm** - (*FieldManager*) An object supporting the FieldManager interface
    * **table** - (*SQLAlchemy Table*) The table to get a list of supported
    dimensions for
    * **adhoc_fms** - (*list, optional*) AdHoc FieldManagers relevant to this
    request
    
    **Returns:**
    
    (*set*) - A set of dimension names
    
    """
    dims = set()
    for col in table.c:
        if not is_active(col):
            continue
        for field in col.zillion.get_field_names():
            if fm.has_dimension(field, adhoc_fms=adhoc_fms):
                dims.add(field)
    return dims


def get_table_fields(table):
    """Get a list of field names supported by a table
    
    **Parameters:**
    
    * **table** - (*SQLAlchemy Table*) The table to get a list of supported
    fields for
    
    **Returns:**
    
    (*set*) - A set of field names
    
    """
    fields = set()
    for col in table.c:
        if not is_active(col):
            continue
        for field in col.zillion.get_field_names():
            fields.add(field)
    return fields


def get_table_field_column(table, field_name):
    """Return the column within a table that supports a given field
    
    **Parameters:**
    
    * **table** - (*Table*) SQLAlchemy table onject
    * **field_name** - (*str*) The name of a field supported by the table
    
    **Returns:**
    
    (*Column*) - A SQLAlchemy column object
    
    """
    for col in table.c:
        if not is_active(col):
            continue
        for field in col.zillion.get_field_names():
            if field == field_name:
                return col
    raise ZillionException(
        "Field %s inactive or not found in table %s" % (field_name, table.fullname)
    )


def table_field_allows_grain(table, field, grain):
    """Check whether a field in a table is restricted by required_grain
    
    **Parameters:**
    
    * **table** - (*Table*) SQLAlchemy table object
    * **field** - (*str*) The name of a field in the table
    * **grain** - (*list of str*) A list of dimenssions that form the target
    grain
    
    """
    grain = grain or set()
    column = get_table_field_column(table, field)
    if not column.zillion.required_grain:
        return True
    if set(column.zillion.required_grain).issubset(grain):
        return True
    return False


def get_conversions_for_type(coltype):
    """Get all conversions for a particular column type
    
    **Parameters:**
    
    * **coltype** - A SQLAlchemy column type class
    
    **Returns:**
    
    (*dict*) - The conversion map for the given column type. Returns None if
    no conversions are found.
    
    """
    for basetype, convs in TYPE_ALLOWED_CONVERSIONS.items():
        if issubclass(coltype, basetype):
            return convs
    return None


def get_dialect_type_conversions(dialect, column):
    """Get all conversions supported by this column type for this dialect
    
    **Parameters:**
    
    * **dialect** - (*str*) SQLAlchemy dialect name
    * **column** - (*Column*) SQLAlchemy column object
    
    **Returns:**
    
    (*list*) - A list of tuples of (field, conversion formula)
    
    """
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
        raiseif(
            any([x != "" for x in format_args]),
            "Field conversion has non-named format arguments: %s" % conv,
        )
        if format_args:
            conv = conv.format(*[column_fullname(column) for i in format_args])
        results.append((field_def, conv))

    return results


DATETIME_CONVERSION_FIELDS = [
    Dimension("year", "Integer", description="Year"),
    Dimension("quarter", "String(8)", description="Year and quarter (YYYY-QN)"),
    Dimension(
        "quarter_of_year", "SmallInteger", description="Numeric quarter of the year"
    ),
    Dimension("month", "String(8)", description="Year and month (YYYY-MM)"),
    Dimension("month_name", "String(8)", description="Full name of the month"),
    Dimension("month_of_year", "SmallInteger", description="Numeric month of the year"),
    Dimension("date", "String(10)", description="Date string formatted YYYY-MM-DD"),
    Dimension("day_name", "String(10)", description="Full name of a day of the week"),
    Dimension(
        "day_of_week",
        "SmallInteger",
        description="Numeric day of the week (monday = 1)",
    ),
    Dimension("day_of_month", "SmallInteger", description="Numeric day of the month"),
    Dimension("day_of_year", "SmallInteger", description="Numeric day of the year"),
    Dimension(
        "hour",
        "String(20)",
        description="Datetime string rounded to the hour (YYYY-MM-DD HH:00:00)",
    ),
    Dimension("hour_of_day", "SmallInteger", description="Numeric hour of day (0-23)"),
    Dimension(
        "minute",
        "String(20)",
        description="Datetime string rounded to the minute (YYYY-MM-DD HH:MM:00)",
    ),
    Dimension(
        "minute_of_hour", "SmallInteger", description="Numeric minute of the hour"
    ),
    Dimension(
        "datetime",
        "String(20)",
        description="Datetime string formatted YYYY-MM-DD HH:MM:SS",
    ),
    Dimension("unixtime", "BigInteger", description="Unix time in seconds"),
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
