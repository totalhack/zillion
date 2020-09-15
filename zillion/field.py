import pandas as pd
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
    default_field_display_name,
)
from zillion.core import *
from zillion.dialects import *
from zillion.model import zillion_engine, DimensionValues
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
# This default warehouse ID is used if no ID has been populated
# on the Warehouse when checking dimension values.
FIELD_VALUE_DEFAULT_WAREHOUSE_ID = 0
FIELD_VALUE_CHECK_OPERATIONS = set(["=", "!=", "in", "not in"])


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
    * **meta** - (*dict, optional*) A dict of additional custom attributes
    * **kwargs** - Additional attributes stored on the field object
    
    **Attributes:**
    
    * **name** - (*str*) The name of the field
    * **type** - (*str*) A string representing the generic SQLAlchemy type
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field
    * **meta** - (*dict, optional*) A dict of additional custom attributes
    * **sa_type** - (*SQLAlchemy type*) If a dialect-specific type object
    is passed in on init it will be coerced to a generic type.
    * **field_type** - (*str*) A valid FieldType string
    * **schema** - (*Marshmallow schema*) A FieldConfigSchema class
    
    """

    repr_attrs = ["name", "type"]
    field_type = None
    schema = FieldConfigSchema

    @initializer
    def __init__(
        self, name, type, display_name=None, description=None, meta=None, **kwargs
    ):
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

    def get_ds_expression(self, column, label=True, ignore_formula=False):
        """Get the datasource-level sql expression for this field
        
        **Parameters:**
        
        * **column** - (*Column*) A SQLAlchemy column that supports this field
        * **label** - (*bool, optional*) If true, label the expression with the
        field name
        * **ignore_formula** - (*bool, optional*) If true, don't apply any available
        datasource formulas
        
        """
        ds_formula = column.zillion.field_ds_formula(self.name)
        if ignore_formula or (not ds_formula):
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
    * **meta** - (*dict, optional*) A dict of additional custom attributes
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
        meta=None,
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
            meta=meta,
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
    filtering
    
    **Parameters:**
    
    * **name** - (*str*) The name of the field
    * **type** - (*str or SQLAlchemy type*) The column type for the field.
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field
    * **values** - (*str or list, optional*) A list of allowed dimension
    values or a name of a callable to provide a list of values
    * **sorter** - (*str, optional*) A reference to an importable callable
    that accepts three arguments: (warehouse ID, dimension object, values).
    Currently values is a pandas Series and the callable is expected to
    return a Series. See `zillion.field.sort_by_value_order` for an example.
    * **meta** - (*dict, optional*) A dict of additional custom attributes
    * **kwargs** - Additional attributes stored on the field object
    
    """

    field_type = FieldTypes.DIMENSION
    schema = DimensionConfigSchema

    @initializer
    def __init__(
        self,
        name,
        type,
        display_name=None,
        description=None,
        values=None,
        sorter=None,
        meta=None,
        **kwargs
    ):
        if values and isinstance(values, list):
            self.values = set(self.values)

        super(Dimension, self).__init__(
            name,
            type,
            display_name=display_name,
            description=description,
            values=values,
            sorter=sorter,
            meta=meta,
            **kwargs
        )

    def get_values(self, warehouse_id):
        """Get allowed values for this Dimension
        
        **Parameters:**
        
        * **warehouse_id** - (*int*) A zillion warehouse ID
        
        **Returns:**
        
        (*list or None*) - A list of valid values or None if no value
        restrictions have been set.
        
        """
        if self.values is None:
            return None
        if isinstance(self.values, str):
            func = import_object(self.values)
            return func(warehouse_id, self)
        return self.values

    def is_valid_value(self, warehouse_id, value, ignore_none=True):
        """Check if a value is allowed for this Dimension
        
        **Parameters:**
        
        * **warehouse_id** - (*int*) A zillion warehouse ID
        * **value** - (*any*) Check if this value is valid
        * **ignore_none** - (*bool*) If True, consider value=None
        to always be valid.
         
        **Returns:**
        
        (*bool*) - True if the dimension value is valid
        
        """
        if ignore_none and value is None:
            return True
        values = self.get_values(warehouse_id)
        if not values:
            return True
        return value in values

    def sort(self, warehouse_id, values):
        """Sort the given dimension values according to the sorter
        
        **Parameters:**
        
        * **warehouse_id** - (*int*) A zillion warehouse ID
        * **values** - (*Series*) A pandas Series of values to sort
        
        **Returns:**
        
        (*Series*) - A pandas Series representing the sort order
        
        """
        raiseifnot(self.sorter, "No sorter defined on Dimension")
        func = import_object(self.sorter)
        return func(warehouse_id, self, values)


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

    def get_ds_expression(self, *args, **kwargs):
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
    * **meta** - (*dict, optional*) A dict of additional custom attributes
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
        meta=None,
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
            meta=meta,
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
    * **meta** - (*dict, optional*) A dict of additional custom attributes
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
        meta=None,
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
            meta=meta,
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
            meta=field_def["meta"],
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

    def get_direct_fields(self):
        """Get a dict of all fields directly supported by this FieldManager"""
        fields = {}
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

    def _add_default_display_names(self, adhoc_fms=None, display_names=None):
        """Populate default display names on all fields"""
        fields = self.get_direct_fields()
        display_names = display_names or {}
        for field, config in fields.items():
            if display_names.get(field, None) and not config.display_name:
                # Make sure we use consistent names if one was specified
                # in a parent field manager.
                config.display_name = display_names[field]
            else:
                default = default_field_display_name(field)
                config.display_name = config.display_name or default
                display_names[field] = config.display_name

        for fm in self.get_field_managers(adhoc_fms=adhoc_fms):
            fm._add_default_display_names(display_names=display_names)

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


def values_from_db(warehouse_id, field):
    """Get allowed field values from the dimension_values table. If
    warehouse_id is `None` the warehouse_id is defaulted to the value
    of `zillion.field.FIELD_VALUE_DEFAULT_WAREHOUSE_ID`. This allows
    pulling dimension values even when a `Warehouse` has not been saved.
    
    **Parameters:**
    
    * **warehouse_id** - (*int*) A zillion warehouse ID
    * **field** - (*Field*) A zillion Dimension object

    **Returns:**
    
    (*list or None*) - A list of valid values or None if no row
    is found for this dimension.
    
    """
    if warehouse_id is None:
        warehouse_id = FIELD_VALUE_DEFAULT_WAREHOUSE_ID

    s = sa.select(DimensionValues.c).where(
        sa.and_(
            DimensionValues.c.warehouse_id == warehouse_id,
            DimensionValues.c.name == field.name,
        )
    )
    conn = zillion_engine.connect()
    try:
        result = conn.execute(s)
        row = result.fetchone()
        if not row:
            return None
        return json.loads(row["values"])
    finally:
        conn.close()


def sort_by_value_order(warehouse_id, field, values):
    """Sort values by the order of the value list defined on the field
    
    **Parameters:**
    
    * **warehouse_id** - (*int*) A zillion warehouse ID
    * **field** - (*Field*) A zillion Field object
    * **values** - (*Series*) A pandas Series to sort
    
    **Returns:**
    
    (*Series*) - A pandas Series representing the sort order. If no value
    list is found for the field, the input values are returned as is.
        
    """
    value_order = field.get_values(warehouse_id)
    if not value_order:
        return values
    mapping = {value: order for order, value in enumerate(value_order)}
    return values.map(mapping)


def get_conversions_for_type(coltype):
    """Get all conversions for a particular column type
    
    **Parameters:**
    
    * **coltype** - A SQLAlchemy column type class
    
    **Returns:**
    
    (*dict*) - The conversion map for the given column type. Returns None if
    no conversions are found.
    
    """
    for basetype, fields in TYPE_ALLOWED_CONVERSIONS.items():
        if issubclass(coltype, basetype):
            return fields
    return None


def replace_non_named_formula_args(formula, column):
    """Do formula arg replacement but raise an error if any named args are present"""
    format_args = get_string_format_args(formula)
    raiseif(
        any([x != "" for x in format_args]),
        "Formula has unexpected named format arguments: %s" % formula,
    )
    if format_args:
        formula = formula.format(*[column_fullname(column) for i in format_args])
    return formula


def get_dialect_type_conversions(dialect, column):
    """Get all conversions supported by this column type for this dialect
    
    **Parameters:**
    
    * **dialect** - (*str*) SQLAlchemy dialect name
    * **column** - (*Column*) SQLAlchemy column object
    
    **Returns:**
    
    (*list*) - A list of dicts containing datasource formulas and criteria
    conversions for each field this column can be converted to
    
    """
    coltype = type(column.type)
    conv_fields = get_conversions_for_type(coltype)
    if not conv_fields:
        return []

    results = []
    for field in conv_fields:
        field_name = field.name
        dialect_field_convs = DIALECT_CONVERSIONS[dialect].get(field_name, None)
        if not dialect_field_convs:
            continue

        if isinstance(dialect_field_convs, str):
            ds_formula = dialect_field_convs
            ds_criteria_conversions = None
        else:
            ds_formula = dialect_field_convs.get("ds_formula", None)
            ds_criteria_conversions = dialect_field_convs.get(
                "ds_criteria_conversions", None
            )

        raiseifnot(
            ds_formula or ds_criteria_conversions,
            "One of ds_formula or ds_criteria_conversions must be set on dialect conversions for %s/%s"
            % (dialect, field_name),
        )

        if ds_formula:
            ds_formula = replace_non_named_formula_args(ds_formula, column)

        results.append(
            dict(
                field=field,
                ds_formula=ds_formula,
                ds_criteria_conversions=ds_criteria_conversions,
            )
        )

    return results


DATETIME_CONVERSION_FIELDS = [
    Dimension("year", "Integer", description="Year"),
    Dimension("quarter", "String(8)", description="Year and quarter (YYYY-QN)"),
    Dimension(
        "quarter_of_year", "SmallInteger", description="Numeric quarter of the year"
    ),
    Dimension("month", "String(8)", description="Year and month (YYYY-MM)"),
    Dimension(
        "month_name",
        "String(8)",
        description="Full name of the month",
        values=[
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ],
        sorter="zillion.field.sort_by_value_order",
    ),
    Dimension("month_of_year", "SmallInteger", description="Numeric month of the year"),
    Dimension("date", "String(10)", description="Date string formatted YYYY-MM-DD"),
    Dimension(
        "day_name",
        "String(10)",
        description="Full name of a day of the week",
        values=[
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ],
        sorter="zillion.field.sort_by_value_order",
    ),
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
    # TODO: not fully tested/supported yet, ignore for now
    # Dimension("unixtime", "BigInteger", description="Unix time in seconds"),
]

DATE_CONVERSION_FIELDS = []
for _dim in DATETIME_CONVERSION_FIELDS:
    if _dim.name == "hour":
        break
    DATE_CONVERSION_FIELDS.append(_dim)

# Map all dialect-specific type conversions, including ds_formulas that can
# be applied directly to columns (i.e. DATE(some_datetime) == '2020-01-01')
# and ds_criteria_conversions that can apply conversions to criteria values
# instead.
#
# Note: For date types: somewhat adhering to ISO 8601, but ignoring the "T"
# between the date/time and not including timezone offsets for now because
# zillion assumes everything is in the same timezone (or the datasource
# formulas take care of aligning timezones).
DIALECT_CONVERSIONS = {
    "sqlite": SQLITE_DIALECT_CONVERSIONS,
    "mysql": MYSQL_DIALECT_CONVERSIONS,
    "postgresql": POSTGRESQL_DIALECT_CONVERSIONS,
}

TYPE_ALLOWED_CONVERSIONS = {
    sa.DateTime: DATETIME_CONVERSION_FIELDS,
    sa.DATETIME: DATETIME_CONVERSION_FIELDS,
    sa.TIMESTAMP: DATETIME_CONVERSION_FIELDS,
    sa.Date: DATE_CONVERSION_FIELDS,
    sa.DATE: DATE_CONVERSION_FIELDS,
}
