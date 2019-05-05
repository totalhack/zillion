from collections import OrderedDict
import re

from marshmallow import Schema, fields as mfields, ValidationError

from sqlaw.core import TableTypes, AggregationTypes
from sqlaw.sql_utils import (type_string_to_sa_type,
                             InvalidSQLAlchemyTypeString)
from sqlaw.utils import (dbg,
                         error,
                         json,
                         st,
                         get_class_var_values,
                         initializer)

FIELD_NAME_REGEX = '[0-9a-zA-Z_]+'

def parse_schema_file(filename, schema, object_pairs_hook=None):
    """Parse a marshmallow schema file"""
    f = open(filename)
    raw = f.read()
    f.close()
    try:
        # This does the schema check, but has a bug in object_pairs_hook so order is not preserved
        result = schema.loads(raw)
        result = json.loads(raw, object_pairs_hook=object_pairs_hook)
    except ValidationError as e:
        error('Schema Validation Error: %s' % schema)
        print(json.dumps(str(e), indent=2))
        raise
    return result

def load_config(filename, preserve_order=False):
    file_schema = SQLAWConfigSchema()
    config = parse_schema_file(filename, file_schema,
                               object_pairs_hook=OrderedDict if preserve_order else None)
    return config

def is_valid_table_type(val):
    if val in get_class_var_values(TableTypes):
        return
    raise ValidationError('Invalid table type: %s' % val)

class TableTypeField(mfields.Field):
    def _validate(self, value):
        is_valid_table_type(value)
        super(TableTypeField, self)._validate(value)

class BaseSchema(Schema):
    class Meta:
        # Use the json module as imported from utils
        json_module = json

class ColumnInfoSchema(BaseSchema):
    fields = mfields.List(mfields.Str())
    active = mfields.Boolean(default=True, missing=True)

class ColumnConfigSchema(ColumnInfoSchema):
    pass

class TableInfoSchema(BaseSchema):
    type = TableTypeField(required=True)
    autocolumns = mfields.Boolean(default=False, missing=False)
    active = mfields.Boolean(default=True, missing=True)
    parent = mfields.Str(default=None, missing=None)

class TableConfigSchema(TableInfoSchema):
    columns = mfields.Dict(keys=mfields.Str(), values=mfields.Nested(ColumnConfigSchema))

class DataSourceConfigSchema(BaseSchema):
    tables = mfields.Dict(keys=mfields.Str(), values=mfields.Nested(TableConfigSchema))

def is_valid_field_name(val):
    if val is None:
        raise ValidationError('Field name can not be null')
    if re.match(FIELD_NAME_REGEX, val):
        return True
    raise ValidationError('Field name must satisfy regex "%s": %s' % (FIELD_NAME_REGEX, val))

def is_valid_fact_type(val):
    if val is not None:
        try:
            sa_type = type_string_to_sa_type(val)
        except InvalidSQLAlchemyTypeString:
            raise ValidationError('Invalid table type: %s' % val)
    return True

def is_valid_aggregation(val):
    if val in get_class_var_values(AggregationTypes):
        return True
    raise ValidationError('Invalid aggregation: %s' % val)

# TODO: if type is missing, formula must be supplied and vice-versa
class FactConfigSchema(BaseSchema):
    name = mfields.String(required=True, validate=is_valid_field_name)
    type = mfields.String(default=None, missing=None, validate=is_valid_fact_type)
    aggregation = mfields.String(default=AggregationTypes.SUM,
                                 missing=AggregationTypes.SUM,
                                 validate=is_valid_aggregation)
    rounding = mfields.Integer(default=None, missing=None)
    formula = mfields.String(default=None, missing=None)

class DimensionConfigSchema(BaseSchema):
    name = mfields.String(required=True, validate=is_valid_field_name)
    type = mfields.String(default=None, missing=None)
    formula = mfields.String(default=None, missing=None)

class SQLAWConfigSchema(BaseSchema):
    facts = mfields.List(mfields.Nested(FactConfigSchema))
    dimensions = mfields.List(mfields.Nested(DimensionConfigSchema))
    datasources = mfields.Dict(keys=mfields.Str(), values=mfields.Nested(DataSourceConfigSchema), required=True)
