from collections import OrderedDict

from marshmallow import Schema, fields, ValidationError

from sqlaw.core import TABLE_TYPES, COLUMN_TYPES
from sqlaw.utils import (dbg,
                         error,
                         json,
                         st,
                         initializer)

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
        error('Schema Validation Error')
        print(json.dumps(str(e), indent=2))
        raise
    return result

def load_config(filename, preserve_order=False):
    file_schema = SQLAWConfigSchema()
    config = parse_schema_file(filename, file_schema,
                               object_pairs_hook=OrderedDict if preserve_order else None)
    return config

def is_valid_table_type(val):
    if val in TABLE_TYPES:
        return
    raise ValidationError('Invalid table type: %s' % val)

class TableTypeField(fields.Field):
    def _validate(self, value):
        is_valid_table_type(value)
        super(TableTypeField, self)._validate(value)

def is_valid_column_type(val):
    if val in COLUMN_TYPES:
        return
    raise ValidationError('Invalid column type: %s' % val)

class ColumnTypeField(fields.Field):
    def _validate(self, value):
        is_valid_column_type(value)
        super(ColumnTypeField, self)._validate(value)

class BaseSchema(Schema):
    class Meta:
        # Use the json module as imported from utils
        json_module = json

class ColumnInfoSchema(BaseSchema):
    fieldname = fields.Str()
    type = ColumnTypeField(default='auto', missing='auto')
    active = fields.Boolean(default=True, missing=True)

class ColumnConfigSchema(ColumnInfoSchema):
    pass

class TableInfoSchema(BaseSchema):
    type = TableTypeField(required=True)
    autocolumns = fields.Boolean(default=False, missing=False)
    active = fields.Boolean(default=True, missing=True)
    parent = fields.Str(default=None, missing=None)

class TableConfigSchema(TableInfoSchema):
    columns = fields.Dict(keys=fields.Str(), values=fields.Nested(ColumnConfigSchema))

class DataSourceConfigSchema(BaseSchema):
    tables = fields.Dict(keys=fields.Str(), values=fields.Nested(TableConfigSchema))

class SQLAWConfigSchema(BaseSchema):
    datasources = fields.Dict(keys=fields.Str(), values=fields.Nested(DataSourceConfigSchema), required=True)
