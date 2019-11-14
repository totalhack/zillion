from collections import OrderedDict
import os

from marshmallow import Schema, fields as mfields, ValidationError, validates_schema
from tlbx import dbg, error, json, st
import yaml

from zillion.core import (
    TableTypes,
    AggregationTypes,
    TechnicalTypes,
    parse_technical_string,
    FIELD_ALLOWABLE_CHARS,
    FIELD_ALLOWABLE_CHARS_STR,
)
from zillion.sql_utils import type_string_to_sa_type, InvalidSQLAlchemyTypeString


def load_zillion_config():
    zillion_config_fname = os.environ.get("ZILLION_CONFIG", None)
    if not zillion_config_fname:
        return dict(
            DEBUG=False,
            ZILLION_DB_URL="sqlite:////tmp/zillion.db",
            ADHOC_DATASOURCE_DIRECTORY="/tmp",
            LOAD_TABLE_CHUNK_SIZE=5000,
            IFNULL_PRETTY_VALUE="--",
            DATASOURCE_QUERY_MODE="sequential",
            DATASOURCE_QUERY_TIMEOUT=None,
        )
    return yaml.safe_load(open(zillion_config_fname))


zillion_config = load_zillion_config()


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
        error("Schema Validation Error: %s" % schema)
        print(json.dumps(str(e), indent=2))
        raise
    return result


def load_warehouse_config(filename, preserve_order=False):
    file_schema = WarehouseConfigSchema()
    config = parse_schema_file(
        filename, file_schema, object_pairs_hook=OrderedDict if preserve_order else None
    )
    return config


def is_valid_table_type(val):
    if val in TableTypes:
        return True
    raise ValidationError("Invalid table type: %s" % val)


def is_valid_field_name(val):
    if val is None:
        raise ValidationError("Field name can not be null")
    if set(val) <= FIELD_ALLOWABLE_CHARS:
        return True
    raise ValidationError(
        'Field name "%s" has invalid characters. Allowed: %s'
        % (val, FIELD_ALLOWABLE_CHARS_STR)
    )


def is_valid_sqlalchemy_type(val):
    if val is not None:
        try:
            sa_type = type_string_to_sa_type(val)
        except InvalidSQLAlchemyTypeString:
            raise ValidationError("Invalid table type: %s" % val)
    return True


def is_valid_aggregation(val):
    if val in AggregationTypes:
        return True
    raise ValidationError("Invalid aggregation: %s" % val)


def is_valid_column_field_config(val):
    if isinstance(val, str):
        return True
    if isinstance(val, dict):
        schema = ColumnFieldConfigSchema()
        schema.load(val)
        return True
    raise ValidationError("Invalid column field config: %s" % val)


def is_valid_technical_type(val):
    if val in TechnicalTypes:
        return True
    raise ValidationError("Invalid technical type: %s" % val)


def is_valid_technical(val):
    if isinstance(val, str):
        val = parse_technical_string(val)
    elif not isinstance(val, dict):
        raise ValidationError("Invalid technical: %s" % val)
    schema = TechnicalInfoSchema()
    val = schema.load(val)
    return True


class BaseSchema(Schema):
    class Meta:
        # Use the json module as imported from tlbx
        json_module = json


class TechnicalInfoSchema(BaseSchema):
    type = mfields.String(required=True, validate=is_valid_technical_type)
    window = mfields.Integer(required=True)
    min_periods = mfields.Integer(default=1, missing=1)
    center = mfields.Boolean(default=False, missing=False)


class TechnicalField(mfields.Field):
    def _validate(self, value):
        is_valid_technical(value)
        super(TechnicalField, self)._validate(value)


class AdHocFieldSchema(BaseSchema):
    name = mfields.String(required=True, validate=is_valid_field_name)
    formula = mfields.String(required=True)


class AdHocMetricSchema(AdHocFieldSchema):
    technical = TechnicalField(default=None, missing=None)
    rounding = mfields.Integer(default=None, missing=None)


class ColumnFieldConfigSchema(BaseSchema):
    name = mfields.Str(required=True, validate=is_valid_field_name)
    ds_formula = mfields.Str(required=True)


class ColumnFieldConfigField(mfields.Field):
    def _validate(self, value):
        is_valid_column_field_config(value)
        super(ColumnFieldConfigField, self)._validate(value)


class ColumnInfoSchema(BaseSchema):
    fields = mfields.List(ColumnFieldConfigField())
    allow_type_conversions = mfields.Boolean(default=False, missing=False)
    active = mfields.Boolean(default=True, missing=True)


class ColumnConfigSchema(ColumnInfoSchema):
    pass


class TableTypeField(mfields.Field):
    def _validate(self, value):
        is_valid_table_type(value)
        super(TableTypeField, self)._validate(value)


class TableInfoSchema(BaseSchema):
    type = TableTypeField(required=True)
    autocolumns = mfields.Boolean(default=False, missing=False)
    active = mfields.Boolean(default=True, missing=True)
    parent = mfields.Str(default=None, missing=None)


class TableConfigSchema(TableInfoSchema):
    columns = mfields.Dict(
        keys=mfields.Str(), values=mfields.Nested(ColumnConfigSchema)
    )


class DataSourceConfigSchema(BaseSchema):
    tables = mfields.Dict(keys=mfields.Str(), values=mfields.Nested(TableConfigSchema))


class MetricConfigSchema(BaseSchema):
    name = mfields.String(required=True, validate=is_valid_field_name)
    type = mfields.String(default=None, missing=None, validate=is_valid_sqlalchemy_type)
    aggregation = mfields.String(
        default=AggregationTypes.SUM,
        missing=AggregationTypes.SUM,
        validate=is_valid_aggregation,
    )
    rounding = mfields.Integer(default=None, missing=None)
    weighting_metric = mfields.Str(default=None, missing=None)
    formula = mfields.String(default=None, missing=None)
    technical = TechnicalField(default=None, missing=None)

    @validates_schema(skip_on_field_errors=True)
    def validate_object(self, data, **kwargs):
        if (not data.get("type", None)) and (not data.get("formula", None)):
            raise ValidationError(
                "Either type or formula must be specified for metric: %s" % data
            )

        if data["weighting_metric"] and not data["aggregation"] == AggregationTypes.AVG:
            raise ValidationError(
                'only "%s" aggregation type is allowed with weighting metrics: %s'
                % (AggregationTypes.AVG, data)
            )


class DimensionConfigSchema(BaseSchema):
    name = mfields.String(required=True, validate=is_valid_field_name)
    type = mfields.String(default=None, missing=None, validate=is_valid_sqlalchemy_type)
    formula = mfields.String(default=None, missing=None)


class WarehouseConfigSchema(BaseSchema):
    metrics = mfields.List(mfields.Nested(MetricConfigSchema))
    dimensions = mfields.List(mfields.Nested(DimensionConfigSchema))
    datasources = mfields.Dict(
        keys=mfields.Str(), values=mfields.Nested(DataSourceConfigSchema), required=True
    )
