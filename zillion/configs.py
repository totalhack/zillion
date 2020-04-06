from collections import OrderedDict
import os
import string

from marshmallow import (
    Schema,
    fields as mfields,
    ValidationError,
    validates_schema,
    pre_load,
    EXCLUDE,
    INCLUDE,
    RAISE,
)
from pandas.io.common import get_filepath_or_buffer
from tlbx import dbg, error, json, st, initializer, MappingMixin, PrintMixin
import yaml

from zillion.core import (
    FieldTypes,
    TableTypes,
    AggregationTypes,
    TechnicalTypes,
    InvalidTechnicalException,
)
from zillion.sql_utils import (
    column_fullname,
    type_string_to_sa_type,
    InvalidSQLAlchemyTypeString,
)


FIELD_ALLOWABLE_CHARS_STR = (
    string.ascii_uppercase + string.ascii_lowercase + string.digits + "_"
)
FIELD_ALLOWABLE_CHARS = set(FIELD_ALLOWABLE_CHARS_STR)

DATASOURCE_ALLOWABLE_CHARS_STR = (
    string.ascii_uppercase + string.ascii_lowercase + string.digits + "_"
)
DATASOURCE_ALLOWABLE_CHARS = set(DATASOURCE_ALLOWABLE_CHARS_STR)


def field_safe_name(name):
    for char in name:
        if char not in FIELD_ALLOWABLE_CHARS:
            name = name.replace(char, "_")
    return name


def default_field_name(column):
    return field_safe_name(column_fullname(column))


def parse_technical_string(val):
    arg1 = None
    arg2 = None

    if "-" in val:
        parts = val.split("-")
        if len(parts) == 2:
            ttype, arg1 = parts
        elif len(parts) == 3:
            ttype, arg1, arg2 = parts
    else:
        ttype = val

    result = dict(type=ttype, params={})

    if ttype == TechnicalTypes.CUMSUM:
        if arg1 or arg2:
            raise InvalidTechnicalException(
                "Invalid %s technical string: %s, no args allowed" % (ttype, val)
            )
    elif ttype in (TechnicalTypes.DIFF, TechnicalTypes.PCT_DIFF):
        if arg2 is not None:
            raise InvalidTechnicalException(
                "Invalid %s technical string: %s, only 1 arg allowed" % (ttype, val)
            )
        if arg1 is not None:
            result["params"] = dict(periods=int(arg1))
    elif ttype in TechnicalTypes:
        window = int(arg1) if arg1 is not None else None
        if window is None:
            raise InvalidTechnicalException(
                "Invalid %s technical string: %s, window arg required" % (ttype, val)
            )
        result["params"] = dict(window=window, min_periods=1)
        if arg2 is not None:
            result["params"]["min_periods"] = int(arg2)
    else:
        raise InvalidTechnicalException("Invalid technical type: %s" % ttype)

    return result


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
            DATASOURCE_CONTEXTS={},
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
        if object_pairs_hook:
            assert False, "Needs to support marshmallow pre_load behavior somehow"
            result = json.loads(raw, object_pairs_hook=object_pairs_hook)
        else:
            result = schema.loads(raw)
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


def load_datasource_config(filename, preserve_order=False):
    file_schema = DataSourceConfigSchema()
    config = parse_schema_file(
        filename, file_schema, object_pairs_hook=OrderedDict if preserve_order else None
    )
    return config


def is_valid_table_type(val):
    if val in TableTypes:
        return True
    raise ValidationError("Invalid table type: %s" % val)


def is_valid_table_name(val):
    if val.count(".") > 1:
        raise ValidationError("Table name has more than one period: %s" % val)
    return True


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
    try:
        tech = create_technical(val)
    except InvalidTechnicalException as e:
        raise ValidationError("Invalid technical: %s" % val) from e
    return True


def is_valid_datasource_config(val):
    if not isinstance(val, dict):
        raise ValidationError("Invalid datasource config: %s" % val)
    schema = DataSourceConfigSchema()
    val = schema.load(val)
    return True


class BaseSchema(Schema):
    class Meta:
        # Use the json module as imported from tlbx
        json_module = json


class TechnicalInfoSchema(BaseSchema):
    type = mfields.String(required=True, validate=is_valid_technical_type)
    params = mfields.Dict(keys=mfields.Str(), default=None, missing=None)


class TechnicalField(mfields.Field):
    def _validate(self, value):
        is_valid_technical(value)
        super()._validate(value)


class AdHocFieldSchema(BaseSchema):
    name = mfields.String(required=True, validate=is_valid_field_name)
    formula = mfields.String(required=True)


class AdHocMetricSchema(AdHocFieldSchema):
    technical = TechnicalField(default=None, missing=None)
    rounding = mfields.Integer(default=None, missing=None)
    required_grain = mfields.List(mfields.Str, default=None, missing=None)


class ColumnFieldConfigSchema(BaseSchema):
    name = mfields.Str(required=True, validate=is_valid_field_name)
    ds_formula = mfields.Str(required=True)


class ColumnFieldConfigField(mfields.Field):
    def _validate(self, value):
        is_valid_column_field_config(value)
        super()._validate(value)


class ColumnInfoSchema(BaseSchema):
    fields = mfields.List(ColumnFieldConfigField())
    allow_type_conversions = mfields.Boolean(default=False, missing=False)
    type_conversion_prefix = mfields.String(default=None, missing=None)
    active = mfields.Boolean(default=True, missing=True)


class ColumnConfigSchema(ColumnInfoSchema):
    pass


class TableTypeField(mfields.Field):
    def _validate(self, value):
        is_valid_table_type(value)
        super()._validate(value)


class TableInfoSchema(BaseSchema):
    type = TableTypeField(required=True)
    active = mfields.Boolean(default=True, missing=True)
    parent = mfields.Str(default=None, missing=None)
    create_fields = mfields.Boolean(default=False, missing=False)
    use_full_column_names = mfields.Boolean(default=True, missing=True)
    primary_key = mfields.List(mfields.Str, required=True)


ADHOC_TABLE_CONFIG_PARAMS = ["url", "adhoc_table_options"]


class TableConfigSchema(TableInfoSchema):
    columns = mfields.Dict(
        keys=mfields.Str(),
        values=mfields.Nested(ColumnConfigSchema),
        missing=None,
        required=False,
    )
    url = mfields.String()
    primary_key = mfields.List(mfields.String())
    adhoc_table_options = mfields.Dict(keys=mfields.Str())


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
    required_grain = mfields.List(mfields.Str, default=None, missing=None)

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
    # TODO: add support for FormulaDimensions
    # formula = mfields.String(default=None, missing=None)


class TableNameField(mfields.Str):
    def _validate(self, value):
        is_valid_table_name(value)
        super()._validate(value)


class DataSourceConfigSchema(BaseSchema):
    url = mfields.String()
    metrics = mfields.List(mfields.Nested(MetricConfigSchema))
    dimensions = mfields.List(mfields.Nested(DimensionConfigSchema))
    tables = mfields.Dict(
        keys=TableNameField(), values=mfields.Nested(TableConfigSchema)
    )

    @pre_load
    def check_table_refs(self, data, **kwargs):
        for table_name, table_config in data.get("tables", {}).items():
            if not isinstance(table_config, str):
                continue

            f, _, _, should_close = get_filepath_or_buffer(table_config)
            close = False or should_close
            if isinstance(f, str):
                f = open(f, "r")
                close = True

            try:
                raw = f.read()
            finally:
                if close:
                    try:
                        f.close()
                    except ValueError:
                        pass

            json_config = json.loads(raw)
            schema = TableConfigSchema()
            config = schema.load(json_config)
            data["tables"][table_name] = config

        return data


class DataSourceConfigField(mfields.Field):
    def _validate(self, value):
        is_valid_datasource_config(value)
        super()._validate(value)


class WarehouseConfigSchema(BaseSchema):
    metrics = mfields.List(mfields.Nested(MetricConfigSchema))
    dimensions = mfields.List(mfields.Nested(DimensionConfigSchema))
    datasources = mfields.Dict(
        keys=mfields.Str(), values=DataSourceConfigField, required=True
    )

    @pre_load
    def check_ds_refs(self, data, **kwargs):
        for ds_name, ds_config in data.get("datasources", {}).items():
            if not isinstance(ds_config, str):
                continue

            f, _, _, should_close = get_filepath_or_buffer(ds_config)
            close = False or should_close
            if isinstance(f, str):
                f = open(f, "r")
                close = True

            try:
                raw = f.read()
            finally:
                if close:
                    try:
                        f.close()
                    except ValueError:
                        pass

            json_config = json.loads(raw)
            schema = DataSourceConfigSchema()
            config = schema.load(json_config)
            data["datasources"][ds_name] = config

        return data


class ZillionInfo(MappingMixin):
    schema = None

    @initializer
    def __init__(self, **kwargs):
        assert self.schema, "ZillionInfo subclass must have a schema defined"
        self.schema().load(self)

    @classmethod
    def schema_validate(cls, zillion_info, unknown=RAISE):
        return cls.schema(unknown=unknown).validate(zillion_info)

    @classmethod
    def schema_load(cls, zillion_info, unknown=RAISE):
        return cls.schema().load(zillion_info, unknown=unknown)

    @classmethod
    def create(cls, zillion_info, unknown=RAISE):
        if isinstance(zillion_info, cls):
            return zillion_info
        assert isinstance(zillion_info, dict), (
            "Raw info must be a dict: %s" % zillion_info
        )
        zillion_info = cls.schema().load(zillion_info, unknown=unknown)
        return cls(**zillion_info)


class TableInfo(ZillionInfo, PrintMixin):
    repr_attrs = ["type", "active", "create_fields", "parent"]
    schema = TableInfoSchema


class ColumnInfo(ZillionInfo, PrintMixin):
    repr_attrs = ["fields", "active"]
    schema = ColumnInfoSchema

    def __init__(self, **kwargs):
        super(ColumnInfo, self).__init__(**kwargs)
        self.field_map = OrderedDict()
        for field in self.fields:
            self.add_field_to_map(field)

    def has_field(self, field):
        if not isinstance(field, str):
            field = field["name"]
        if field in self.field_map:
            return True
        return False

    def add_field_to_map(self, field):
        assert not self.has_field(field), "Field %s is already added" % field
        if isinstance(field, str):
            self.field_map[field] = None
        else:
            # TODO: FieldInfoSchema?
            assert isinstance(field, dict) and "name" in field, (
                "Invalid field config: %s" % field
            )
            self.field_map[field["name"]] = field

    def add_field(self, field):
        self.add_field_to_map(field)
        self.fields.append(field)

    def get_fields(self):
        return {k: v for k, v in self.field_map.items()}

    def get_field_names(self):
        return self.field_map.keys()


# TODO: is there a better home for this?
class Technical(MappingMixin, PrintMixin):
    repr_attrs = ["type", "params"]
    allowed_params = set()

    @initializer
    def __init__(self, type, params):
        self.check_params(params)

    @classmethod
    def check_params(cls, params):
        if not params:
            return
        for k, v in params.items():
            if k not in cls.allowed_params:
                raise InvalidTechnicalException("Invalid param for %s: %s" % (cls, k))

    def apply(self, df, column):
        raise NotImplementedError


class CumulativeTechnical(Technical):
    def apply(self, df, column):
        return df[column].cumsum()


class DiffTechnical(Technical):
    allowed_params = set(["periods"])

    def apply(self, df, column):
        if self.type == TechnicalTypes.DIFF:
            return df[column].diff(**self.params)
        elif self.type == TechnicalTypes.PCT_DIFF:
            return df[column].pct_change(**self.params)
        raise InvalidTechnicalException("Invalid DiffTechnical type: %s" % self.type)


class RollingTechnical(Technical):
    allowed_params = set(["window", "min_periods", "center"])

    def apply(self, df, column):
        rolling = df[column].rolling(**self.params)

        if self.type == TechnicalTypes.MA:
            return rolling.mean()
        elif self.type == TechnicalTypes.SUM:
            return rolling.sum()
        elif self.type == TechnicalTypes.BOLL:
            ma = rolling.mean()
            std = rolling.std()
            lower = ma - 2 * std
            upper = ma + 2 * std
            return lower, upper

        raise InvalidTechnicalException("Invalid RollingTechnical type: %s" % self.type)


def create_technical(info):
    if isinstance(info, Technical):
        return info
    if isinstance(info, str):
        info = parse_technical_string(info)
    assert isinstance(info, dict), "Raw info must be a dict: %s" % info

    info = TechnicalInfoSchema().load(info)

    if info["type"] == TechnicalTypes.CUMSUM:
        cls = CumulativeTechnical
    elif info["type"] in (TechnicalTypes.DIFF, TechnicalTypes.PCT_DIFF):
        cls = DiffTechnical
    elif info["type"] in TechnicalTypes:
        cls = RollingTechnical
    else:
        raise InvalidTechnicalException("Invalid technical type: %s" % info["type"])

    return cls(info["type"], info.get("params", {}))
