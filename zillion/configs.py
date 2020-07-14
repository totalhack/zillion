from collections import OrderedDict, defaultdict
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
import yaml

from zillion.core import *
from zillion.sql_utils import (
    column_fullname,
    type_string_to_sa_type,
    InvalidSQLAlchemyTypeString,
)


FIELD_NAME_ALLOWED_CHARS_STR = (
    string.ascii_uppercase + string.ascii_lowercase + string.digits + "_"
)
FIELD_NAME_ALLOWED_CHARS = set(FIELD_NAME_ALLOWED_CHARS_STR)

FIELD_DISPLAY_NAME_ALLOWED_CHARS_STR = (
    string.ascii_uppercase
    + string.ascii_lowercase
    + string.digits
    + "_-:/|<>+-=@[]{}()$%&*?,. "
)
FIELD_DISPLAY_NAME_ALLOWED_CHARS = set(FIELD_DISPLAY_NAME_ALLOWED_CHARS_STR)

DATASOURCE_NAME_ALLOWED_CHARS_STR = (
    string.ascii_uppercase + string.ascii_lowercase + string.digits + "_"
)
DATASOURCE_NAME_ALLOWED_CHARS = set(DATASOURCE_NAME_ALLOWED_CHARS_STR)
DATASOURCE_CONNECT_FUNC_DEFAULT = "zillion.datasource.url_connect"


def load_zillion_config():
    """If the ZILLION_CONFIG environment variable is defined, read the YAML
    config from this file. Otherwise return a default config.
    
    **Returns:**

    (*dict*) - The zillion config dict.
    
    """
    zillion_config_fname = os.environ.get("ZILLION_CONFIG", None)
    if not zillion_config_fname:
        return dict(
            DEBUG=False,
            ZILLION_DB_URL="sqlite:////tmp/zillion.db",
            ADHOC_DATASOURCE_DIRECTORY="/tmp",
            LOAD_TABLE_CHUNK_SIZE=5000,
            DATASOURCE_QUERY_MODE=DataSourceQueryModes.SEQUENTIAL,
            DATASOURCE_QUERY_TIMEOUT=None,
            DATASOURCE_CONTEXTS={},
        )
    return yaml.safe_load(open(zillion_config_fname))


zillion_config = load_zillion_config()


def parse_schema_file(f, schema, object_pairs_hook=None):
    """Parse a marshmallow schema file
    
    **Parameters:**
    
    * **f** - (*str or buffer*) A file path or buffer to read the raw schema
    contents from
    * **schema** - (*marshmallow schema*) The marshmallow schema to use to parse
    the data
    * **object_pairs_hook** - (*optional*) Passed through to json.loads. This
    has some issues and currently produces an error if specified.
    
    **Returns:**

    (*dict*) - A JSON structure loaded from the schema file
    
    """
    raw = read_filepath_or_buffer(f)
    try:
        # This does the schema check, but has a bug in object_pairs_hook so
        # order is not preserved
        if object_pairs_hook:
            raise AssertionError(
                "Needs to support marshmallow pre_load behavior somehow"
            )
            result = json.loads(raw, object_pairs_hook=object_pairs_hook)
        else:
            result = schema.loads(raw)
    except ValidationError as e:
        error("Schema Validation Error: %s" % schema)
        print(json.dumps(str(e), indent=2))
        raise
    return result


def load_warehouse_config(cfg, preserve_order=False):
    """Parse a warehouse JSON config
    
    **Parameters:**
    
    * **cfg** - (*dict, str, or buffer*) A warehouse config dict or a file
    path/buffer to read the config contents from.
    * **preserve_order** - (*bool, optional*) If true and a str or buffer is
    passed for the cfg arg, use OrderedDict as the object_pairs_hook to preserve
    order.
    
    **Returns:**
    
    (*dict*) - The parsed warehouse config
    
    """
    if isinstance(cfg, dict):
        return WarehouseConfigSchema().load(cfg)

    return parse_schema_file(
        cfg,
        WarehouseConfigSchema(),
        object_pairs_hook=OrderedDict if preserve_order else None,
    )


def load_warehouse_config_from_env(var, preserve_order=False):
    """Parse a warehouse JSON config from a location stored in an environment
    variable"""
    f = os.environ.get(var)
    return load_warehouse_config(f, preserve_order=preserve_order)


def load_datasource_config(cfg, preserve_order=False):
    """Parse a datasource JSON config
    
    **Parameters:**
    
    * **cfg** - (*dict, str, or buffer*) A datasource config dict or a file
    path/buffer to read the config contents from.
    * **preserve_order** - (*bool, optional*) If true and a str or buffer is
    passed for the cfg arg, use OrderedDict as the object_pairs_hook to preserve
    order.
    
    **Returns:**
    
    (*dict*) - The parsed datasource config
    
    """
    if isinstance(cfg, dict):
        return DataSourceConfigSchema().load(cfg)

    return parse_schema_file(
        cfg,
        DataSourceConfigSchema(),
        object_pairs_hook=OrderedDict if preserve_order else None,
    )


def load_datasource_config_from_env(var, preserve_order=False):
    """Parse a datasource JSON config from a location stored in an environment
    variable"""
    f = os.environ.get(var)
    return load_datasource_config(f, preserve_order=preserve_order)


def field_safe_name(name):
    """Replace characters with underscores if they are not in
    FIELD_NAME_ALLOWED_CHARS
    
    **Parameters:**
    
    * **name** - (*str*) The field name to process
    
    **Returns:**
    
    (*str*) - The "safe" field name
    
    """
    for char in name:
        if char not in FIELD_NAME_ALLOWED_CHARS:
            name = name.replace(char, "_")
    return name


def default_field_name(column):
    """Get the default field name from a SQLAlchemy column
    
    **Parameters:**
    
    * **column** - (*SQLAlchemy column*) A column to get the default field name
    for
    
    **Returns:**
    
    (*str*) - The default field name for the column
    
    """
    return field_safe_name(column_fullname(column))


def default_field_display_name(name):
    """Determine a default display name from the field name

    **Parameters:**

    * **name** - (*str*) The field name to process

    **Returns:**

    (*str*) - The field display name

    """
    name = field_safe_name(name)
    display_name = " ".join(name.replace("_", " ").split()).title()
    return display_name


def is_active(obj):
    """Helper to test if an object is an active part of the zillion config"""
    if not getattr(obj, "zillion", None):
        return False
    if not obj.zillion.active:
        return False
    return True


def is_valid_table_type(val):
    """Validate table type"""
    if val in TableTypes:
        return True
    raise ValidationError("Invalid table type: %s" % val)


def is_valid_table_name(val):
    """Validate table name"""
    if val.count(".") > 1:
        raise ValidationError("Table name has more than one period: %s" % val)
    return True


def is_valid_if_exists(val):
    """Validate if_exists param"""
    if val in IfExistsModes:
        return True
    raise ValidationError("Invalid if_exists value: %s" % val)


def is_valid_field_name(val):
    """Validate field name"""
    if val is None:
        raise ValidationError("Field name can not be null")
    if set(val) <= FIELD_NAME_ALLOWED_CHARS:
        return True
    raise ValidationError(
        'Field name "%s" has invalid characters. Allowed: %s'
        % (val, FIELD_NAME_ALLOWED_CHARS_STR)
    )


def is_valid_field_display_name(val):
    """Validate field display name"""
    if val is None:
        raise ValidationError("Field display name can not be null")
    if set(val) <= FIELD_DISPLAY_NAME_ALLOWED_CHARS:
        return True
    raise ValidationError(
        'Field display name "%s" has invalid characters. Allowed: %s'
        % (val, FIELD_DISPLAY_NAME_ALLOWED_CHARS_STR)
    )


def is_valid_sqlalchemy_type(val):
    """Validate SQLAlchemy type string"""
    if val is not None:
        try:
            type_string_to_sa_type(val)
        except InvalidSQLAlchemyTypeString as e:
            raise ValidationError("Invalid sqlalchemy type: %s" % val)
    return True


def is_valid_aggregation(val):
    """Validate aggregation type"""
    if val in AggregationTypes:
        return True
    raise ValidationError("Invalid aggregation: %s" % val)


def is_valid_column_field_config(val):
    """Validate column field config"""
    if isinstance(val, str):
        return True
    if isinstance(val, dict):
        schema = ColumnFieldConfigSchema()
        schema.load(val)
        return True
    raise ValidationError("Invalid column field config: %s" % val)


def is_valid_technical_type(val):
    """Validate technical type"""
    if val in TechnicalTypes:
        return True
    raise ValidationError("Invalid technical type: %s" % val)


def is_valid_technical_mode(val):
    """Validate technical mode"""
    if val in TechnicalModes:
        return True
    raise ValidationError("Invalid technical mode: %s" % val)


def is_valid_technical(val):
    """Validate technical"""
    try:
        create_technical(val)
    except InvalidTechnicalException as e:
        raise ValidationError("Invalid technical: %s" % val) from e
    return True


def is_valid_connect_type(val):
    """Validate technical type"""
    if isinstance(val, str):
        try:
            import_object(val)
        except Exception as e:
            raise ValidationError("Could not import connect type: %s" % val) from e
        return True
    raise ValidationError("Invalid connect type: %s" % val)


def is_valid_datasource_connect(val):
    """Validate datasource connect value"""
    if isinstance(val, str):
        return True
    if isinstance(val, dict):
        schema = DataSourceConnectSchema()
        schema.load(val)
        return True
    raise ValidationError("Invalid datasource connect config: %s" % val)


def is_valid_datasource_config(val):
    """Validate datasource config"""
    if not isinstance(val, dict):
        raise ValidationError("Invalid datasource config: %s" % val)
    schema = DataSourceConfigSchema()
    val = schema.load(val)
    return True


# Inspiration: https://gist.github.com/ramnes/89245fbd9f2dfff52a78
class PolyNested(mfields.Nested):
    """A polytype nested field that iterates through a list of possible types"""

    def _deserialize(self, value, attr, data, partial=None, **kwargs):
        raiseifnot(isinstance(self.nested, list), "Expected list of schemas")
        errors = []
        for schema in self.nested:
            if isinstance(schema, type):
                schema = schema()
            try:
                result = schema.load(value)
            except ValidationError as e:
                errors.append(e)
                continue
            return result
        raise ValidationError(
            "Could not deserialize value with PolyNested schemas. Data:%s\nSchemas:%s"
            % (value, self.nested)
        )


class BaseSchema(Schema):
    """Base Schema with custom JSON module"""

    class Meta:
        """Use the json module as imported from tlbx"""

        json_module = json


class TechnicalInfoSchema(BaseSchema):
    """The schema of a technical configuration"""

    type = mfields.String(required=True, validate=is_valid_technical_type)
    params = mfields.Dict(keys=mfields.Str(), default=None, missing=None)
    mode = mfields.String(validate=is_valid_technical_mode, default=None, missing=None)


class TechnicalField(mfields.Field):
    """A field for defining technical calculations"""

    def _validate(self, value):
        is_valid_technical(value)
        super()._validate(value)


class ColumnFieldConfigSchema(BaseSchema):
    """The schema of a column's field attribute
    
    **Attributes:**
    
    * **name** - (*str*) The name of the field
    * **ds_formula** - (*str*) A formula used to calculate the field value at
    the datasource query level. It must use syntax specific to the datasource.
    
    """

    name = mfields.Str(required=True, validate=is_valid_field_name)
    ds_formula = mfields.Str(required=True)


class ColumnFieldConfigField(mfields.Field):
    """A marshmallow field for the column's field attribute"""

    def _validate(self, value):
        is_valid_column_field_config(value)
        super()._validate(value)


class ColumnInfoSchema(BaseSchema):
    """The schema of column info that ends up in the zillion column metadata
    
    **Attributes:**
    
    * **fields** - (*list of ColumnFieldConfigField, optional*) A list of field
    names or definitions
    * **allow_type_conversions** - (*bool, optional*) A flag denoting whether
    additional fields may be inferred from this column based on its column type
    (such as deriving year from a date).
    * **type_conversion_prefix** - (*str, optional*) A prefix to apply to all
    fields defined through automated type conversions.
    * **active** - (*bool, optional*) A flag denoting whether this column is
    active.
    * **required_grain** - (*list of str, optional*) If specified, a list of
    dimensions that must be present in the dimension grain of any report that
    aims to include this column.
    
    """

    fields = mfields.List(ColumnFieldConfigField())
    allow_type_conversions = mfields.Boolean(default=False, missing=False)
    type_conversion_prefix = mfields.String(default=None, missing=None)
    active = mfields.Boolean(default=True, missing=True)
    required_grain = mfields.List(mfields.Str, default=None, missing=None)


class ColumnConfigSchema(ColumnInfoSchema):
    """The schema of a column configuration"""

    pass


class TableTypeField(mfields.Field):
    """A field for the type of a table"""

    def _validate(self, value):
        is_valid_table_type(value)
        super()._validate(value)


class TableInfoSchema(BaseSchema):
    """The schema of table info that ends up in the zillion table metadata
    
    **Attributes:**
    
    * **type** - (*str*) Specifies the TableType
    * **active** - (*bool, optional*) A flag denoting whether this table is
    active or not.
    * **parent** - (*str, optional*) A reference to the full name of a parent
    table. This impacts the possible join relationships of this table. It is
    assumed to be safe to join back to any parent or ancestor table via shared
    keys (the child table must have the primary key of the parent table).
    * **create_fields** - (*bool, optional*) If true, try to create Field
    objects from all columns in the table. Specifying the fields in a column
    config will override this behavior. Metric vs Dimension fields are inferred
    from the type. It is generally better to be explicit about your fields and
    field types, but this option provides convenience for special cases,
    particularly adhoc use cases.
    * **use_full_column_names** - (*bool, optional*) If True and create_fields
    is True, fully qualify the created field names using the full table and
    column names. If false, assume it is safe to simply use the column name as
    the field name.
    * **primary_key** - (*list of str*) A list of fields representing the
    primary key of the table
    * **incomplete_dimensions** - (*list of str, optional*) If specified, a list
    of dimensions that are not safe to use for joins.
    
    """

    type = TableTypeField(required=True)
    active = mfields.Boolean(default=True, missing=True)
    parent = mfields.Str(default=None, missing=None)
    create_fields = mfields.Boolean(default=False, missing=False)
    use_full_column_names = mfields.Boolean(default=True, missing=True)
    primary_key = mfields.List(mfields.Str, required=True)
    incomplete_dimensions = mfields.List(mfields.Str, default=None, missing=None)


class TableConfigSchema(TableInfoSchema):
    """The schema of a table configuration
    
    **Attributes:**
    
    * **columns** - (*dict, optional*) A dict mapping of column name to
    ColumnConfigSchema
    * **data_url** - (*str, optional*) A url used to download table data if this
    is an adhoc table
    * **if_exists** - (*str, optional*) Control whether to replace, fail, or
    ignore when the table data already exists.
    * **primary_key** - (*list of str, optional*) A list of fields representing
    the primary key of the table
    * **adhoc_table_options** - (*dict, optional*) A dict of additional params
    to pass to the adhoc table class as kwargs
    
    """

    columns = mfields.Dict(
        keys=mfields.Str(),
        values=mfields.Nested(ColumnConfigSchema),
        missing=None,
        required=False,
    )
    data_url = mfields.String()
    if_exists = mfields.String(validate=is_valid_if_exists)
    primary_key = mfields.List(mfields.String())
    adhoc_table_options = mfields.Dict(keys=mfields.Str())


class FieldConfigSchema(BaseSchema):
    """The based schema of a field configuration
    
    **Attributes:**
    
    * **name** - (*str*) The name of the field
    * **type** - (*str*) A string representing the data type of the field. This
    will be converted to a SQLAlchemy type via `ast.literal_eval`.
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field

    """

    name = mfields.String(required=True, validate=is_valid_field_name)
    type = mfields.String(default=None, missing=None, validate=is_valid_sqlalchemy_type)
    display_name = mfields.String(
        default=None, missing=None, validate=is_valid_field_display_name
    )
    description = mfields.String(default=None, missing=None)

    @pre_load
    def _set_default_display_name(self, data, **kwargs):
        """Set a default display name based on the field name"""
        if not data.get("display_name", None):
            data["display_name"] = default_field_display_name(data["name"])
        return data


class FormulaFieldConfigSchema(BaseSchema):
    """The based schema of a formula field configuration
    
    **Attributes:**
    
    * **name** - (*str*) The name of the field
    * **formula** - (*str, optional*) A formula used to compute the field value.
    Formula fields are applied at the combined query layer, rather than in
    datasources queries, so the syntax must match that of the combined query
    layer database.
    * **display_name** - (*str, optional*) The display name of the field
    * **description** - (*str, optional*) The description of the field
    
    """

    name = mfields.String(required=True, validate=is_valid_field_name)
    formula = mfields.String(required=True)
    display_name = mfields.String(
        default=None, missing=None, validate=is_valid_field_display_name
    )
    description = mfields.String(default=None, missing=None)

    @pre_load
    def _set_default_display_name(self, data, **kwargs):
        """Set a default display name based on the field name"""
        if not data.get("display_name", None):
            data["display_name"] = default_field_display_name(data["name"])
        return data


class MetricConfigSchemaMixin:
    """Common attributes and logic for metric configs
    
    **Attributes:**
    
    * **aggregation** - (*str, optional*) A string representing the aggregation
    type to apply to this metric. See `zillion.core.AggregationTypes`.
    * **rounding** - (*int, optional*) If specified, the number of decimal
    places to round to
    * **weighting_metric** - (*str, optional*) A reference to a metric to use
    for weighting when aggregating averages
    * **technical** - (*str or dict, optional*) A string or dict that will be
    parsed as a TechnicalField to define a technical computation to be applied
    to the metric.
    * **required_grain** - (*list of str, optional*) If specified, a list of
    dimensions that must be present in the dimension grain of any report that
    aims to include this metric.
    
    """

    aggregation = mfields.String(
        default=AggregationTypes.SUM,
        missing=AggregationTypes.SUM,
        validate=is_valid_aggregation,
    )
    rounding = mfields.Integer(default=None, missing=None)
    weighting_metric = mfields.Str(default=None, missing=None)
    technical = TechnicalField(default=None, missing=None)
    required_grain = mfields.List(mfields.Str, default=None, missing=None)

    def _validate_weighting_aggregation(self, data):
        if (
            data["weighting_metric"]
            and not data["aggregation"] == AggregationTypes.MEAN
        ):
            raise ValidationError(
                'only "%s" aggregation type is allowed with weighting metrics: %s'
                % (AggregationTypes.MEAN, data)
            )


class MetricConfigSchema(FieldConfigSchema, MetricConfigSchemaMixin):
    """The schema of a metric configuration"""

    pass


class FormulaMetricConfigSchema(FormulaFieldConfigSchema, MetricConfigSchemaMixin):
    """The schema of a formula metric configuration"""

    pass


class DimensionConfigSchema(FieldConfigSchema):
    """The schema of a dimension configuration"""

    pass


class AdHocFieldSchema(FormulaFieldConfigSchema):
    """Base schema for an adhoc field"""

    pass


class AdHocMetricSchema(AdHocFieldSchema):
    """The schema of an adhoc metric
    
    **Attributes:**
    
    * **technical** - (*str or dict, optional*) A string or dict that will be
    parsed as a TechnicalField to define a technical computation to be applied
    to the metric.
    * **rounding** - (*int, optional*) If specified, the number of decimal
    places to round to
    * **required_grain** - (*list of str, optional*) If specified, a list of
    dimensions that must be present in the dimension grain of any report that
    aims to include this metric.
    
    """

    technical = TechnicalField(default=None, missing=None)
    rounding = mfields.Integer(default=None, missing=None)
    required_grain = mfields.List(mfields.Str, default=None, missing=None)


class TableNameField(mfields.Str):
    """The schema of a table configuration represented as a marshmallow Field"""

    def _validate(self, value):
        is_valid_table_name(value)
        super()._validate(value)


class DataSourceConnectSchema(BaseSchema):
    """The schema of a technical configuration"""

    func = mfields.String(
        validate=is_valid_connect_type,
        default=DATASOURCE_CONNECT_FUNC_DEFAULT,
        missing=DATASOURCE_CONNECT_FUNC_DEFAULT,
    )
    params = mfields.Dict(keys=mfields.Str(), default=None, missing=None)


class DataSourceConnectField(mfields.Field):
    """The schema of a datasource connect field"""

    def _validate(self, value):
        is_valid_datasource_connect(value)
        super()._validate(value)


class DataSourceConfigSchema(BaseSchema):
    """The schema of a datasource configuration
    
    **Attributes:**
    
    * **connect** - (*str or dict*) A connection string or dict for establishing
    the datasource connection. This may have placeholders that get filled in
    from the DATASOURCE_CONTEXTS of the zillion config. See
    DataSourceConnectField for more details on passing a dict.
    * **skip_conversion_fields** - (*bool, optional*) Don't add any conversion
    fields when applying a config
    * **metrics** - (*marshmallow field, optional*) A list of MetricConfigSchema
    * **dimensions** - (*marshmallow field, optional*) A list of
    DimensionConfigSchema
    * **tables** - (*marshmallow field, optional*) A dict mapping of
    TableNameField -> TableConfigSchema
    
    """

    connect = DataSourceConnectField(default=None, missing=None)
    skip_conversion_fields = mfields.Boolean(default=False, missing=False)
    metrics = mfields.List(PolyNested([MetricConfigSchema, FormulaMetricConfigSchema]))
    dimensions = mfields.List(mfields.Nested(DimensionConfigSchema))
    tables = mfields.Dict(
        keys=TableNameField(), values=mfields.Nested(TableConfigSchema)
    )

    @pre_load
    def _check_table_refs(self, data, **kwargs):
        """Load remote table configs before processing"""
        for table_name, table_config in data.get("tables", {}).items():
            if not isinstance(table_config, str):
                continue

            raw = read_filepath_or_buffer(table_config)
            json_config = json.loads(raw)
            schema = TableConfigSchema()
            config = schema.load(json_config)
            data["tables"][table_name] = config

        return data


class DataSourceConfigField(mfields.Field):
    """The schema of a datasource configuration represented as a marshmallow
    Field"""

    def _validate(self, value):
        is_valid_datasource_config(value)
        super()._validate(value)


class WarehouseConfigSchema(BaseSchema):
    """The schema of a warehouse configuration.
    
    **Attributes:**
    
    * **metrics** - (*marshmallow field, optional*) A list of MetricConfigSchema
    * **dimensions** - (*marshmallow field, optional*) A list of
    DimensionConfigSchema
    * **datasources** - (*marshmallow field*) A dict mapping of datasource name
    -> DataSourceConfigField
    
    """

    metrics = mfields.List(PolyNested([MetricConfigSchema, FormulaMetricConfigSchema]))
    dimensions = mfields.List(mfields.Nested(DimensionConfigSchema))
    datasources = mfields.Dict(
        keys=mfields.Str(), values=DataSourceConfigField, required=True
    )

    @pre_load
    def _check_ds_refs(self, data, **kwargs):
        """Load remote datasource configs before processing"""
        for ds_name, ds_config in data.get("datasources", {}).items():
            if not isinstance(ds_config, str):
                continue

            raw = read_filepath_or_buffer(ds_config)
            json_config = json.loads(raw)
            schema = DataSourceConfigSchema()
            config = schema.load(json_config)
            data["datasources"][ds_name] = config

        return data


class ConfigMixin:
    """Mixin to allow validation against a marshmallow schema"""

    schema = None

    def __init__(self, *args, **kwargs):
        """Validate the object against a marshmallow schema"""
        raiseifnot(self.schema, "ConfigMixin requires a schema attribute")
        self.schema().load(self.__dict__, unknown=EXCLUDE)

    def to_config(self):
        """Get the config for this object"""
        return self.schema().load(self.__dict__, unknown=EXCLUDE)

    @classmethod
    def from_config(cls, config):
        """Create a the object from a config"""
        return cls(**config)


class ZillionInfo(MappingMixin):
    """Information that defines a part of the zillion configuration. The
    information may come from a JSON config file or directly from the SQLALchemy
    object's `info.zillion` attribute. The JSON schema is parsed with a
    marshmallow schema object. See the particular schema used with each subclass
    for details on fields.
    
    **Parameters:**
    
    * **kwargs** - Parameters that will be parsed with the given marshmallow
    schema.
    
    **Attributes:**
    
    * **schema** - (*marshmallow schema*) A class attribute that specifies the
    marshmallow schema used to parse the input args on init.
    
    """

    schema = None

    @initializer
    def __init__(self, **kwargs):
        raiseifnot(self.schema, "ZillionInfo subclass must have a schema defined")
        self.schema().load(self)

    @classmethod
    def schema_validate(cls, zillion_info, unknown=RAISE):
        """Validate an info dict against a schema.
        
        **Parameters:**
        
        * **zillion_info** - (*dict*) A dict to validate against the schema
        * **unknown** - (*optional*) A flag passed through to marshmallow's
        schema processing
        
        """
        return cls.schema(unknown=unknown).validate(zillion_info)

    @classmethod
    def schema_load(cls, zillion_info, unknown=RAISE):
        """Load an info dict with a marshmallow schema
        
        **Parameters:**
        
        * **zillion_info** - (*dict*) A dict to load with the schema
        * **unknown** - (*optional*) A flag passed through to marshmallow's
        schema processing
        
        **Returns:**
        
        (*dict*) - The loaded schema result
        
        """
        return cls.schema().load(zillion_info, unknown=unknown)

    @classmethod
    def create(cls, zillion_info, unknown=RAISE):
        """Factory to create a ZillionInfo object from the class schema"""
        if isinstance(zillion_info, cls):
            return zillion_info
        raiseifnot(
            isinstance(zillion_info, dict), "Raw info must be a dict: %s" % zillion_info
        )
        zillion_info = cls.schema().load(zillion_info, unknown=unknown)
        return cls(**zillion_info)


class TableInfo(ZillionInfo, PrintMixin):
    """ZillionInfo for a table. See TableInfoSchema for more details about
    fields."""

    repr_attrs = ["type", "active", "create_fields", "parent"]
    schema = TableInfoSchema


class ColumnInfo(ZillionInfo, PrintMixin):
    """ZillionInfo for a column in a table. See ColumnInfoSchema for more
    details about fields."""

    repr_attrs = ["fields", "active"]
    schema = ColumnInfoSchema

    def __init__(self, **kwargs):
        super(ColumnInfo, self).__init__(**kwargs)
        self._field_map = OrderedDict()
        for field in self.fields:
            self._add_field_to_map(field)

    def has_field(self, field):
        """Determine if the column supports the given field"""
        if not isinstance(field, str):
            field = field["name"]
        if field in self._field_map:
            return True
        return False

    def add_field(self, field):
        """Add the field to the column's fields"""
        self._add_field_to_map(field)
        self.fields.append(field)

    def get_field(self, name):
        """Get the reference to the field defined on this column. This may
        return a string or a dict depending on how the field was defined on the
        column.
        
        **Parameters:**
        
        * **name** - (*str*) The name of the field
        
        **Returns:**
        
        (*str or dict*) - The name of the field or the dict defining the field
        
        """
        raiseifnot(self.has_field(name), "Field %s is not in column fields" % name)
        return self._field_map[name] or name

    def get_fields(self):
        """Get all fields mapped on this column"""
        return {k: v for k, v in self._field_map.items()}

    def get_field_names(self):
        """Get the names of all fields mapped on this column"""
        return self._field_map.keys()

    def field_ds_formula(self, name):
        """Get the datasource-level formula for a field if it exists"""
        field = self.get_field(name)
        if isinstance(field, str):
            return None
        return field.get("ds_formula", None)

    def _add_field_to_map(self, field):
        """Add to the map of fields on this column"""
        raiseif(self.has_field(field), "Field %s is already added" % field)
        if isinstance(field, str):
            self._field_map[field] = None
        else:
            raiseifnot(
                isinstance(field, dict) and "name" in field,
                "Invalid field config: %s" % field,
            )
            self._field_map[field["name"]] = field


# TODO: might be a better home for some of the Technical stuff


class Technical(MappingMixin, PrintMixin):
    """A technical computation on a DataFrame column
    
    **Parameters:**
    
    * **type** - (*str*) The TechnicalType
    * **params** - (*dict*) Params for the technical computation
    * **mode** - (*str*) The mode that controls how to apply the technical
    computation across the data's dimensions. See TechnicalModes for options. If
    None, the default mode will be set based on the technical type.
    
    **Attributes:**
    
    * **allowed_params** - (*set*) Define the allowed technical parameters
    
    """

    repr_attrs = ["type", "params", "mode"]
    allowed_params = set()

    @initializer
    def __init__(self, type, params, mode=None):
        if mode is None:
            self.mode = self.get_default_mode()
        raiseifnot(
            self.mode in TechnicalModes, "Invalid Technical mode: %s" % self.mode
        )
        self._check_params(params)

    @classmethod
    def _check_params(cls, params):
        """Validate the technical params"""
        if not params:
            return
        for k, v in params.items():
            if k not in cls.allowed_params:
                raise InvalidTechnicalException("Invalid param for %s: %s" % (cls, k))

    @classmethod
    def parse_technical_string_params(cls, val):
        """Return named params from a technical string"""
        ttype, params, mode = _extract_technical_string_parts(val)
        if params:
            raise InvalidTechnicalException(
                "Invalid %s technical string: %s, no args allowed" % (ttype, val)
            )
        return {}

    @classmethod
    def get_default_mode(cls):
        """Get the default mode for applying the technical calculation"""
        return TechnicalModes.GROUP

    def _apply(self, df, column, indexer, rounding=None):
        """Apply the technical computation along a target slice of a
        dataframe"""
        raise NotImplementedError

    def apply(self, df, column, rounding=None):
        """Apply a technical computation to a dataframe. If the dataframe has a
        multilevel index and the technical is being applied in group mode, then
        the data will be sliced along the second to last level and the technical
        applied to each subgroup. Otherwise the technical is applied across the
        entire dataframe. The technical is applied to the dataframe in place.
        
        **Parameters:**
        
        * **df** - (*DataFrame*) A DataFrame to apply a technical computation to
        * **column** - (*str*) The name of the target column for the technical
        computation
        * **rounding** - (*dict, optional*) The rounding settings for the
        report's columns
        
        """
        if df.empty:
            return

        if self.mode == TechnicalModes.GROUP and hasattr(df.index, "levels"):
            raiseif(df.index.empty, "Need support for empty index")
            index_len = len(df.index.levels)
            level = max(index_len - 2, 0)
            index_vals = df.index.levels[level]

            for val in index_vals:
                slice_parts = []
                for i in range(index_len):
                    if i != level:
                        slice_parts.append(slice(None))
                    else:
                        slice_parts.append(val)

                indexer = tuple(slice_parts)
                self._apply(df, column, indexer, rounding=rounding)
        else:
            indexer = slice(None)
            self._apply(df, column, indexer, rounding=rounding)


class PandasTechnical(Technical):
    """A generic Technical runs a pandas method"""

    def _apply(self, df, column, indexer, **kwargs):
        """This assumes the Technical type string matches the pandas method
        name"""
        method = getattr(df.loc[indexer, column], self.type.lower())
        df.loc[indexer, column] = method(**self.params)


class RankTechnical(PandasTechnical):
    """A Technical specific to the pandas rank function"""

    def _apply(self, df, column, indexer, **kwargs):
        params = {}
        if self.type == TechnicalTypes.PCT_RANK:
            params = {"pct": True}
        df.loc[indexer, column] = df.loc[indexer, column].rank(**params)


class DiffTechnical(PandasTechnical):
    """A Technical that computes a periodic diff on a DataFrame"""

    allowed_params = set(["periods"])

    @classmethod
    def parse_technical_string_params(cls, val):
        """Return named params from a technical string"""
        ttype, params, mode = _extract_technical_string_parts(val)
        if len(params) > 1:
            raise InvalidTechnicalException(
                "Invalid %s technical string: %s, only 1 arg allowed" % (ttype, val)
            )
        if params and params[0] is not None:
            return dict(periods=int(params[0]))
        return {}


class RollingTechnical(Technical):
    """A Technical that uses the pandas rolling feature"""

    allowed_params = set(["window", "min_periods", "center"])

    def _apply(self, df, column, indexer, rounding=None):
        """Apply a rolling function to a column of a DataFrame"""
        rolling = df.loc[indexer, column].rolling(**self.params)
        method = getattr(rolling, self.type.lower())
        df.loc[indexer, column] = method()

    @classmethod
    def parse_technical_string_params(cls, val):
        """Return named params from a technical string"""
        ttype, params, mode = _extract_technical_string_parts(val)
        if len(params) not in (1, 2):
            raise InvalidTechnicalException(
                "Invalid %s technical string: %s, 1 or 2 args allowed" % (ttype, val)
            )
        result = dict(window=int(params[0]), min_periods=1)
        if len(params) == 2:
            result["min_periods"] = int(params[1])
        return result


class BollingerTechnical(RollingTechnical):
    """Compute a rolling average and bollinger bands for a column. This adds
    additional columns to the input dataframe."""

    def _apply(self, df, column, indexer, rounding=None):
        rolling = df.loc[indexer, column].rolling(**self.params)
        ma = rolling.mean()
        std = rolling.std()
        lower = ma - 2 * std
        upper = ma + 2 * std
        col_lower = column + "_lower"
        col_upper = column + "_upper"

        df.loc[indexer, column] = ma
        if rounding and column in rounding:
            # This adds some extra columns for the bounds, so we use
            # the same rounding as the root column if applicable.
            df.loc[indexer, col_lower] = round(lower, rounding[column])
            df.loc[indexer, col_upper] = round(upper, rounding[column])
        else:
            df.loc[indexer, col_lower] = lower
            df.loc[indexer, col_upper] = upper


ROLLING_TECHNICALS = set(
    [
        TechnicalTypes.MEAN,
        TechnicalTypes.SUM,
        TechnicalTypes.MEDIAN,
        TechnicalTypes.MIN,
        TechnicalTypes.MAX,
        TechnicalTypes.STD,
        TechnicalTypes.VAR,
        TechnicalTypes.BOLL,
    ]
)

DIFF_TECHNICALS = set([TechnicalTypes.DIFF, TechnicalTypes.PCT_CHANGE])

RANK_TECHNICALS = set([TechnicalTypes.RANK, TechnicalTypes.PCT_RANK])

TECHNICAL_CLASS_MAP = defaultdict(lambda: PandasTechnical)
TECHNICAL_CLASS_MAP.update({k: RollingTechnical for k in ROLLING_TECHNICALS})
TECHNICAL_CLASS_MAP.update({k: DiffTechnical for k in DIFF_TECHNICALS})
TECHNICAL_CLASS_MAP.update({k: RankTechnical for k in RANK_TECHNICALS})

TECHNICAL_CLASS_MAP[TechnicalTypes.BOLL] = BollingerTechnical


def _extract_technical_string_parts(val):
    """Extract params for a technical from shorthand string
    
    General format: TYPE(PARAM1, ...):MODE  Params and mode are optional.
    
    """
    params = []
    mode = None

    if ":" in val:
        tech_params, mode = val.split(":")
    else:
        tech_params = val

    if "(" in tech_params:
        parts = tech_params.rstrip(")").split("(")
        ttype = parts[0]
        params = [x.strip() for x in parts[1].split(",") if x]
    else:
        ttype = tech_params

    raiseifnot(ttype, "No technical type could be parsed from string: %s" % val)
    return ttype, params, mode


def parse_technical_string(val):
    """Parse Technical args from a shorthand string
    
    **Parameters:**
    
    * **val** - (*str*) The technical string to parse. The general format is:
    `type(*args):mode`. The type must be a valid value in TechnicalTypes. The
    argument requirements vary by type, and are optional in some cases. The mode
    controls whether the computation is done across the last group or the full
    data. The mode is optional, and will default to a value specific to that
    technical type (usually "group" mode). Examples:

        * "mean(5)" for moving average, window=5
        * "mean(5,2)" for moving average, window=5, min_period=2
        * "cumsum" for cumulative sum (no args)
        * "cumsum:all" for cumulative sum across all data, regardless of dimension
    
    **Returns:**
    
    (*dict*) - A dict of Technical args
    
    """
    ttype, params, mode = _extract_technical_string_parts(val)
    if not ttype in TechnicalTypes:
        raise InvalidTechnicalException("Invalid technical type: %s" % ttype)
    result = dict(type=ttype, params={}, mode=mode)
    cls = TECHNICAL_CLASS_MAP[ttype]
    result["params"] = cls.parse_technical_string_params(val)
    return result


def create_technical(info):
    """Create a technical instance from the input object
    
    **Parameters:**
    
    * **info** - (*str or dict*) If str, parse as atechnical string. If a dict,
    parse as TechnicalInfoSchema.
    
    **Returns:**
    
    (*Technical*) - A Technical object based on the input.
    
    """
    if isinstance(info, Technical):
        return info
    if isinstance(info, str):
        info = parse_technical_string(info)
    raiseifnot(isinstance(info, dict), "Raw info must be a dict: %s" % info)

    info = TechnicalInfoSchema().load(info)
    if info["type"] not in TechnicalTypes:
        raise InvalidTechnicalException("Invalid technical type: %s" % info["type"])

    cls = TECHNICAL_CLASS_MAP[info["type"]]
    return cls(info["type"], info.get("params", {}), info.get("mode", None))
