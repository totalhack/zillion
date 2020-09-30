[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.configs


## [AdHocFieldSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L750-L753)

*Bases*: zillion.configs.FormulaFieldConfigSchema

::: zillion.configs.AdHocFieldSchema
    :docstring:
    


## [AdHocMetricSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L756-L774)

*Bases*: zillion.configs.AdHocFieldSchema

::: zillion.configs.AdHocMetricSchema
    :docstring:
    


## [BaseSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L442-L457)

*Bases*: marshmallow.schema.Schema

::: zillion.configs.BaseSchema
    :docstring:
    


## [BollingerTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1232-L1253)

*Bases*: zillion.configs.RollingTechnical

::: zillion.configs.BollingerTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [ColumnConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L548-L551)

*Bases*: zillion.configs.ColumnInfoSchema

::: zillion.configs.ColumnConfigSchema
    :docstring:
    


## [ColumnFieldConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L513-L518)

*Bases*: marshmallow.fields.Field

::: zillion.configs.ColumnFieldConfigField
    :docstring:
    


## [ColumnFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L495-L510)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnFieldConfigSchema
    :docstring:
    


## [ColumnInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L987-L1065)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.ColumnInfo
    :docstring:
    :members: add_field create field_ds_formula get_criteria_conversion get_field get_field_names get_fields has_field schema_load schema_validate


## [ColumnInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L521-L545)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnInfoSchema
    :docstring:
    


## [ConfigMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L891-L908)

::: zillion.configs.ConfigMixin
    :docstring:
    :members: from_config to_config


## [DataSourceConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L847-L853)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceConfigField
    :docstring:
    


## [DataSourceConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L804-L844)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConfigSchema
    :docstring:
    


## [DataSourceConnectField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L796-L801)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceConnectField
    :docstring:
    


## [DataSourceConnectSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L785-L793)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConnectSchema
    :docstring:
    


## [DataSourceCriteriaConversionsField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L476-L484)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceCriteriaConversionsField
    :docstring:
    


## [DiffTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1189-L1204)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.DiffTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [DimensionConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L727-L747)

*Bases*: zillion.configs.FieldConfigSchema

::: zillion.configs.DimensionConfigSchema
    :docstring:
    


## [DimensionValuesField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L487-L492)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DimensionValuesField
    :docstring:
    


## [FieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L630-L648)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FieldConfigSchema
    :docstring:
    


## [FormulaFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L651-L671)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FormulaFieldConfigSchema
    :docstring:
    


## [FormulaMetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L721-L724)

*Bases*: zillion.configs.FormulaFieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.FormulaMetricConfigSchema
    :docstring:
    


## [MetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L715-L718)

*Bases*: zillion.configs.FieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.MetricConfigSchema
    :docstring:
    


## [MetricConfigSchemaMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L674-L712)

::: zillion.configs.MetricConfigSchemaMixin
    :docstring:
    


## [PandasTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1169-L1176)

*Bases*: zillion.configs.Technical

::: zillion.configs.PandasTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [PolyNested](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L421-L439)

*Bases*: marshmallow.fields.Nested

::: zillion.configs.PolyNested
    :docstring:
    


## [RankTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1179-L1186)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.RankTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [RollingTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1207-L1229)

*Bases*: zillion.configs.Technical

::: zillion.configs.RollingTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TableConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L600-L627)

*Bases*: zillion.configs.TableInfoSchema

::: zillion.configs.TableConfigSchema
    :docstring:
    


## [TableInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L979-L984)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.TableInfo
    :docstring:
    :members: create schema_load schema_validate


## [TableInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L562-L597)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TableInfoSchema
    :docstring:
    


## [TableNameField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L777-L782)

*Bases*: marshmallow.fields.String

::: zillion.configs.TableNameField
    :docstring:
    


## [TableTypeField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L554-L559)

*Bases*: marshmallow.fields.Field

::: zillion.configs.TableTypeField
    :docstring:
    


## [Technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1071-L1166)

*Bases*: tlbx.object_utils.MappingMixin, tlbx.logging_utils.PrintMixin

::: zillion.configs.Technical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TechnicalField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L468-L473)

*Bases*: marshmallow.fields.Field

::: zillion.configs.TechnicalField
    :docstring:
    


## [TechnicalInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L460-L465)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TechnicalInfoSchema
    :docstring:
    


## [WarehouseConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L856-L888)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.WarehouseConfigSchema
    :docstring:
    


## [ZillionInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L911-L976)

*Bases*: tlbx.object_utils.MappingMixin

::: zillion.configs.ZillionInfo
    :docstring:
    :members: create schema_load schema_validate


## [create_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1337-L1361)

::: zillion.configs.create_technical
    :docstring:


## [default_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L217-L231)

::: zillion.configs.default_field_display_name
    :docstring:


## [default_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L201-L214)

::: zillion.configs.default_field_name
    :docstring:


## [field_safe_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L182-L198)

::: zillion.configs.field_safe_name
    :docstring:


## [is_active](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L234-L240)

::: zillion.configs.is_active
    :docstring:


## [is_valid_aggregation](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L300-L304)

::: zillion.configs.is_valid_aggregation
    :docstring:


## [is_valid_column_field_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L307-L315)

::: zillion.configs.is_valid_column_field_config
    :docstring:


## [is_valid_connect_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L389-L397)

::: zillion.configs.is_valid_connect_type
    :docstring:


## [is_valid_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L411-L417)

::: zillion.configs.is_valid_datasource_config
    :docstring:


## [is_valid_datasource_connect](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L400-L408)

::: zillion.configs.is_valid_datasource_connect
    :docstring:


## [is_valid_datasource_criteria_conversions](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L350-L386)

::: zillion.configs.is_valid_datasource_criteria_conversions
    :docstring:


## [is_valid_dimension_values](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L341-L347)

::: zillion.configs.is_valid_dimension_values
    :docstring:


## [is_valid_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L278-L287)

::: zillion.configs.is_valid_field_display_name
    :docstring:


## [is_valid_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L264-L275)

::: zillion.configs.is_valid_field_name
    :docstring:


## [is_valid_if_exists](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L257-L261)

::: zillion.configs.is_valid_if_exists
    :docstring:


## [is_valid_sqlalchemy_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L290-L297)

::: zillion.configs.is_valid_sqlalchemy_type
    :docstring:


## [is_valid_table_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L250-L254)

::: zillion.configs.is_valid_table_name
    :docstring:


## [is_valid_table_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L243-L247)

::: zillion.configs.is_valid_table_type
    :docstring:


## [is_valid_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L332-L338)

::: zillion.configs.is_valid_technical
    :docstring:


## [is_valid_technical_mode](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L325-L329)

::: zillion.configs.is_valid_technical_mode
    :docstring:


## [is_valid_technical_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L318-L322)

::: zillion.configs.is_valid_technical_type
    :docstring:


## [load_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L149-L172)

::: zillion.configs.load_datasource_config
    :docstring:


## [load_datasource_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L175-L179)

::: zillion.configs.load_datasource_config_from_env
    :docstring:


## [load_warehouse_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L116-L139)

::: zillion.configs.load_warehouse_config
    :docstring:


## [load_warehouse_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L142-L146)

::: zillion.configs.load_warehouse_config_from_env
    :docstring:


## [load_zillion_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L46-L75)

::: zillion.configs.load_zillion_config
    :docstring:


## [parse_schema_file](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L81-L113)

::: zillion.configs.parse_schema_file
    :docstring:


## [parse_technical_string](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1306-L1334)

::: zillion.configs.parse_technical_string
    :docstring:

