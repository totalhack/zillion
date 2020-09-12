[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.configs


## [AdHocFieldSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L697-L700)

*Bases*: zillion.configs.FormulaFieldConfigSchema

::: zillion.configs.AdHocFieldSchema
    :docstring:
    


## [AdHocMetricSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L703-L721)

*Bases*: zillion.configs.AdHocFieldSchema

::: zillion.configs.AdHocMetricSchema
    :docstring:
    


## [BaseSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L423-L429)

*Bases*: marshmallow.schema.Schema

::: zillion.configs.BaseSchema
    :docstring:
    


## [BollingerTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1179-L1200)

*Bases*: zillion.configs.RollingTechnical

::: zillion.configs.BollingerTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [ColumnConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L512-L515)

*Bases*: zillion.configs.ColumnInfoSchema

::: zillion.configs.ColumnConfigSchema
    :docstring:
    


## [ColumnFieldConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L477-L482)

*Bases*: marshmallow.fields.Field

::: zillion.configs.ColumnFieldConfigField
    :docstring:
    


## [ColumnFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L459-L474)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnFieldConfigSchema
    :docstring:
    


## [ColumnInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L934-L1012)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.ColumnInfo
    :docstring:
    :members: add_field create field_ds_formula get_criteria_conversion get_field get_field_names get_fields has_field schema_load schema_validate


## [ColumnInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L485-L509)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnInfoSchema
    :docstring:
    


## [ConfigMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L838-L855)

::: zillion.configs.ConfigMixin
    :docstring:
    :members: from_config to_config


## [DataSourceConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L794-L800)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceConfigField
    :docstring:
    


## [DataSourceConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L751-L791)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConfigSchema
    :docstring:
    


## [DataSourceConnectField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L743-L748)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceConnectField
    :docstring:
    


## [DataSourceConnectSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L732-L740)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConnectSchema
    :docstring:
    


## [DataSourceCriteriaConversionsField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L448-L456)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceCriteriaConversionsField
    :docstring:
    


## [DiffTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1136-L1151)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.DiffTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [DimensionConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L691-L694)

*Bases*: zillion.configs.FieldConfigSchema

::: zillion.configs.DimensionConfigSchema
    :docstring:
    


## [FieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L594-L612)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FieldConfigSchema
    :docstring:
    


## [FormulaFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L615-L635)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FormulaFieldConfigSchema
    :docstring:
    


## [FormulaMetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L685-L688)

*Bases*: zillion.configs.FormulaFieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.FormulaMetricConfigSchema
    :docstring:
    


## [MetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L679-L682)

*Bases*: zillion.configs.FieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.MetricConfigSchema
    :docstring:
    


## [MetricConfigSchemaMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L638-L676)

::: zillion.configs.MetricConfigSchemaMixin
    :docstring:
    


## [PandasTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1116-L1123)

*Bases*: zillion.configs.Technical

::: zillion.configs.PandasTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [PolyNested](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L402-L420)

*Bases*: marshmallow.fields.Nested

::: zillion.configs.PolyNested
    :docstring:
    


## [RankTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1126-L1133)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.RankTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [RollingTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1154-L1176)

*Bases*: zillion.configs.Technical

::: zillion.configs.RollingTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TableConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L564-L591)

*Bases*: zillion.configs.TableInfoSchema

::: zillion.configs.TableConfigSchema
    :docstring:
    


## [TableInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L926-L931)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.TableInfo
    :docstring:
    :members: create schema_load schema_validate


## [TableInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L526-L561)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TableInfoSchema
    :docstring:
    


## [TableNameField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L724-L729)

*Bases*: marshmallow.fields.String

::: zillion.configs.TableNameField
    :docstring:
    


## [TableTypeField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L518-L523)

*Bases*: marshmallow.fields.Field

::: zillion.configs.TableTypeField
    :docstring:
    


## [Technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1018-L1113)

*Bases*: tlbx.object_utils.MappingMixin, tlbx.logging_utils.PrintMixin

::: zillion.configs.Technical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TechnicalField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L440-L445)

*Bases*: marshmallow.fields.Field

::: zillion.configs.TechnicalField
    :docstring:
    


## [TechnicalInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L432-L437)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TechnicalInfoSchema
    :docstring:
    


## [WarehouseConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L803-L835)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.WarehouseConfigSchema
    :docstring:
    


## [ZillionInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L858-L923)

*Bases*: tlbx.object_utils.MappingMixin

::: zillion.configs.ZillionInfo
    :docstring:
    :members: create schema_load schema_validate


## [create_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1284-L1308)

::: zillion.configs.create_technical
    :docstring:


## [default_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L207-L221)

::: zillion.configs.default_field_display_name
    :docstring:


## [default_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L191-L204)

::: zillion.configs.default_field_name
    :docstring:


## [field_safe_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L172-L188)

::: zillion.configs.field_safe_name
    :docstring:


## [is_active](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L224-L230)

::: zillion.configs.is_active
    :docstring:


## [is_valid_aggregation](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L290-L294)

::: zillion.configs.is_valid_aggregation
    :docstring:


## [is_valid_column_field_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L297-L305)

::: zillion.configs.is_valid_column_field_config
    :docstring:


## [is_valid_connect_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L370-L378)

::: zillion.configs.is_valid_connect_type
    :docstring:


## [is_valid_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L392-L398)

::: zillion.configs.is_valid_datasource_config
    :docstring:


## [is_valid_datasource_connect](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L381-L389)

::: zillion.configs.is_valid_datasource_connect
    :docstring:


## [is_valid_datasource_criteria_conversions](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L331-L367)

::: zillion.configs.is_valid_datasource_criteria_conversions
    :docstring:


## [is_valid_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L268-L277)

::: zillion.configs.is_valid_field_display_name
    :docstring:


## [is_valid_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L254-L265)

::: zillion.configs.is_valid_field_name
    :docstring:


## [is_valid_if_exists](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L247-L251)

::: zillion.configs.is_valid_if_exists
    :docstring:


## [is_valid_sqlalchemy_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L280-L287)

::: zillion.configs.is_valid_sqlalchemy_type
    :docstring:


## [is_valid_table_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L240-L244)

::: zillion.configs.is_valid_table_name
    :docstring:


## [is_valid_table_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L233-L237)

::: zillion.configs.is_valid_table_type
    :docstring:


## [is_valid_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L322-L328)

::: zillion.configs.is_valid_technical
    :docstring:


## [is_valid_technical_mode](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L315-L319)

::: zillion.configs.is_valid_technical_mode
    :docstring:


## [is_valid_technical_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L308-L312)

::: zillion.configs.is_valid_technical_type
    :docstring:


## [load_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L139-L162)

::: zillion.configs.load_datasource_config
    :docstring:


## [load_datasource_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L165-L169)

::: zillion.configs.load_datasource_config_from_env
    :docstring:


## [load_warehouse_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L106-L129)

::: zillion.configs.load_warehouse_config
    :docstring:


## [load_warehouse_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L132-L136)

::: zillion.configs.load_warehouse_config_from_env
    :docstring:


## [load_zillion_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L45-L65)

::: zillion.configs.load_zillion_config
    :docstring:


## [parse_schema_file](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L71-L103)

::: zillion.configs.parse_schema_file
    :docstring:


## [parse_technical_string](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1253-L1281)

::: zillion.configs.parse_technical_string
    :docstring:

