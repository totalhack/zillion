[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.configs


## [AdHocFieldSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L982-L985)

*Bases*: zillion.configs.FormulaFieldConfigSchema

::: zillion.configs.AdHocFieldSchema
    :docstring:
    


## [AdHocMetricSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L988-L1015)

*Bases*: zillion.configs.AdHocFieldSchema

::: zillion.configs.AdHocMetricSchema
    :docstring:
    


## [BaseSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L541-L556)

*Bases*: marshmallow.schema.Schema

::: zillion.configs.BaseSchema
    :docstring:
    


## [BollingerTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1609-L1637)

*Bases*: zillion.configs.RollingTechnical

::: zillion.configs.BollingerTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [ColumnConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L648-L651)

*Bases*: zillion.configs.ColumnInfoSchema

::: zillion.configs.ColumnConfigSchema
    :docstring:
    


## [ColumnFieldConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L610-L615)

*Bases*: marshmallow.fields.Field

::: zillion.configs.ColumnFieldConfigField
    :docstring:
    


## [ColumnFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L594-L607)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnFieldConfigSchema
    :docstring:
    


## [ColumnInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1324-L1410)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.ColumnInfo
    :docstring:
    :members: add_field create field_ds_formula get_criteria_conversion get_field get_field_names get_fields has_field has_field_ds_formula schema_load schema_validate


## [ColumnInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L618-L645)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnInfoSchema
    :docstring:
    


## [ConfigMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1228-L1245)

::: zillion.configs.ConfigMixin
    :docstring:
    :members: from_config to_config


## [DataSourceConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1124-L1130)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceConfigField
    :docstring:
    


## [DataSourceConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1069-L1121)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConfigSchema
    :docstring:
    


## [DataSourceConnectField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1036-L1041)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceConnectField
    :docstring:
    


## [DataSourceConnectSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1026-L1033)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConnectSchema
    :docstring:
    


## [DataSourceCriteriaConversionsField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L575-L583)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceCriteriaConversionsField
    :docstring:
    


## [DiffTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1561-L1576)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.DiffTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [DimensionConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L968-L971)

*Bases*: zillion.configs.FieldConfigSchema, zillion.configs.DimensionConfigSchemaMixin

::: zillion.configs.DimensionConfigSchema
    :docstring:
    


## [DimensionConfigSchemaMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L945-L965)

::: zillion.configs.DimensionConfigSchemaMixin
    :docstring:
    


## [DimensionValuesField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L586-L591)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DimensionValuesField
    :docstring:
    


## [DivisorsConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L914-L919)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DivisorsConfigField
    :docstring:
    


## [DivisorsConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L892-L911)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DivisorsConfigSchema
    :docstring:
    


## [FieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L796-L819)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FieldConfigSchema
    :docstring:
    


## [FieldMetaNLPConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L765-L779)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FieldMetaNLPConfigSchema
    :docstring:
    


## [FormulaDimensionConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L974-L979)

*Bases*: zillion.configs.FormulaFieldConfigSchema, zillion.configs.DimensionConfigSchemaMixin

::: zillion.configs.FormulaDimensionConfigSchema
    :docstring:
    


## [FormulaFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L822-L847)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FormulaFieldConfigSchema
    :docstring:
    


## [FormulaMetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L939-L942)

*Bases*: zillion.configs.FormulaFieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.FormulaMetricConfigSchema
    :docstring:
    


## [MetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L922-L936)

*Bases*: zillion.configs.FieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.MetricConfigSchema
    :docstring:
    


## [MetricConfigSchemaMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L850-L889)

::: zillion.configs.MetricConfigSchemaMixin
    :docstring:
    


## [NLPEmbeddingTextField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L757-L762)

*Bases*: marshmallow.fields.Field

::: zillion.configs.NLPEmbeddingTextField
    :docstring:
    


## [PandasTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1530-L1542)

*Bases*: zillion.configs.Technical

::: zillion.configs.PandasTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [PolyNested](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L520-L538)

*Bases*: marshmallow.fields.Nested

::: zillion.configs.PolyNested
    :docstring:
    


## [RankTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1545-L1558)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.RankTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [RollingTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1579-L1606)

*Bases*: zillion.configs.Technical

::: zillion.configs.RollingTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TableConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L715-L754)

*Bases*: zillion.configs.TableInfoSchema

::: zillion.configs.TableConfigSchema
    :docstring:
    


## [TableInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1316-L1321)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.TableInfo
    :docstring:
    :members: create schema_load schema_validate


## [TableInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L662-L712)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TableInfoSchema
    :docstring:
    


## [TableNameField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1018-L1023)

*Bases*: marshmallow.fields.String

::: zillion.configs.TableNameField
    :docstring:
    


## [TableTypeField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L654-L659)

*Bases*: marshmallow.fields.Field

::: zillion.configs.TableTypeField
    :docstring:
    


## [Technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1416-L1527)

*Bases*: tlbx.object_utils.MappingMixin, tlbx.logging_utils.PrintMixin

::: zillion.configs.Technical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TechnicalField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L567-L572)

*Bases*: marshmallow.fields.Field

::: zillion.configs.TechnicalField
    :docstring:
    


## [TechnicalInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L559-L564)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TechnicalInfoSchema
    :docstring:
    


## [WarehouseConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1154-L1225)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.WarehouseConfigSchema
    :docstring:
    


## [WarehouseMetaNLPConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1133-L1151)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.WarehouseMetaNLPConfigSchema
    :docstring:
    


## [ZillionInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1248-L1313)

*Bases*: tlbx.object_utils.MappingMixin

::: zillion.configs.ZillionInfo
    :docstring:
    :members: create schema_load schema_validate


## [check_field_meta_nlp_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L782-L793)

::: zillion.configs.check_field_meta_nlp_config
    :docstring:


## [check_metric_configs](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1044-L1066)

::: zillion.configs.check_metric_configs
    :docstring:


## [create_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1721-L1745)

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


## [get_aggregation_metrics](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L503-L516)

::: zillion.configs.get_aggregation_metrics
    :docstring:


## [get_divisor_metrics](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L457-L500)

::: zillion.configs.get_divisor_metrics
    :docstring:


## [has_valid_sqlalchemy_type_values](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L300-L307)

::: zillion.configs.has_valid_sqlalchemy_type_values
    :docstring:


## [is_active](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L234-L240)

::: zillion.configs.is_active
    :docstring:


## [is_valid_aggregation](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L310-L319)

::: zillion.configs.is_valid_aggregation
    :docstring:


## [is_valid_column_field_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L322-L330)

::: zillion.configs.is_valid_column_field_config
    :docstring:


## [is_valid_connect_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L408-L416)

::: zillion.configs.is_valid_connect_type
    :docstring:


## [is_valid_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L430-L436)

::: zillion.configs.is_valid_datasource_config
    :docstring:


## [is_valid_datasource_connect](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L419-L427)

::: zillion.configs.is_valid_datasource_connect
    :docstring:


## [is_valid_datasource_criteria_conversions](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L365-L405)

::: zillion.configs.is_valid_datasource_criteria_conversions
    :docstring:


## [is_valid_dimension_values](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L356-L362)

::: zillion.configs.is_valid_dimension_values
    :docstring:


## [is_valid_divisors_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L448-L454)

::: zillion.configs.is_valid_divisors_config
    :docstring:


## [is_valid_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L278-L287)

::: zillion.configs.is_valid_field_display_name
    :docstring:


## [is_valid_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L264-L275)

::: zillion.configs.is_valid_field_name
    :docstring:


## [is_valid_field_nlp_embedding_text_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L439-L445)

::: zillion.configs.is_valid_field_nlp_embedding_text_config
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


## [is_valid_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L347-L353)

::: zillion.configs.is_valid_technical
    :docstring:


## [is_valid_technical_mode](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L340-L344)

::: zillion.configs.is_valid_technical_mode
    :docstring:


## [is_valid_technical_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L333-L337)

::: zillion.configs.is_valid_technical_type
    :docstring:


## [load_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L137-L153)

::: zillion.configs.load_datasource_config
    :docstring:


## [load_datasource_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L156-L160)

::: zillion.configs.load_datasource_config_from_env
    :docstring:


## [load_warehouse_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L111-L127)

::: zillion.configs.load_warehouse_config
    :docstring:


## [load_warehouse_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L130-L134)

::: zillion.configs.load_warehouse_config_from_env
    :docstring:


## [parse_schema_file](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L86-L108)

::: zillion.configs.parse_schema_file
    :docstring:


## [parse_technical_string](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1690-L1718)

::: zillion.configs.parse_technical_string
    :docstring:


## [table_safe_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L163-L179)

::: zillion.configs.table_safe_name
    :docstring:

