[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.configs


## [AdHocFieldSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1063-L1066)

*Bases*: zillion.configs.FormulaFieldConfigSchema

::: zillion.configs.AdHocFieldSchema
    :docstring:
    


## [AdHocMetricSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1069-L1096)

*Bases*: zillion.configs.AdHocFieldSchema

::: zillion.configs.AdHocMetricSchema
    :docstring:
    


## [BaseSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L613-L628)

*Bases*: marshmallow.schema.Schema

::: zillion.configs.BaseSchema
    :docstring:
    


## [BollingerTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1691-L1719)

*Bases*: zillion.configs.RollingTechnical

::: zillion.configs.BollingerTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [ColumnConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L728-L731)

*Bases*: zillion.configs.ColumnInfoSchema

::: zillion.configs.ColumnConfigSchema
    :docstring:
    


## [ColumnFieldConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L690-L695)

*Bases*: abc.Field

::: zillion.configs.ColumnFieldConfigField
    :docstring:
    


## [ColumnFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L674-L687)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnFieldConfigSchema
    :docstring:
    


## [ColumnInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1406-L1492)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.ColumnInfo
    :docstring:
    :members: add_field create field_ds_formula get_criteria_conversion get_field get_field_names get_fields has_field has_field_ds_formula schema_load schema_validate


## [ColumnInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L698-L725)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnInfoSchema
    :docstring:
    


## [ConfigMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1310-L1327)

::: zillion.configs.ConfigMixin
    :docstring:
    :members: from_config to_config


## [CriteriaLimitsField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L658-L663)

*Bases*: abc.Field

::: zillion.configs.CriteriaLimitsField
    :docstring:
    


## [DataSourceConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1205-L1211)

*Bases*: abc.Field

::: zillion.configs.DataSourceConfigField
    :docstring:
    


## [DataSourceConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1150-L1202)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConfigSchema
    :docstring:
    


## [DataSourceConnectField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1117-L1122)

*Bases*: abc.Field

::: zillion.configs.DataSourceConnectField
    :docstring:
    


## [DataSourceConnectSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1107-L1114)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConnectSchema
    :docstring:
    


## [DataSourceCriteriaConversionsField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L647-L655)

*Bases*: abc.Field

::: zillion.configs.DataSourceCriteriaConversionsField
    :docstring:
    


## [DiffTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1643-L1658)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.DiffTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [DimensionConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1049-L1052)

*Bases*: zillion.configs.FieldConfigSchema, zillion.configs.DimensionConfigSchemaMixin

::: zillion.configs.DimensionConfigSchema
    :docstring:
    


## [DimensionConfigSchemaMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1026-L1046)

::: zillion.configs.DimensionConfigSchemaMixin
    :docstring:
    


## [DimensionValuesField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L666-L671)

*Bases*: abc.Field

::: zillion.configs.DimensionValuesField
    :docstring:
    


## [DivisorsConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L995-L1000)

*Bases*: abc.Field

::: zillion.configs.DivisorsConfigField
    :docstring:
    


## [DivisorsConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L973-L992)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DivisorsConfigSchema
    :docstring:
    


## [FieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L877-L900)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FieldConfigSchema
    :docstring:
    


## [FieldMetaNLPConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L846-L860)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FieldMetaNLPConfigSchema
    :docstring:
    


## [FormulaDimensionConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1055-L1060)

*Bases*: zillion.configs.FormulaFieldConfigSchema, zillion.configs.DimensionConfigSchemaMixin

::: zillion.configs.FormulaDimensionConfigSchema
    :docstring:
    


## [FormulaFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L903-L928)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FormulaFieldConfigSchema
    :docstring:
    


## [FormulaMetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1020-L1023)

*Bases*: zillion.configs.FormulaFieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.FormulaMetricConfigSchema
    :docstring:
    


## [MetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1003-L1017)

*Bases*: zillion.configs.FieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.MetricConfigSchema
    :docstring:
    


## [MetricConfigSchemaMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L931-L970)

::: zillion.configs.MetricConfigSchemaMixin
    :docstring:
    


## [NLPEmbeddingTextField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L838-L843)

*Bases*: abc.Field

::: zillion.configs.NLPEmbeddingTextField
    :docstring:
    


## [PandasTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1612-L1624)

*Bases*: zillion.configs.Technical

::: zillion.configs.PandasTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [PatchedField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L23-L33)

*Bases*: marshmallow.fields.Field

::: zillion.configs.PatchedField
    :docstring:
    


## [PolyNested](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L592-L610)

*Bases*: marshmallow.fields.Nested

::: zillion.configs.PolyNested
    :docstring:
    


## [RankTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1627-L1640)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.RankTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [RollingTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1661-L1688)

*Bases*: zillion.configs.Technical

::: zillion.configs.RollingTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TableConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L796-L835)

*Bases*: zillion.configs.TableInfoSchema

::: zillion.configs.TableConfigSchema
    :docstring:
    


## [TableInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1398-L1403)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.TableInfo
    :docstring:
    :members: create schema_load schema_validate


## [TableInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L742-L793)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TableInfoSchema
    :docstring:
    


## [TableNameField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1099-L1104)

*Bases*: abc.Str

::: zillion.configs.TableNameField
    :docstring:
    


## [TableTypeField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L734-L739)

*Bases*: abc.Field

::: zillion.configs.TableTypeField
    :docstring:
    


## [Technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1498-L1609)

*Bases*: tlbx.object_utils.MappingMixin, tlbx.logging_utils.PrintMixin

::: zillion.configs.Technical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TechnicalField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L639-L644)

*Bases*: abc.Field

::: zillion.configs.TechnicalField
    :docstring:
    


## [TechnicalInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L631-L636)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TechnicalInfoSchema
    :docstring:
    


## [WarehouseConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1235-L1307)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.WarehouseConfigSchema
    :docstring:
    


## [WarehouseMetaNLPConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1214-L1232)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.WarehouseMetaNLPConfigSchema
    :docstring:
    


## [ZillionInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1330-L1395)

*Bases*: tlbx.object_utils.MappingMixin

::: zillion.configs.ZillionInfo
    :docstring:
    :members: create schema_load schema_validate


## [check_field_meta_nlp_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L863-L874)

::: zillion.configs.check_field_meta_nlp_config
    :docstring:


## [check_metric_configs](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1125-L1147)

::: zillion.configs.check_metric_configs
    :docstring:


## [create_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1803-L1827)

::: zillion.configs.create_technical
    :docstring:


## [default_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L247-L261)

::: zillion.configs.default_field_display_name
    :docstring:


## [default_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L231-L244)

::: zillion.configs.default_field_name
    :docstring:


## [field_safe_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L212-L228)

::: zillion.configs.field_safe_name
    :docstring:


## [get_aggregation_metrics](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L575-L588)

::: zillion.configs.get_aggregation_metrics
    :docstring:


## [get_divisor_metrics](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L529-L572)

::: zillion.configs.get_divisor_metrics
    :docstring:


## [has_valid_sqlalchemy_type_values](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L330-L337)

::: zillion.configs.has_valid_sqlalchemy_type_values
    :docstring:


## [is_active](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L264-L270)

::: zillion.configs.is_active
    :docstring:


## [is_valid_aggregation](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L340-L349)

::: zillion.configs.is_valid_aggregation
    :docstring:


## [is_valid_column_field_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L352-L360)

::: zillion.configs.is_valid_column_field_config
    :docstring:


## [is_valid_connect_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L480-L488)

::: zillion.configs.is_valid_connect_type
    :docstring:


## [is_valid_criteria_limits](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L467-L477)

::: zillion.configs.is_valid_criteria_limits
    :docstring:


## [is_valid_criteria_row](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L438-L464)

::: zillion.configs.is_valid_criteria_row
    :docstring:


## [is_valid_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L502-L508)

::: zillion.configs.is_valid_datasource_config
    :docstring:


## [is_valid_datasource_connect](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L491-L499)

::: zillion.configs.is_valid_datasource_connect
    :docstring:


## [is_valid_datasource_criteria_conversions](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L395-L435)

::: zillion.configs.is_valid_datasource_criteria_conversions
    :docstring:


## [is_valid_dimension_values](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L386-L392)

::: zillion.configs.is_valid_dimension_values
    :docstring:


## [is_valid_divisors_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L520-L526)

::: zillion.configs.is_valid_divisors_config
    :docstring:


## [is_valid_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L308-L317)

::: zillion.configs.is_valid_field_display_name
    :docstring:


## [is_valid_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L294-L305)

::: zillion.configs.is_valid_field_name
    :docstring:


## [is_valid_field_nlp_embedding_text_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L511-L517)

::: zillion.configs.is_valid_field_nlp_embedding_text_config
    :docstring:


## [is_valid_if_exists](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L287-L291)

::: zillion.configs.is_valid_if_exists
    :docstring:


## [is_valid_sqlalchemy_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L320-L327)

::: zillion.configs.is_valid_sqlalchemy_type
    :docstring:


## [is_valid_table_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L280-L284)

::: zillion.configs.is_valid_table_name
    :docstring:


## [is_valid_table_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L273-L277)

::: zillion.configs.is_valid_table_type
    :docstring:


## [is_valid_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L377-L383)

::: zillion.configs.is_valid_technical
    :docstring:


## [is_valid_technical_mode](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L370-L374)

::: zillion.configs.is_valid_technical_mode
    :docstring:


## [is_valid_technical_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L363-L367)

::: zillion.configs.is_valid_technical_type
    :docstring:


## [load_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L167-L183)

::: zillion.configs.load_datasource_config
    :docstring:


## [load_datasource_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L186-L190)

::: zillion.configs.load_datasource_config_from_env
    :docstring:


## [load_warehouse_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L139-L157)

::: zillion.configs.load_warehouse_config
    :docstring:


## [load_warehouse_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L160-L164)

::: zillion.configs.load_warehouse_config_from_env
    :docstring:


## [parse_schema_file](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L114-L136)

::: zillion.configs.parse_schema_file
    :docstring:


## [parse_technical_string](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1772-L1800)

::: zillion.configs.parse_technical_string
    :docstring:


## [table_safe_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L193-L209)

::: zillion.configs.table_safe_name
    :docstring:

