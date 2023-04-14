[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.configs


## [AdHocFieldSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L917-L920)

*Bases*: zillion.configs.FormulaFieldConfigSchema

::: zillion.configs.AdHocFieldSchema
    :docstring:
    


## [AdHocMetricSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L923-L951)

*Bases*: zillion.configs.AdHocFieldSchema

::: zillion.configs.AdHocMetricSchema
    :docstring:
    


## [BaseSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L476-L491)

*Bases*: marshmallow.schema.Schema

::: zillion.configs.BaseSchema
    :docstring:
    


## [BollingerTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1497-L1518)

*Bases*: zillion.configs.RollingTechnical

::: zillion.configs.BollingerTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [ColumnConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L582-L585)

*Bases*: zillion.configs.ColumnInfoSchema

::: zillion.configs.ColumnConfigSchema
    :docstring:
    


## [ColumnFieldConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L547-L552)

*Bases*: marshmallow.fields.Field

::: zillion.configs.ColumnFieldConfigField
    :docstring:
    


## [ColumnFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L529-L544)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnFieldConfigSchema
    :docstring:
    


## [ColumnInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1244-L1330)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.ColumnInfo
    :docstring:
    :members: add_field create field_ds_formula get_criteria_conversion get_field get_field_names get_fields has_field has_field_ds_formula schema_load schema_validate


## [ColumnInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L555-L579)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.ColumnInfoSchema
    :docstring:
    


## [ConfigMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1148-L1165)

::: zillion.configs.ConfigMixin
    :docstring:
    :members: from_config to_config


## [DataSourceConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1040-L1046)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceConfigField
    :docstring:
    


## [DataSourceConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L981-L1037)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConfigSchema
    :docstring:
    


## [DataSourceConnectField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L973-L978)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceConnectField
    :docstring:
    


## [DataSourceConnectSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L962-L970)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DataSourceConnectSchema
    :docstring:
    


## [DataSourceCriteriaConversionsField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L510-L518)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DataSourceCriteriaConversionsField
    :docstring:
    


## [DiffTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1454-L1469)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.DiffTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [DimensionConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L903-L906)

*Bases*: zillion.configs.FieldConfigSchema, zillion.configs.DimensionConfigSchemaMixin

::: zillion.configs.DimensionConfigSchema
    :docstring:
    


## [DimensionConfigSchemaMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L880-L900)

::: zillion.configs.DimensionConfigSchemaMixin
    :docstring:
    


## [DimensionValuesField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L521-L526)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DimensionValuesField
    :docstring:
    


## [DivisorsConfigField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L849-L854)

*Bases*: marshmallow.fields.Field

::: zillion.configs.DivisorsConfigField
    :docstring:
    


## [DivisorsConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L827-L846)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.DivisorsConfigSchema
    :docstring:
    


## [FieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L730-L753)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FieldConfigSchema
    :docstring:
    


## [FieldMetaNLPConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L699-L713)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FieldMetaNLPConfigSchema
    :docstring:
    


## [FormulaDimensionConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L909-L914)

*Bases*: zillion.configs.FormulaFieldConfigSchema, zillion.configs.DimensionConfigSchemaMixin

::: zillion.configs.FormulaDimensionConfigSchema
    :docstring:
    


## [FormulaFieldConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L756-L781)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.FormulaFieldConfigSchema
    :docstring:
    


## [FormulaMetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L874-L877)

*Bases*: zillion.configs.FormulaFieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.FormulaMetricConfigSchema
    :docstring:
    


## [MetricConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L857-L871)

*Bases*: zillion.configs.FieldConfigSchema, zillion.configs.MetricConfigSchemaMixin

::: zillion.configs.MetricConfigSchema
    :docstring:
    


## [MetricConfigSchemaMixin](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L784-L824)

::: zillion.configs.MetricConfigSchemaMixin
    :docstring:
    


## [NLPEmbeddingTextField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L691-L696)

*Bases*: marshmallow.fields.Field

::: zillion.configs.NLPEmbeddingTextField
    :docstring:
    


## [PandasTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1434-L1441)

*Bases*: zillion.configs.Technical

::: zillion.configs.PandasTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [PolyNested](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L455-L473)

*Bases*: marshmallow.fields.Nested

::: zillion.configs.PolyNested
    :docstring:
    


## [RankTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1444-L1451)

*Bases*: zillion.configs.PandasTechnical

::: zillion.configs.RankTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [RollingTechnical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1472-L1494)

*Bases*: zillion.configs.Technical

::: zillion.configs.RollingTechnical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TableConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L649-L688)

*Bases*: zillion.configs.TableInfoSchema

::: zillion.configs.TableConfigSchema
    :docstring:
    


## [TableInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1236-L1241)

*Bases*: zillion.configs.ZillionInfo, tlbx.logging_utils.PrintMixin

::: zillion.configs.TableInfo
    :docstring:
    :members: create schema_load schema_validate


## [TableInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L596-L646)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TableInfoSchema
    :docstring:
    


## [TableNameField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L954-L959)

*Bases*: marshmallow.fields.String

::: zillion.configs.TableNameField
    :docstring:
    


## [TableTypeField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L588-L593)

*Bases*: marshmallow.fields.Field

::: zillion.configs.TableTypeField
    :docstring:
    


## [Technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1336-L1431)

*Bases*: tlbx.object_utils.MappingMixin, tlbx.logging_utils.PrintMixin

::: zillion.configs.Technical
    :docstring:
    :members: apply get_default_mode parse_technical_string_params


## [TechnicalField](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L502-L507)

*Bases*: marshmallow.fields.Field

::: zillion.configs.TechnicalField
    :docstring:
    


## [TechnicalInfoSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L494-L499)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.TechnicalInfoSchema
    :docstring:
    


## [WarehouseConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1070-L1145)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.WarehouseConfigSchema
    :docstring:
    


## [WarehouseMetaNLPConfigSchema](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1049-L1067)

*Bases*: zillion.configs.BaseSchema

::: zillion.configs.WarehouseMetaNLPConfigSchema
    :docstring:
    


## [ZillionInfo](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1168-L1233)

*Bases*: tlbx.object_utils.MappingMixin

::: zillion.configs.ZillionInfo
    :docstring:
    :members: create schema_load schema_validate


## [check_field_meta_nlp_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L716-L727)

::: zillion.configs.check_field_meta_nlp_config
    :docstring:


## [create_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1602-L1626)

::: zillion.configs.create_technical
    :docstring:


## [default_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L178-L192)

::: zillion.configs.default_field_display_name
    :docstring:


## [default_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L162-L175)

::: zillion.configs.default_field_name
    :docstring:


## [field_safe_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L143-L159)

::: zillion.configs.field_safe_name
    :docstring:


## [get_divisor_metrics](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L413-L451)

::: zillion.configs.get_divisor_metrics
    :docstring:


## [has_valid_sqlalchemy_type_values](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L261-L268)

::: zillion.configs.has_valid_sqlalchemy_type_values
    :docstring:


## [is_active](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L195-L201)

::: zillion.configs.is_active
    :docstring:


## [is_valid_aggregation](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L271-L275)

::: zillion.configs.is_valid_aggregation
    :docstring:


## [is_valid_column_field_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L278-L286)

::: zillion.configs.is_valid_column_field_config
    :docstring:


## [is_valid_connect_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L364-L372)

::: zillion.configs.is_valid_connect_type
    :docstring:


## [is_valid_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L386-L392)

::: zillion.configs.is_valid_datasource_config
    :docstring:


## [is_valid_datasource_connect](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L375-L383)

::: zillion.configs.is_valid_datasource_connect
    :docstring:


## [is_valid_datasource_criteria_conversions](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L321-L361)

::: zillion.configs.is_valid_datasource_criteria_conversions
    :docstring:


## [is_valid_dimension_values](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L312-L318)

::: zillion.configs.is_valid_dimension_values
    :docstring:


## [is_valid_divisors_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L404-L410)

::: zillion.configs.is_valid_divisors_config
    :docstring:


## [is_valid_field_display_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L239-L248)

::: zillion.configs.is_valid_field_display_name
    :docstring:


## [is_valid_field_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L225-L236)

::: zillion.configs.is_valid_field_name
    :docstring:


## [is_valid_field_nlp_embedding_text_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L395-L401)

::: zillion.configs.is_valid_field_nlp_embedding_text_config
    :docstring:


## [is_valid_if_exists](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L218-L222)

::: zillion.configs.is_valid_if_exists
    :docstring:


## [is_valid_sqlalchemy_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L251-L258)

::: zillion.configs.is_valid_sqlalchemy_type
    :docstring:


## [is_valid_table_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L211-L215)

::: zillion.configs.is_valid_table_name
    :docstring:


## [is_valid_table_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L204-L208)

::: zillion.configs.is_valid_table_type
    :docstring:


## [is_valid_technical](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L303-L309)

::: zillion.configs.is_valid_technical
    :docstring:


## [is_valid_technical_mode](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L296-L300)

::: zillion.configs.is_valid_technical_mode
    :docstring:


## [is_valid_technical_type](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L289-L293)

::: zillion.configs.is_valid_technical_type
    :docstring:


## [load_datasource_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L98-L114)

::: zillion.configs.load_datasource_config
    :docstring:


## [load_datasource_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L117-L121)

::: zillion.configs.load_datasource_config_from_env
    :docstring:


## [load_warehouse_config](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L72-L88)

::: zillion.configs.load_warehouse_config
    :docstring:


## [load_warehouse_config_from_env](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L91-L95)

::: zillion.configs.load_warehouse_config_from_env
    :docstring:


## [parse_schema_file](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L47-L69)

::: zillion.configs.parse_schema_file
    :docstring:


## [parse_technical_string](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L1571-L1599)

::: zillion.configs.parse_technical_string
    :docstring:


## [table_safe_name](https://github.com/totalhack/zillion/blob/master/zillion/configs.py#L124-L140)

::: zillion.configs.table_safe_name
    :docstring:

