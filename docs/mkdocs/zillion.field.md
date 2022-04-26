[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.field


## [AdHocDimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L757-L760)

*Bases*: zillion.field.AdHocField

::: zillion.field.AdHocDimension
    :docstring:
    :members: copy create from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [AdHocField](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L674-L682)

*Bases*: zillion.field.FormulaField

::: zillion.field.AdHocField
    :docstring:
    :members: copy create from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [AdHocMetric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L685-L754)

*Bases*: zillion.field.FormulaMetric

::: zillion.field.AdHocMetric
    :docstring:
    :members: copy create from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [Dimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L336-L450)

*Bases*: zillion.field.Field

::: zillion.field.Dimension
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields get_values is_valid_value sort to_config


## [Field](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L40-L173)

*Bases*: zillion.configs.ConfigMixin, tlbx.logging_utils.PrintMixin

::: zillion.field.Field
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [FieldManagerMixin](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L792-L1133)

::: zillion.field.FieldManagerMixin
    :docstring:
    :members: add_dimension add_metric directly_has_dimension directly_has_field directly_has_metric get_child_field_managers get_dimension get_dimension_configs get_dimension_names get_dimensions get_direct_dimension_configs get_direct_dimensions get_direct_fields get_direct_metric_configs get_direct_metrics get_field get_field_instances get_field_managers get_field_names get_fields get_metric get_metric_configs get_metric_names get_metrics has_dimension has_field has_metric print_dimensions print_metrics


## [FormulaDimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L569-L588)

*Bases*: zillion.field.FormulaField

::: zillion.field.FormulaDimension
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [FormulaField](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L453-L566)

*Bases*: zillion.field.Field

::: zillion.field.FormulaField
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [FormulaMetric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L591-L671)

*Bases*: zillion.field.FormulaField

::: zillion.field.FormulaMetric
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [Metric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L176-L333)

*Bases*: zillion.field.Field

::: zillion.field.Metric
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [create_dimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L779-L789)

::: zillion.field.create_dimension
    :docstring:


## [create_metric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L763-L776)

::: zillion.field.create_metric
    :docstring:


## [get_conversions_for_type](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1313-L1329)

::: zillion.field.get_conversions_for_type
    :docstring:


## [get_dialect_type_conversions](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1344-L1397)

::: zillion.field.get_dialect_type_conversions
    :docstring:


## [get_table_dimensions](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1162-L1185)

::: zillion.field.get_table_dimensions
    :docstring:


## [get_table_field_column](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1210-L1231)

::: zillion.field.get_table_field_column
    :docstring:


## [get_table_fields](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1188-L1207)

::: zillion.field.get_table_fields
    :docstring:


## [get_table_metrics](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1136-L1159)

::: zillion.field.get_table_metrics
    :docstring:


## [replace_non_named_formula_args](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1332-L1341)

::: zillion.field.replace_non_named_formula_args
    :docstring:


## [sort_by_value_order](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1291-L1310)

::: zillion.field.sort_by_value_order
    :docstring:


## [table_field_allows_grain](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1234-L1251)

::: zillion.field.table_field_allows_grain
    :docstring:


## [values_from_db](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1254-L1288)

::: zillion.field.values_from_db
    :docstring:

