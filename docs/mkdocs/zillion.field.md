[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.field


## [AdHocDimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L726-L729)

*Bases*: zillion.field.AdHocField

::: zillion.field.AdHocDimension
    :docstring:
    :members: copy create from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [AdHocField](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L653-L661)

*Bases*: zillion.field.FormulaField

::: zillion.field.AdHocField
    :docstring:
    :members: copy create from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [AdHocMetric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L664-L723)

*Bases*: zillion.field.FormulaMetric

::: zillion.field.AdHocMetric
    :docstring:
    :members: copy create from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [Dimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L322-L429)

*Bases*: zillion.field.Field

::: zillion.field.Dimension
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields get_values is_valid_value sort to_config


## [Field](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L40-L173)

*Bases*: zillion.configs.ConfigMixin, tlbx.logging_utils.PrintMixin

::: zillion.field.Field
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [FieldManagerMixin](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L761-L1102)

::: zillion.field.FieldManagerMixin
    :docstring:
    :members: add_dimension add_metric directly_has_dimension directly_has_field directly_has_metric get_child_field_managers get_dimension get_dimension_configs get_dimension_names get_dimensions get_direct_dimension_configs get_direct_dimensions get_direct_fields get_direct_metric_configs get_direct_metrics get_field get_field_instances get_field_managers get_field_names get_fields get_metric get_metric_configs get_metric_names get_metrics has_dimension has_field has_metric print_dimensions print_metrics


## [FormulaDimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L548-L567)

*Bases*: zillion.field.FormulaField

::: zillion.field.FormulaDimension
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [FormulaField](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L432-L545)

*Bases*: zillion.field.Field

::: zillion.field.FormulaField
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [FormulaMetric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L570-L650)

*Bases*: zillion.field.FormulaField

::: zillion.field.FormulaMetric
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [Metric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L176-L319)

*Bases*: zillion.field.Field

::: zillion.field.Metric
    :docstring:
    :members: copy from_config get_all_raw_fields get_ds_expression get_final_select_clause get_formula_fields to_config


## [create_dimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L748-L758)

::: zillion.field.create_dimension
    :docstring:


## [create_metric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L732-L745)

::: zillion.field.create_metric
    :docstring:


## [get_conversions_for_type](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1282-L1298)

::: zillion.field.get_conversions_for_type
    :docstring:


## [get_dialect_type_conversions](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1313-L1365)

::: zillion.field.get_dialect_type_conversions
    :docstring:


## [get_table_dimensions](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1131-L1154)

::: zillion.field.get_table_dimensions
    :docstring:


## [get_table_field_column](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1179-L1200)

::: zillion.field.get_table_field_column
    :docstring:


## [get_table_fields](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1157-L1176)

::: zillion.field.get_table_fields
    :docstring:


## [get_table_metrics](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1105-L1128)

::: zillion.field.get_table_metrics
    :docstring:


## [replace_non_named_formula_args](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1301-L1310)

::: zillion.field.replace_non_named_formula_args
    :docstring:


## [sort_by_value_order](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1260-L1279)

::: zillion.field.sort_by_value_order
    :docstring:


## [table_field_allows_grain](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1203-L1220)

::: zillion.field.table_field_allows_grain
    :docstring:


## [values_from_db](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1223-L1257)

::: zillion.field.values_from_db
    :docstring:

