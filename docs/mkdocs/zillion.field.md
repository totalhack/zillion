[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.field


## [AdHocDimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L532-L535)

*Bases*: zillion.field.AdHocField

::: zillion.field.AdHocDimension
    :docstring:
    :members: copy create from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [AdHocField](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L463-L471)

*Bases*: zillion.field.FormulaField

::: zillion.field.AdHocField
    :docstring:
    :members: copy create from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [AdHocMetric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L474-L529)

*Bases*: zillion.field.FormulaMetric

::: zillion.field.AdHocMetric
    :docstring:
    :members: copy create from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [Dimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L264-L269)

*Bases*: zillion.field.Field

::: zillion.field.Dimension
    :docstring:
    :members: copy from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [Field](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L32-L140)

*Bases*: zillion.configs.ConfigMixin, tlbx.logging_utils.PrintMixin

::: zillion.field.Field
    :docstring:
    :members: copy from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [FieldManagerMixin](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L567-L896)

::: zillion.field.FieldManagerMixin
    :docstring:
    :members: add_dimension add_metric directly_has_dimension directly_has_field directly_has_metric get_child_field_managers get_dimension get_dimension_configs get_dimension_names get_dimensions get_direct_dimension_configs get_direct_dimensions get_direct_metric_configs get_direct_metrics get_field get_field_instances get_field_managers get_field_names get_fields get_metric get_metric_configs get_metric_names get_metrics has_dimension has_field has_metric print_dimensions print_metrics


## [FormulaDimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L383-L402)

*Bases*: zillion.field.FormulaField

::: zillion.field.FormulaDimension
    :docstring:
    :members: copy from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [FormulaField](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L272-L380)

*Bases*: zillion.field.Field

::: zillion.field.FormulaField
    :docstring:
    :members: copy from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [FormulaMetric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L405-L460)

*Bases*: zillion.field.FormulaField

::: zillion.field.FormulaMetric
    :docstring:
    :members: copy from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [Metric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L143-L261)

*Bases*: zillion.field.Field

::: zillion.field.Metric
    :docstring:
    :members: copy from_config get_ds_expression get_final_select_clause get_formula_fields to_config


## [create_dimension](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L554-L564)

::: zillion.field.create_dimension
    :docstring:


## [create_metric](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L538-L551)

::: zillion.field.create_metric
    :docstring:


## [get_conversions_for_type](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1017-L1033)

::: zillion.field.get_conversions_for_type
    :docstring:


## [get_dialect_type_conversions](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L1036-L1072)

::: zillion.field.get_dialect_type_conversions
    :docstring:


## [get_table_dimensions](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L925-L948)

::: zillion.field.get_table_dimensions
    :docstring:


## [get_table_field_column](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L973-L994)

::: zillion.field.get_table_field_column
    :docstring:


## [get_table_fields](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L951-L970)

::: zillion.field.get_table_fields
    :docstring:


## [get_table_metrics](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L899-L922)

::: zillion.field.get_table_metrics
    :docstring:


## [table_field_allows_grain](https://github.com/totalhack/zillion/blob/master/zillion/field.py#L997-L1014)

::: zillion.field.table_field_allows_grain
    :docstring:

