[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.datasource


## [AdHocDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1457-L1625)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.AdHocDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [CSVDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1644-L1654)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.CSVDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [DataSource](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L462-L1454)

*Bases*: zillion.field.FieldManagerMixin, tlbx.logging_utils.PrintMixin

::: zillion.datasource.DataSource
    :docstring:
    :members: add_dimension add_metric apply_config directly_has_dimension directly_has_field directly_has_metric find_descendent_tables find_neighbor_tables find_possible_table_sets from_data_url from_datatables get_child_field_managers get_columns_with_field get_dialect_name get_dim_tables_with_dim get_dimension get_dimension_configs get_dimension_names get_dimensions get_direct_dimension_configs get_direct_dimensions get_direct_fields get_direct_metric_configs get_direct_metrics get_field get_field_instances get_field_managers get_field_names get_fields get_metric get_metric_configs get_metric_names get_metric_tables_with_metric get_metrics get_params get_possible_joins get_table get_tables_with_field has_dimension has_field has_metric has_table print_dimensions print_info print_metrics


## [ExcelDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1657-L1669)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.ExcelDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [GoogleSheetsDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1699-L1721)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.GoogleSheetsDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [HTMLDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1683-L1696)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.HTMLDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [JSONDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1672-L1680)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.JSONDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [Join](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L309-L419)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.Join
    :docstring:
    :members: add_field add_fields add_join_part_tables combine get_covered_fields join_fields_for_table join_parts_for_table


## [JoinPart](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L299-L306)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.JoinPart
    :docstring:
    


## [NeighborTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L452-L459)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.NeighborTable
    :docstring:
    


## [SQLiteDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1628-L1641)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.SQLiteDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [TableSet](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L248-L296)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.TableSet
    :docstring:
    :members: get_covered_fields get_covered_metrics


## [connect_url_to_metadata](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L65-L74)

::: zillion.datasource.connect_url_to_metadata
    :docstring:


## [data_url_to_metadata](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L105-L134)

::: zillion.datasource.data_url_to_metadata
    :docstring:


## [datatable_from_config](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1724-L1768)

::: zillion.datasource.datatable_from_config
    :docstring:


## [get_adhoc_datasource_filename](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L187-L190)

::: zillion.datasource.get_adhoc_datasource_filename
    :docstring:


## [get_adhoc_datasource_url](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L193-L195)

::: zillion.datasource.get_adhoc_datasource_url
    :docstring:


## [get_ds_config_context](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L52-L54)

::: zillion.datasource.get_ds_config_context
    :docstring:


## [join_from_path](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L422-L449)

::: zillion.datasource.join_from_path
    :docstring:


## [metadata_from_connect](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L137-L157)

::: zillion.datasource.metadata_from_connect
    :docstring:


## [parse_replace_after](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L77-L102)

::: zillion.datasource.parse_replace_after
    :docstring:


## [populate_url_context](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L57-L62)

::: zillion.datasource.populate_url_context
    :docstring:


## [reflect_metadata](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L160-L184)

::: zillion.datasource.reflect_metadata
    :docstring:


## [url_connect](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L198-L245)

::: zillion.datasource.url_connect
    :docstring:

