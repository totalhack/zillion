[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.datasource


## [AdHocDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1456-L1624)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.AdHocDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [CSVDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1643-L1653)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.CSVDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [DataSource](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L461-L1453)

*Bases*: zillion.field.FieldManagerMixin, tlbx.logging_utils.PrintMixin

::: zillion.datasource.DataSource
    :docstring:
    :members: add_dimension add_metric apply_config directly_has_dimension directly_has_field directly_has_metric find_descendent_tables find_neighbor_tables find_possible_table_sets from_data_url from_datatables get_child_field_managers get_columns_with_field get_dialect_name get_dim_tables_with_dim get_dimension get_dimension_configs get_dimension_names get_dimensions get_direct_dimension_configs get_direct_dimensions get_direct_fields get_direct_metric_configs get_direct_metrics get_field get_field_instances get_field_managers get_field_names get_fields get_metric get_metric_configs get_metric_names get_metric_tables_with_metric get_metrics get_params get_possible_joins get_table get_tables_with_field has_dimension has_field has_metric has_table print_dimensions print_info print_metrics


## [ExcelDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1656-L1668)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.ExcelDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [GoogleSheetsDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1698-L1720)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.GoogleSheetsDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [HTMLDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1682-L1695)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.HTMLDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [JSONDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1671-L1679)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.JSONDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [Join](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L308-L418)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.Join
    :docstring:
    :members: add_field add_fields add_join_part_tables combine get_covered_fields join_fields_for_table join_parts_for_table


## [JoinPart](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L298-L305)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.JoinPart
    :docstring:
    


## [NeighborTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L451-L458)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.NeighborTable
    :docstring:
    


## [SQLiteDataTable](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1627-L1640)

*Bases*: zillion.datasource.AdHocDataTable

::: zillion.datasource.SQLiteDataTable
    :docstring:
    :members: get_dataframe table_exists to_sql


## [TableSet](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L247-L295)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.datasource.TableSet
    :docstring:
    :members: get_covered_fields get_covered_metrics


## [connect_url_to_metadata](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L64-L73)

::: zillion.datasource.connect_url_to_metadata
    :docstring:


## [data_url_to_metadata](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L104-L133)

::: zillion.datasource.data_url_to_metadata
    :docstring:


## [datatable_from_config](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L1723-L1767)

::: zillion.datasource.datatable_from_config
    :docstring:


## [get_adhoc_datasource_filename](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L186-L189)

::: zillion.datasource.get_adhoc_datasource_filename
    :docstring:


## [get_adhoc_datasource_url](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L192-L194)

::: zillion.datasource.get_adhoc_datasource_url
    :docstring:


## [get_ds_config_context](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L51-L53)

::: zillion.datasource.get_ds_config_context
    :docstring:


## [join_from_path](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L421-L448)

::: zillion.datasource.join_from_path
    :docstring:


## [metadata_from_connect](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L136-L156)

::: zillion.datasource.metadata_from_connect
    :docstring:


## [parse_replace_after](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L76-L101)

::: zillion.datasource.parse_replace_after
    :docstring:


## [populate_url_context](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L56-L61)

::: zillion.datasource.populate_url_context
    :docstring:


## [reflect_metadata](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L159-L183)

::: zillion.datasource.reflect_metadata
    :docstring:


## [url_connect](https://github.com/totalhack/zillion/blob/master/zillion/datasource.py#L197-L244)

::: zillion.datasource.url_connect
    :docstring:

