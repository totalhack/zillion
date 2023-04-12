[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.report


## [BaseCombinedResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L707-L833)

::: zillion.report.BaseCombinedResult
    :docstring:
    :members: add_warning clean_up create_table get_conn get_cursor get_final_result get_metric_clause ifnull_clause load_table


## [DataSourceQuery](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L162-L647)

*Bases*: zillion.report.ExecutionStateMixin, tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQuery
    :docstring:
    :members: add_metric covers_field covers_metric execute get_conn get_datasource get_datasource_name get_dialect_name get_tables kill


## [DataSourceQueryResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L688-L704)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQueryResult
    :docstring:
    


## [DataSourceQuerySummary](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L650-L685)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQuerySummary
    :docstring:
    :members: format


## [ExecutionStateMixin](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L50-L159)

::: zillion.report.ExecutionStateMixin
    :docstring:
    


## [Report](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L1448-L2269)

*Bases*: zillion.report.ExecutionStateMixin

::: zillion.report.Report
    :docstring:
    :members: delete execute from_params from_text get_dimension_grain get_grain get_json get_params kill load load_warehouse_id_for_report save


## [ReportResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L2272-L2358)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.ReportResult
    :docstring:
    


## [SQLiteMemoryCombinedResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L836-L1445)

*Bases*: zillion.report.BaseCombinedResult

::: zillion.report.SQLiteMemoryCombinedResult
    :docstring:
    :members: add_warning clean_up create_table get_conn get_cursor get_final_result get_metric_clause ifnull_clause load_table

