[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.report


## [BaseCombinedResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L701-L827)

::: zillion.report.BaseCombinedResult
    :docstring:
    :members: add_warning clean_up create_table get_conn get_cursor get_final_result get_metric_clause ifnull_clause load_table


## [DataSourceQuery](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L157-L641)

*Bases*: zillion.report.ExecutionStateMixin, tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQuery
    :docstring:
    :members: add_metric covers_field covers_metric execute get_conn get_datasource get_datasource_name get_dialect_name get_tables kill


## [DataSourceQueryResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L682-L698)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQueryResult
    :docstring:
    


## [DataSourceQuerySummary](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L644-L679)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQuerySummary
    :docstring:
    :members: format


## [ExecutionStateMixin](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L45-L154)

::: zillion.report.ExecutionStateMixin
    :docstring:
    


## [Report](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L1433-L2223)

*Bases*: zillion.report.ExecutionStateMixin

::: zillion.report.Report
    :docstring:
    :members: delete execute from_params get_dimension_grain get_grain get_json get_params kill load load_warehouse_id_for_report save


## [ReportResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L2226-L2312)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.ReportResult
    :docstring:
    


## [SQLiteMemoryCombinedResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L830-L1430)

*Bases*: zillion.report.BaseCombinedResult

::: zillion.report.SQLiteMemoryCombinedResult
    :docstring:
    :members: add_warning clean_up create_table get_conn get_cursor get_final_result get_metric_clause ifnull_clause load_table

