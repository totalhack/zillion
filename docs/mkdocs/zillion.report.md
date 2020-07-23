[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.report


## [BaseCombinedResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L605-L701)

::: zillion.report.BaseCombinedResult
    :docstring:
    :members: clean_up create_table get_conn get_cursor get_final_result load_table


## [DataSourceQuery](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L166-L545)

*Bases*: zillion.report.ExecutionStateMixin, tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQuery
    :docstring:
    :members: add_metric covers_field covers_metric execute get_conn get_datasource get_datasource_name get_dialect_name kill


## [DataSourceQueryResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L586-L602)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQueryResult
    :docstring:
    


## [DataSourceQuerySummary](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L548-L583)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQuerySummary
    :docstring:
    :members: format


## [ExecutionStateMixin](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L54-L163)

::: zillion.report.ExecutionStateMixin
    :docstring:
    


## [Report](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L1096-L1756)

*Bases*: zillion.report.ExecutionStateMixin

::: zillion.report.Report
    :docstring:
    :members: delete execute from_params get_dimension_grain get_grain get_json get_params kill load save


## [ReportResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L1759-L1827)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.ReportResult
    :docstring:
    


## [SQLiteMemoryCombinedResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L704-L1093)

*Bases*: zillion.report.BaseCombinedResult

::: zillion.report.SQLiteMemoryCombinedResult
    :docstring:
    :members: clean_up create_table get_conn get_cursor get_final_result load_table

