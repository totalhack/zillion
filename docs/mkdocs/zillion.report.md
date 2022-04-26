[//]: # (This is an auto-generated file. Do not edit)
# Module zillion.report


## [BaseCombinedResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L709-L831)

::: zillion.report.BaseCombinedResult
    :docstring:
    :members: add_warning clean_up create_table get_conn get_cursor get_final_result ifnull_clause load_table


## [DataSourceQuery](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L165-L649)

*Bases*: zillion.report.ExecutionStateMixin, tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQuery
    :docstring:
    :members: add_metric covers_field covers_metric execute get_conn get_datasource get_datasource_name get_dialect_name get_tables kill


## [DataSourceQueryResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L690-L706)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQueryResult
    :docstring:
    


## [DataSourceQuerySummary](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L652-L687)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.DataSourceQuerySummary
    :docstring:
    :members: format


## [ExecutionStateMixin](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L53-L162)

::: zillion.report.ExecutionStateMixin
    :docstring:
    


## [Report](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L1400-L2198)

*Bases*: zillion.report.ExecutionStateMixin

::: zillion.report.Report
    :docstring:
    :members: delete execute from_params get_dimension_grain get_grain get_json get_params kill load load_warehouse_id_for_report save


## [ReportResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L2201-L2284)

*Bases*: tlbx.logging_utils.PrintMixin

::: zillion.report.ReportResult
    :docstring:
    


## [SQLiteMemoryCombinedResult](https://github.com/totalhack/zillion/blob/master/zillion/report.py#L834-L1397)

*Bases*: zillion.report.BaseCombinedResult

::: zillion.report.SQLiteMemoryCombinedResult
    :docstring:
    :members: add_warning clean_up create_table get_conn get_cursor get_final_result ifnull_clause load_table

