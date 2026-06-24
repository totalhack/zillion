from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json as std_json
import logging
import os
import re
from typing import TYPE_CHECKING, Any, Callable

import pandas as pd

try:
    from agents import Agent, Runner, function_tool

    agent_sdk_installed = True
except ImportError:
    Agent = None
    Runner = None
    function_tool = None
    agent_sdk_installed = False

from tlbx import raiseif, raiseifnot

from zillion.core import info, zillion_config
from zillion.report import Report


if TYPE_CHECKING:
    from zillion.warehouse import Warehouse

for logger_name in ("openai._base_client", "httpx"):
    sdk_logger = logging.getLogger(logger_name)
    if sdk_logger.level == logging.NOTSET or sdk_logger.level < logging.WARNING:
        sdk_logger.setLevel(logging.WARNING)

DEFAULT_AGENT_MODEL = "gpt-5.4"
DEFAULT_MAX_REPORTS = 4
DEFAULT_COMBINE_STRATEGY = "auto"
DEFAULT_PLANNER_INSTRUCTIONS = """
You are planning analytics work over a zillion warehouse.
Use the tools to inspect the available metrics and dimensions, then produce JSON only.

Zillion usage notes:
- Each report_spec must be a valid Warehouse.execute() parameter dict.
- Warehouse.execute reference:
    Warehouse.execute(
            metrics=None,
            dimensions=None,
            criteria=None,
            row_filters=None,
            rollup=None,
            pivot=None,
            order_by=None,
            limit=None,
            limit_first=False,
    )
    - metrics: list of metric names to return. Treat this as required for normal analytic questions.
    - dimensions: list of dimension names used for grouping. If the question asks "by <dimension>", include it here.
    - criteria: pre-aggregation warehouse filters such as date windows, partner filters, or other dimensional constraints.
    - row_filters: post-aggregation filters on the returned result.
    - rollup: optional totals or subtotal behavior.
    - pivot: optional dimension list to pivot into columns.
    - order_by: optional list of [field, asc|desc] pairs for presentation ordering.
    - limit and limit_first: optional row limiting controls.
- Prefer one report when the warehouse can answer the question directly with multiple metrics and dimensions.
- Use multiple reports only when one report cannot express the answer, then merge on shared dimensions.
- If the question says "by <dimension>", include that field in dimensions.
- If the question names exact metrics or dimensions, include all of them unless you mark requires_confirmation=true and explain why.
- If the question asks for a relative time window such as last 7 days, include explicit temporal criteria in the report_spec.
- For relative dates, compute concrete literal values using the current date/time context provided below. Do not use SQL expressions or placeholders like CURRENT_DATE, NOW(), TODAY, INTERVAL, or DATEADD.
- Prefer criteria for warehouse-level filtering and row_filters only for post-aggregation filtering.
- Add order_by only when it helps produce the requested presentation.
- Set wrangling_steps to [] unless a small post-combination dataframe transformation is absolutely necessary.
- Never put natural-language text in wrangling_steps. Each wrangling step must be a JSON object.

Example report_spec formats:
- Single report for a grouped time series:
    {
        "metrics": ["revenue", "sales"],
        "dimensions": ["date"],
        "criteria": [["date", ">=", "2024-01-01"], ["date", "<=", "2024-01-07"]],
        "order_by": [["date", "asc"]]
    }
- Single report for a grouped dimension breakdown:
    {
        "metrics": ["revenue"],
        "dimensions": ["partner_name"],
        "criteria": [["date", ">=", "2024-01-01"]],
        "order_by": [["revenue", "desc"]],
        "limit": 10
    }
- Multiple reports only when one report cannot express the answer:
    {
        "report_specs": [
            {"metrics": ["revenue"], "dimensions": ["date"]},
            {"metrics": ["sales"], "dimensions": ["date"]}
        ],
        "combine_strategy": "merge",
        "combine_on": ["date"]
    }

Your JSON must have:
- report_specs: list of one or more zillion execute() parameter dicts
- rationale: short explanation
- steps: ordered list of execution steps
- ambiguities: list of open questions
- warnings: list of caveats
- requires_confirmation: boolean
- combine_strategy: one of auto, merge, join_on_index, concat_rows, separate
- combine_on: list of field names when merge is needed
- wrangling_steps: optional list of explicit dataframe operations
    - Use [] by default.
    - If present, each item must be an object like {"operation": "sort_values", "by": ["date"], "ascending": true}.
    - Valid operations are only: select_columns, rename_columns, sort_values, filter_rows, fillna.
    - Do not emit explanatory strings such as "sort by date".

Rules:
- Prefer exact warehouse field names discovered via tools.
- Use validate_report_spec before finalizing each report.
- Set requires_confirmation=true when the question is ambiguous enough that executing may be misleading.
- Keep wrangling_steps conservative and explicit, and prefer [] when the answer can be expressed without extra dataframe post-processing.
""".strip()
SUPPORTED_COMBINE_STRATEGIES = {
    "auto",
    "merge",
    "join_on_index",
    "concat_rows",
    "separate",
}
SUPPORTED_WRANGLING_OPERATIONS = {
    "select_columns",
    "rename_columns",
    "sort_values",
    "filter_rows",
    "fillna",
}
SUPPORTED_FILTER_OPERATORS = {
    "=",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "in",
    "not in",
}
AGENT_SUMMARY_PREVIEW_ROWS = 200


@dataclass
class QuestionPlan:
    question: str
    model: str
    report_specs: list[dict[str, Any]]
    rationale: str = ""
    steps: list[str] = field(default_factory=list)
    ambiguities: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    requires_confirmation: bool = False
    combine_strategy: str = DEFAULT_COMBINE_STRATEGY
    combine_on: list[str] = field(default_factory=list)
    wrangling_steps: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ExecutedQuestionReport:
    index: int
    spec: dict[str, Any]
    result: Any


@dataclass
class QuestionAnswerResult:
    data: Any
    summary: str
    plan: QuestionPlan
    executed_reports: list[ExecutedQuestionReport] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AgentProgressEvent:
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)


ProgressCallback = Callable[[AgentProgressEvent], None]


def _emit_progress(
    progress_callback: ProgressCallback | None,
    event_type: str,
    **payload,
):
    if progress_callback is None:
        return
    progress_callback(AgentProgressEvent(event_type=event_type, payload=payload))


def plan_warehouse_question(
    warehouse: "Warehouse",
    question: str,
    adhoc_datasources=None,
    allow_partial: bool = False,
    model: str | None = None,
    progress_callback: ProgressCallback | None = None,
):
    """Build an execution plan for a natural language question."""
    model = model or zillion_config.get("OPENAI_MODEL") or DEFAULT_AGENT_MODEL
    _emit_progress(
        progress_callback, "planning_started", question=question, model=model
    )
    raw_plan = _run_planning_agent(
        warehouse,
        question,
        adhoc_datasources=adhoc_datasources,
        allow_partial=allow_partial,
        model=model,
        progress_callback=progress_callback,
    )
    plan = _normalize_question_plan(
        warehouse,
        question,
        raw_plan,
        adhoc_datasources=adhoc_datasources,
        allow_partial=allow_partial,
        model=model,
    )
    _emit_progress(
        progress_callback,
        "plan_completed",
        report_count=len(plan.report_specs),
        requires_confirmation=plan.requires_confirmation,
        warnings=list(plan.warnings),
        report_specs=plan.report_specs,
        combine_strategy=plan.combine_strategy,
        combine_on=list(plan.combine_on),
        wrangling_steps=list(plan.wrangling_steps),
        rationale=plan.rationale,
        ambiguities=list(plan.ambiguities),
    )
    return plan


def answer_warehouse_question(
    warehouse: "Warehouse",
    question: str,
    adhoc_datasources=None,
    allow_partial: bool = False,
    model: str | None = None,
    require_confirmation: bool = False,
    progress_callback: ProgressCallback | None = None,
):
    """Plan, execute, combine, and summarize one or more reports."""
    plan = plan_warehouse_question(
        warehouse,
        question,
        adhoc_datasources=adhoc_datasources,
        allow_partial=allow_partial,
        model=model,
        progress_callback=progress_callback,
    )
    if require_confirmation and plan.requires_confirmation:
        result = QuestionAnswerResult(
            data=None,
            summary="Execution deferred pending confirmation of the proposed plan.",
            plan=plan,
            warnings=list(plan.warnings),
        )
        _emit_progress(
            progress_callback,
            "answer_completed",
            deferred=True,
            warnings=list(result.warnings),
        )
        return result
    return execute_question_plan(
        warehouse,
        plan,
        adhoc_datasources=adhoc_datasources,
        allow_partial=allow_partial,
        model=model,
        progress_callback=progress_callback,
    )


def execute_question_plan(
    warehouse: "Warehouse",
    plan: QuestionPlan,
    adhoc_datasources=None,
    allow_partial: bool = False,
    model: str | None = None,
    progress_callback: ProgressCallback | None = None,
):
    """Execute a prepared question plan and return combined data plus summary."""
    executed_reports = []
    warnings = list(plan.warnings)
    for index, spec in enumerate(plan.report_specs):
        _emit_progress(
            progress_callback,
            "report_execution_started",
            report_index=index,
            spec=spec,
        )
        spec_to_run = _validate_report_spec(
            warehouse,
            spec,
            adhoc_datasources=adhoc_datasources,
            allow_partial=allow_partial,
        )
        spec_to_run.setdefault("allow_partial", allow_partial)
        result = warehouse.execute(
            adhoc_datasources=adhoc_datasources,
            **spec_to_run,
        )
        executed_reports.append(
            ExecutedQuestionReport(index=index, spec=spec_to_run, result=result)
        )
        _emit_progress(
            progress_callback,
            "report_execution_finished",
            report_index=index,
            rowcount=result.rowcount,
            columns=list(result.df.columns),
        )

    _emit_progress(
        progress_callback,
        "data_combination_started",
        combine_strategy=plan.combine_strategy,
        report_count=len(executed_reports),
    )
    combined_data = combine_report_data(
        executed_reports,
        combine_strategy=plan.combine_strategy,
        combine_on=plan.combine_on,
    )
    combined_data = apply_wrangling_plan(combined_data, plan.wrangling_steps)
    _emit_progress(
        progress_callback,
        "summary_started",
        report_count=len(executed_reports),
        combine_strategy=plan.combine_strategy,
    )
    summary = _run_summary_agent(
        question=plan.question,
        plan=plan,
        combined_data=combined_data,
        executed_reports=executed_reports,
        model=model or plan.model,
    )
    result = QuestionAnswerResult(
        data=combined_data,
        summary=summary,
        plan=plan,
        executed_reports=executed_reports,
        warnings=warnings,
    )
    _emit_progress(
        progress_callback,
        "answer_completed",
        deferred=False,
        warnings=list(result.warnings),
    )
    return result


def combine_report_data(
    executed_reports: list[ExecutedQuestionReport],
    combine_strategy: str = DEFAULT_COMBINE_STRATEGY,
    combine_on: list[str] | None = None,
):
    """Combine multiple report dataframes into a single data payload."""
    combine_on = combine_on or []
    frames = [report.result.df.copy() for report in executed_reports]
    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return frames[0]

    if combine_strategy not in SUPPORTED_COMBINE_STRATEGIES:
        raise ValueError(f"Unsupported combine strategy: {combine_strategy}")

    if combine_strategy == "separate":
        return {
            f"report_{report.index}": report.result.df.copy()
            for report in executed_reports
        }

    if combine_strategy == "concat_rows":
        labeled_frames = []
        for report in executed_reports:
            frame = report.result.df.reset_index()
            frame.insert(0, "report_index", report.index)
            labeled_frames.append(frame)
        return pd.concat(labeled_frames, ignore_index=True, sort=False)

    if combine_strategy == "join_on_index":
        return _join_frames_on_index(frames)

    if combine_strategy == "merge":
        return _merge_frames(frames, combine_on=combine_on)

    auto_keys = combine_on or _infer_merge_keys(frames)
    if auto_keys:
        return _merge_frames(frames, combine_on=auto_keys)
    if _can_join_on_index(frames):
        return _join_frames_on_index(frames)
    return combine_report_data(executed_reports, combine_strategy="concat_rows")


def apply_wrangling_plan(data, wrangling_steps: list[dict[str, Any]] | None):
    """Apply a small, explicit wrangling plan to a combined dataframe payload."""
    wrangling_steps = wrangling_steps or []
    if not wrangling_steps or not isinstance(data, pd.DataFrame):
        return data

    frame = data.copy()
    for step in wrangling_steps:
        operation = step.get("operation")
        if operation not in SUPPORTED_WRANGLING_OPERATIONS:
            raise ValueError(f"Unsupported wrangling operation: {operation}")

        if operation == "select_columns":
            columns = step.get("columns") or []
            frame = frame.loc[:, columns]
        elif operation == "rename_columns":
            frame = frame.rename(columns=step.get("mapping") or {})
        elif operation == "sort_values":
            by = step.get("by") or []
            ascending = step.get("ascending", True)
            frame = frame.sort_values(by=by, ascending=ascending)
        elif operation == "fillna":
            frame = frame.fillna(step.get("value"))
        elif operation == "filter_rows":
            frame = _apply_filter_rows(frame, step.get("filters") or [])
    return frame


def _apply_filter_rows(frame, filters):
    for field_name, operator, value in filters:
        if operator not in SUPPORTED_FILTER_OPERATORS:
            raise ValueError(f"Unsupported filter operator: {operator}")
        series = frame[field_name]
        if operator == "=":
            frame = frame.loc[series == value]
        elif operator == "!=":
            frame = frame.loc[series != value]
        elif operator == ">":
            frame = frame.loc[series > value]
        elif operator == ">=":
            frame = frame.loc[series >= value]
        elif operator == "<":
            frame = frame.loc[series < value]
        elif operator == "<=":
            frame = frame.loc[series <= value]
        elif operator == "in":
            frame = frame.loc[series.isin(value)]
        elif operator == "not in":
            frame = frame.loc[~series.isin(value)]
    return frame


def _can_join_on_index(frames):
    index_names = [tuple(frame.index.names) for frame in frames]
    return len(set(index_names)) == 1 and all(
        any(name is not None for name in names) for names in index_names
    )


def _join_frames_on_index(frames):
    renamed_frames = []
    seen_columns = set()
    for index, frame in enumerate(frames):
        renamed = frame.copy()
        overlap = seen_columns & set(renamed.columns)
        if overlap:
            renamed = renamed.rename(
                columns={column: f"report_{index}_{column}" for column in overlap}
            )
        seen_columns.update(renamed.columns)
        renamed_frames.append(renamed)
    result = renamed_frames[0]
    for frame in renamed_frames[1:]:
        result = result.join(frame, how="outer")
    return result


def _infer_merge_keys(frames):
    shared = None
    for frame in frames:
        columns = set(frame.reset_index().columns)
        shared = columns if shared is None else shared & columns
    return sorted(shared or [])


def _merge_frames(frames, combine_on):
    raiseifnot(combine_on, "combine_on is required for merge strategy")
    merged = frames[0].reset_index()
    for index, frame in enumerate(frames[1:], start=1):
        other = frame.reset_index()
        overlap = (set(merged.columns) & set(other.columns)) - set(combine_on)
        if overlap:
            other = other.rename(
                columns={column: f"report_{index}_{column}" for column in overlap}
            )
        merged = merged.merge(other, how="outer", on=combine_on)
    return merged


def _normalize_question_text(text: str):
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def _field_reference_pattern(field_name: str):
    tokens = [re.escape(token) for token in field_name.lower().split("_") if token]
    if not tokens:
        return None
    joined_tokens = r"[\s_]+".join(tokens)
    return rf"\b{joined_tokens}\b"


def _question_mentions_field(question: str, field_name: str):
    pattern = _field_reference_pattern(field_name)
    if not pattern:
        return False
    return bool(re.search(pattern, _normalize_question_text(question)))


def _extract_question_metric_references(
    warehouse: "Warehouse", question: str, adhoc_datasources=None
):
    question_text = _normalize_question_text(question)
    metrics = _get_agent_accessible_fields(
        warehouse,
        adhoc_datasources=adhoc_datasources,
        field_type="metric",
    )
    matches = []
    claimed_spans: list[tuple[int, int]] = []
    for name in sorted(metrics, key=len, reverse=True):
        pattern = _field_reference_pattern(name)
        if not pattern:
            continue
        for match in re.finditer(pattern, question_text):
            span = match.span()
            if any(start <= span[0] and span[1] <= end for start, end in claimed_spans):
                continue
            matches.append(name)
            claimed_spans.append(span)
            break
    return sorted(matches)


def _extract_question_group_by_dimensions(
    warehouse: "Warehouse", question: str, adhoc_datasources=None
):
    question_text = _normalize_question_text(question)
    dimensions = _get_agent_accessible_fields(
        warehouse,
        adhoc_datasources=adhoc_datasources,
        field_type="dimension",
    )
    matches = []
    for name in dimensions:
        pattern = _field_reference_pattern(name)
        if pattern and re.search(rf"\bby\s+{pattern}", question_text):
            matches.append(name)
    return sorted(matches)


def _question_requests_relative_time(question: str):
    question_text = _normalize_question_text(question)
    return bool(
        re.search(
            r"\b(last|past|previous)\s+\d+\s+"
            r"(day|days|week|weeks|month|months|year|years)\b",
            question_text,
        )
    )


def _is_temporal_field(field_name: str, field_def):
    type_text = str(
        getattr(field_def, "type", None) or getattr(field_def, "sa_type", None) or ""
    ).lower()
    name_text = (field_name or "").lower()
    return any(
        token in name_text for token in ("date", "time", "day", "week", "month", "year")
    ) or any(token in type_text for token in ("date", "time"))


def _iter_report_spec_field_names(spec):
    for field_name in spec.get("dimensions") or []:
        yield field_name
    for field_name, *_ in spec.get("criteria") or []:
        yield field_name
    for field_name, *_ in spec.get("row_filters") or []:
        yield field_name
    for field_name, *_ in spec.get("order_by") or []:
        yield field_name
    pivot = spec.get("pivot")
    if isinstance(pivot, str) and pivot:
        yield pivot


def _validate_question_plan_coverage(
    warehouse: "Warehouse",
    question: str,
    report_specs: list[dict[str, Any]],
    adhoc_datasources=None,
):
    if not hasattr(warehouse, "get_metrics") or not hasattr(
        warehouse, "get_dimensions"
    ):
        return

    metrics_in_plan = {
        metric for spec in report_specs for metric in (spec.get("metrics") or [])
    }
    group_by_dimensions_in_plan = {
        dimension
        for spec in report_specs
        for dimension in (spec.get("dimensions") or [])
    }
    criteria_fields_in_plan = {
        field_name
        for spec in report_specs
        for field_name, *_ in (spec.get("criteria") or [])
    }

    requested_metrics = _extract_question_metric_references(
        warehouse,
        question,
        adhoc_datasources=adhoc_datasources,
    )
    requested_group_by_dimensions = _extract_question_group_by_dimensions(
        warehouse,
        question,
        adhoc_datasources=adhoc_datasources,
    )

    missing_reasons = []
    missing_metrics = sorted(set(requested_metrics) - metrics_in_plan)
    if missing_metrics:
        missing_reasons.append(f"missing requested metrics: {missing_metrics}")

    missing_group_by_dimensions = sorted(
        set(requested_group_by_dimensions) - group_by_dimensions_in_plan
    )
    if missing_group_by_dimensions:
        missing_reasons.append(
            f"missing requested group-by dimensions: {missing_group_by_dimensions}"
        )

    if _question_requests_relative_time(question):
        temporal_dimensions = {
            name
            for name, field_def in _get_agent_accessible_fields(
                warehouse,
                adhoc_datasources=adhoc_datasources,
                field_type="dimension",
            ).items()
            if _is_temporal_field(name, field_def)
        }
        if temporal_dimensions and not (criteria_fields_in_plan & temporal_dimensions):
            missing_reasons.append(
                "missing temporal criteria for the requested relative time window"
            )

    raiseif(
        missing_reasons,
        "Question plan does not cover the request: " + "; ".join(missing_reasons),
        exc=ValueError,
    )


def _normalize_question_plan(
    warehouse: "Warehouse",
    question: str,
    raw_plan,
    adhoc_datasources=None,
    allow_partial: bool = False,
    model: str | None = None,
):
    if isinstance(raw_plan, str):
        raw_plan = _parse_json_output(raw_plan)

    raiseifnot(isinstance(raw_plan, dict), "Question plan must be a dict")
    raw_report_specs = raw_plan.get("report_specs") or []
    raiseifnot(raw_report_specs, "Question plan did not include report_specs")
    raiseif(
        len(raw_report_specs)
        > zillion_config.get("AGENT_MAX_REPORTS", DEFAULT_MAX_REPORTS),
        "Question plan requested too many reports",
    )

    report_specs = [
        _validate_report_spec(
            warehouse,
            spec,
            adhoc_datasources=adhoc_datasources,
            allow_partial=allow_partial,
        )
        for spec in raw_report_specs
    ]
    combine_strategy = raw_plan.get("combine_strategy") or DEFAULT_COMBINE_STRATEGY
    raiseif(
        combine_strategy not in SUPPORTED_COMBINE_STRATEGIES,
        f"Unsupported combine strategy: {combine_strategy}",
    )
    wrangling_steps = raw_plan.get("wrangling_steps") or []
    raiseifnot(
        isinstance(wrangling_steps, list),
        "wrangling_steps must be a list",
        exc=ValueError,
    )
    for step in wrangling_steps:
        raiseifnot(
            isinstance(step, dict),
            f"Each wrangling step must be a dict, got {type(step).__name__}",
            exc=ValueError,
        )
        operation = step.get("operation")
        raiseif(
            operation not in SUPPORTED_WRANGLING_OPERATIONS,
            f"Unsupported wrangling operation: {operation}",
        )
    _validate_question_plan_coverage(
        warehouse,
        question,
        report_specs,
        adhoc_datasources=adhoc_datasources,
    )

    return QuestionPlan(
        question=question,
        model=model or zillion_config.get("OPENAI_MODEL") or DEFAULT_AGENT_MODEL,
        report_specs=report_specs,
        rationale=raw_plan.get("rationale") or "",
        steps=raw_plan.get("steps") or [],
        ambiguities=raw_plan.get("ambiguities") or [],
        warnings=raw_plan.get("warnings") or [],
        requires_confirmation=bool(raw_plan.get("requires_confirmation")),
        combine_strategy=combine_strategy,
        combine_on=raw_plan.get("combine_on") or [],
        wrangling_steps=wrangling_steps,
    )


def _validate_report_spec(
    warehouse: "Warehouse",
    spec,
    adhoc_datasources=None,
    allow_partial: bool = False,
):
    raiseifnot(isinstance(spec, dict), "Each report spec must be a dict")
    valid_keys = {
        "metrics",
        "dimensions",
        "criteria",
        "row_filters",
        "rollup",
        "pivot",
        "order_by",
        "limit",
        "limit_first",
        "allow_partial",
        "disabled_tables",
    }
    unexpected = set(spec.keys()) - valid_keys
    raiseif(unexpected, f"Unexpected report spec keys: {sorted(unexpected)}")

    normalized = dict(spec)
    for metric in normalized.get("metrics") or []:
        metric_def = warehouse.get_metric(metric, adhoc_fms=adhoc_datasources)
        _raise_if_agent_field_disabled(warehouse, metric, metric_def)
    for dimension in normalized.get("dimensions") or []:
        dimension_def = warehouse.get_dimension(dimension, adhoc_fms=adhoc_datasources)
        _raise_if_agent_field_disabled(warehouse, dimension, dimension_def)
    for field_name, _, _ in normalized.get("criteria") or []:
        dimension_def = warehouse.get_dimension(field_name, adhoc_fms=adhoc_datasources)
        _raise_if_agent_field_disabled(warehouse, field_name, dimension_def)
    _raise_if_report_spec_uses_sql_date_expressions(normalized)
    for field_name, _, _ in normalized.get("row_filters") or []:
        field_def = warehouse.get_field(field_name, adhoc_fms=adhoc_datasources)
        _raise_if_agent_field_disabled(warehouse, field_name, field_def)
    for field_name, _ in normalized.get("order_by") or []:
        field_def = warehouse.get_field(field_name, adhoc_fms=adhoc_datasources)
        _raise_if_agent_field_disabled(warehouse, field_name, field_def)

    normalized.setdefault("allow_partial", allow_partial)
    Report(
        warehouse,
        adhoc_datasources=adhoc_datasources,
        **normalized,
    )
    return normalized


def _value_uses_sql_date_expression(value):
    if isinstance(value, str):
        return bool(
            re.search(
                r"\b(CURRENT_DATE|CURRENT_TIMESTAMP|NOW\s*\(|TODAY|INTERVAL|DATEADD|DATEDIFF)\b",
                value,
                flags=re.IGNORECASE,
            )
        )
    if isinstance(value, (list, tuple)):
        return any(_value_uses_sql_date_expression(item) for item in value)
    return False


def _raise_if_report_spec_uses_sql_date_expressions(spec):
    invalid_criteria = []
    for criterion in spec.get("criteria") or []:
        if len(criterion) >= 3 and _value_uses_sql_date_expression(criterion[2]):
            invalid_criteria.append(criterion)

    raiseif(
        invalid_criteria,
        "Criteria must use concrete literal date/datetime values, not SQL date expressions: "
        + std_json.dumps(invalid_criteria, default=str),
        exc=ValueError,
    )


def _run_planning_agent(
    warehouse: "Warehouse",
    question: str,
    adhoc_datasources=None,
    allow_partial: bool = False,
    model: str | None = None,
    progress_callback: ProgressCallback | None = None,
):
    _ensure_agent_sdk()
    _ensure_openai_api_key()
    model = model or zillion_config.get("OPENAI_MODEL") or DEFAULT_AGENT_MODEL

    @function_tool
    def get_warehouse_overview():
        overview = {
            "metrics": _serialize_fields(
                _get_agent_accessible_fields(
                    warehouse,
                    adhoc_datasources=adhoc_datasources,
                    field_type="metric",
                )
            ),
            "dimensions": _serialize_fields(
                _get_agent_accessible_fields(
                    warehouse,
                    adhoc_datasources=adhoc_datasources,
                    field_type="dimension",
                )
            ),
        }
        _emit_progress(
            progress_callback,
            "warehouse_overview_loaded",
            metric_count=len(overview["metrics"]),
            dimension_count=len(overview["dimensions"]),
        )
        return overview

    @function_tool
    def find_fields(search: str = "", field_type: str = "all"):
        search = (search or "").lower()
        fields = _get_agent_accessible_fields(
            warehouse,
            adhoc_datasources=adhoc_datasources,
            field_type=field_type,
        )
        filtered = {}
        for name, field_def in fields.items():
            haystack = " ".join(
                [
                    name,
                    getattr(field_def, "display_name", "") or "",
                    std_json.dumps(getattr(field_def, "meta", {}) or {}),
                ]
            ).lower()
            if not search or search in haystack:
                filtered[name] = field_def
        serialized = _serialize_fields(filtered)
        _emit_progress(
            progress_callback,
            "field_search_performed",
            search=search,
            field_type=field_type,
            match_count=len(serialized),
        )
        return serialized

    def validate_report_spec(spec: dict):
        try:
            normalized = _validate_report_spec(
                warehouse,
                spec,
                adhoc_datasources=adhoc_datasources,
                allow_partial=allow_partial,
            )
            _emit_progress(
                progress_callback,
                "report_spec_validated",
                ok=True,
                metrics=normalized.get("metrics") or [],
                dimensions=normalized.get("dimensions") or [],
                criteria=normalized.get("criteria") or [],
                row_filters=normalized.get("row_filters") or [],
                order_by=normalized.get("order_by") or [],
                limit=normalized.get("limit"),
            )
            return {"ok": True, "normalized": normalized}
        except Exception as exc:  # noqa: BLE001
            _emit_progress(
                progress_callback,
                "report_spec_validated",
                ok=False,
                error=str(exc),
            )
            return {"ok": False, "error": str(exc)}

    try:
        validate_report_spec = function_tool(strict_mode=False)(validate_report_spec)
    except TypeError:
        validate_report_spec = function_tool(validate_report_spec)

    instructions = _get_planner_runtime_instructions()

    agent = Agent(
        name="ZillionQuestionPlanner",
        instructions=instructions,
        model=model,
        tools=[get_warehouse_overview, find_fields, validate_report_spec],
    )
    result = Runner.run_sync(agent, f"Question: {question}")
    return result.final_output


def _run_summary_agent(question, plan, combined_data, executed_reports, model=None):
    model = model or zillion_config.get("OPENAI_MODEL") or DEFAULT_AGENT_MODEL
    if not agent_sdk_installed:
        return _build_fallback_summary(question, combined_data, executed_reports)

    _ensure_openai_api_key()
    agent = Agent(
        name="ZillionQuestionSummarizer",
        model=model,
        instructions=(
            "Summarize executed analytics results for the user. "
            "Use only the supplied data and plan, and make sure your summary is relevant to the user's question. "
            "Optionally highlight anything notable in the data as it pertains to the question. "
            "Optionally mention important caveats briefly if any exist."
        ),
    )
    payload = {
        "question": question,
        "plan": {
            "report_specs": plan.report_specs,
            "combine_strategy": plan.combine_strategy,
            "combine_on": plan.combine_on,
            "wrangling_steps": plan.wrangling_steps,
            "warnings": plan.warnings,
        },
        "data_preview": _serialize_combined_data(combined_data),
        "executed_reports": [
            {
                "index": report.index,
                "spec": report.spec,
                "rowcount": report.result.rowcount,
                "columns": list(report.result.df.columns),
            }
            for report in executed_reports
        ],
    }
    result = Runner.run_sync(agent, std_json.dumps(payload, default=str))
    return result.final_output


def _build_fallback_summary(question, combined_data, executed_reports):
    if isinstance(combined_data, pd.DataFrame):
        shape = f"{combined_data.shape[0]} rows x {combined_data.shape[1]} columns"
    elif isinstance(combined_data, dict):
        shape = f"{len(combined_data)} separate report datasets"
    else:
        shape = "no tabular output"
    return (
        f"Answered question '{question}' using {len(executed_reports)} report(s). "
        f"Combined output shape: {shape}."
    )


def _get_agent_accessible_fields(
    warehouse: "Warehouse", adhoc_datasources=None, field_type: str = "all"
):
    if field_type == "metric":
        fields = warehouse.get_metrics(adhoc_fms=adhoc_datasources)
    elif field_type == "dimension":
        fields = warehouse.get_dimensions(adhoc_fms=adhoc_datasources)
    else:
        fields = warehouse.get_fields(adhoc_fms=adhoc_datasources)

    return {
        name: field_def
        for name, field_def in fields.items()
        if _agent_field_enabled(warehouse, name, field_def)
    }


def _agent_field_enabled(warehouse: "Warehouse", field_name: str, field_def):
    warehouse_agent_meta = (warehouse.meta or {}).get("agent", None) or {}
    field_meta = getattr(field_def, "meta", None) or {}
    field_agent_meta = field_meta.get("agent", None) or {}

    if field_agent_meta.get("enabled", True) is False:
        return False

    field_group = field_meta.get("group")
    if field_group in (warehouse_agent_meta.get("field_disabled_groups") or []):
        return False

    for pattern in warehouse_agent_meta.get("field_disabled_patterns") or []:
        if re.search(pattern, field_name):
            return False

    return True


def _raise_if_agent_field_disabled(warehouse: "Warehouse", field_name: str, field_def):
    raiseif(
        not _agent_field_enabled(warehouse, field_name, field_def),
        f"Field '{field_name}' is not accessible to the agent",
    )


def _get_planner_instructions():
    base_prompt_file = zillion_config.get("AGENT_BASE_PROMPT_FILE")
    custom_rules_file = zillion_config.get("AGENT_CUSTOM_RULES_FILE")

    instructions = (
        _read_prompt_file(base_prompt_file)
        if base_prompt_file
        else DEFAULT_PLANNER_INSTRUCTIONS
    )
    custom_rules = _read_prompt_file(custom_rules_file) if custom_rules_file else ""
    if custom_rules:
        instructions = f"{instructions.rstrip()}\n\nAdditional rules:\n{custom_rules}"
    return instructions.strip()


def _get_planner_runtime_instructions(now: datetime | None = None):
    now = now or datetime.now(timezone.utc).astimezone()
    current_date = now.date().isoformat()
    current_timestamp = now.isoformat(timespec="seconds")
    return (
        f"{_get_planner_instructions()}\n\n"
        "Current date/time context for relative-date questions:\n"
        f"- Current local timestamp: {current_timestamp}\n"
        f"- Current local date: {current_date}\n"
        "- When the user asks for relative dates such as last 7 days, convert them to concrete literal values in criteria.\n"
        "- Never emit SQL expressions or placeholders such as CURRENT_DATE, NOW(), TODAY, INTERVAL, DATEADD, or DATEDIFF."
    )


def _read_prompt_file(path):
    raiseifnot(path, "Prompt file path must be specified")
    raiseif(not os.path.isfile(path), f"Prompt file not found: {path}")
    with open(path, "r") as f:
        content = f.read().strip()
    raiseifnot(content, f"Prompt file is empty: {path}")
    return content


def _serialize_fields(fields):
    serialized = []
    for name, field_def in fields.items():
        serialized.append(
            {
                "name": name,
                "display_name": getattr(field_def, "display_name", None),
                "field_type": getattr(field_def, "field_type", None),
                "type": str(
                    getattr(field_def, "type", None)
                    or getattr(field_def, "sa_type", None)
                    or "unknown"
                ),
                "aggregation": getattr(field_def, "aggregation", None),
                "meta": getattr(field_def, "meta", None),
            }
        )
    return serialized


def _serialize_combined_data(data, preview_rows: int = AGENT_SUMMARY_PREVIEW_ROWS):
    if isinstance(data, pd.DataFrame):
        preview = data.reset_index().head(preview_rows)
        return {
            "kind": "dataframe",
            "columns": list(preview.columns),
            "rows": preview.to_dict(orient="records"),
        }
    if isinstance(data, dict):
        return {
            key: _serialize_combined_data(value, preview_rows=preview_rows)
            for key, value in data.items()
        }
    return data


def _parse_json_output(output):
    output = output.strip()
    if output.startswith("```"):
        output = output.strip("`")
        if output.startswith("json"):
            output = output[4:]
        output = output.strip()
    try:
        return std_json.loads(output)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Unable to parse agent output as JSON: {exc}") from exc


def _ensure_agent_sdk():
    raiseif(
        not agent_sdk_installed,
        "OpenAI Agents SDK is not installed. Install the agent extra to use question planning.",
    )


def _ensure_openai_api_key():
    key = zillion_config.get("OPENAI_API_KEY")
    raiseifnot(key, "Missing OPENAI_API_KEY in zillion config")
    os.environ.setdefault("OPENAI_API_KEY", key)
    info("Using OpenAI Agents SDK for warehouse question planning")
