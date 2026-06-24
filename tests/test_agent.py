import json as std_json
import os
from types import SimpleNamespace

import pytest
import pandas as pd

from .test_utils import *
import zillion.agent as agent_module
from zillion.warehouse import Warehouse
from zillion.agent import (
    AgentProgressEvent,
    DEFAULT_PLANNER_INSTRUCTIONS,
    ExecutedQuestionReport,
    QuestionPlan,
    _extract_question_metric_references,
    _get_agent_accessible_fields,
    _get_planner_instructions,
    answer_warehouse_question,
    plan_warehouse_question,
    execute_question_plan,
)


class FakeWarehouse:
    def __init__(self, frames_by_metric):
        self.frames_by_metric = frames_by_metric

    def execute(self, **spec):
        metric = spec["metrics"][0]
        df = self.frames_by_metric[metric].copy()
        return SimpleNamespace(df=df, rowcount=len(df))


def _print_agent_progress(event):
    print(
        f"[agent:{event.event_type}] "
        f"{std_json.dumps(event.payload, default=str, sort_keys=True)}"
    )


def _collect_plan_metrics(plan):
    return {
        metric for spec in plan.report_specs for metric in (spec.get("metrics") or [])
    }


def _collect_plan_dimensions(plan):
    return {
        dimension
        for spec in plan.report_specs
        for dimension in (spec.get("dimensions") or [])
    }


def _collect_plan_criteria_fields(plan):
    return {
        field_name
        for spec in plan.report_specs
        for field_name, *_ in (spec.get("criteria") or [])
    }


def _require_agent_test_environment():
    if not agent_module.agent_sdk_installed:
        raise RuntimeError("OpenAI Agents SDK is not installed")

    key = agent_module.zillion_config.get("OPENAI_API_KEY") or os.environ.get(
        "OPENAI_API_KEY"
    )
    if not key:
        key = os.environ.get("ZILLION_OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not configured")


def _install_fake_agent_sdk(monkeypatch):
    class FakeAgent:
        def __init__(self, **kwargs):
            self.tools = kwargs["tools"]

    class FakeRunner:
        @staticmethod
        def run_sync(agent, prompt):
            assert "Question:" in prompt
            overview = agent.tools[0]()
            assert overview["metrics"]
            prompt_text = prompt.lower()
            if "revenue and sales by date for the last 7 days" in prompt_text:
                matches = agent.tools[1](search="date", field_type="dimension")
                assert matches
                validated = agent.tools[2](
                    spec={
                        "metrics": ["revenue", "sales"],
                        "dimensions": ["date"],
                        "criteria": [
                            ["date", ">=", "2020-04-24"],
                            ["date", "<=", "2020-04-30"],
                        ],
                    }
                )
                assert validated["ok"] is True
                return SimpleNamespace(
                    final_output={
                        "report_specs": [validated["normalized"]],
                        "rationale": "Fake planning output.",
                        "steps": [
                            "Use one zillion report with both requested metrics grouped by date.",
                            "Apply explicit date criteria for the requested 7 day window.",
                        ],
                        "ambiguities": [],
                        "warnings": [],
                        "requires_confirmation": False,
                        "combine_strategy": "auto",
                        "combine_on": [],
                        "wrangling_steps": [],
                    }
                )

            if "revenue by partner name" in prompt_text:
                matches = agent.tools[1](search="partner name", field_type="dimension")
                assert matches
                validated = agent.tools[2](
                    spec={"metrics": ["revenue"], "dimensions": ["partner_name"]}
                )
                assert validated["ok"] is True
                return SimpleNamespace(
                    final_output={
                        "report_specs": [validated["normalized"]],
                        "rationale": "Fake planning output.",
                        "steps": ["Use one zillion report grouped by partner_name."],
                        "ambiguities": [],
                        "warnings": [],
                        "requires_confirmation": False,
                        "combine_strategy": "auto",
                        "combine_on": [],
                        "wrangling_steps": [],
                    }
                )

            matches = agent.tools[1](search="revenue", field_type="metric")
            assert matches
            validated = agent.tools[2](
                spec={"metrics": ["revenue"], "dimensions": ["date"]}
            )
            assert validated["ok"] is True
            return SimpleNamespace(
                final_output={
                    "report_specs": [validated["normalized"]],
                    "rationale": "Fake planning output.",
                    "steps": ["Use one zillion report grouped by date."],
                    "ambiguities": [],
                    "warnings": [],
                    "requires_confirmation": False,
                    "combine_strategy": "auto",
                    "combine_on": [],
                    "wrangling_steps": [],
                }
            )

    monkeypatch.setattr(agent_module, "agent_sdk_installed", True)
    monkeypatch.setattr(agent_module, "Agent", FakeAgent)
    monkeypatch.setattr(agent_module, "Runner", FakeRunner)
    monkeypatch.setattr(
        agent_module,
        "function_tool",
        lambda func=None, **_: func if func is not None else (lambda fn: fn),
    )
    monkeypatch.setattr(agent_module, "_ensure_openai_api_key", lambda: None)


def test_require_agent_test_environment_raises_when_sdk_missing(monkeypatch):
    monkeypatch.setattr(agent_module, "agent_sdk_installed", False)

    with pytest.raises(RuntimeError, match="OpenAI Agents SDK is not installed"):
        _require_agent_test_environment()


def test_require_agent_test_environment_raises_when_api_key_missing(monkeypatch):
    monkeypatch.setattr(agent_module, "agent_sdk_installed", True)
    monkeypatch.setitem(agent_module.zillion_config, "OPENAI_API_KEY", None)

    with pytest.raises(RuntimeError, match="OPENAI_API_KEY is not configured"):
        _require_agent_test_environment()


def test_execute_question_plan_merges_and_wrangles(monkeypatch):
    frames = {
        "revenue": pd.DataFrame(
            {"revenue": [10.0, 20.0]},
            index=pd.Index(["2024-01-01", "2024-01-02"], name="date"),
        ),
        "sales": pd.DataFrame(
            {"sales": [1, 3]},
            index=pd.Index(["2024-01-01", "2024-01-02"], name="date"),
        ),
    }
    warehouse = FakeWarehouse(frames)
    plan = QuestionPlan(
        question="How did revenue and sales trend by date?",
        model="gpt-5.4",
        report_specs=[
            {"metrics": ["revenue"], "dimensions": ["date"]},
            {"metrics": ["sales"], "dimensions": ["date"]},
        ],
        combine_strategy="merge",
        combine_on=["date"],
        wrangling_steps=[
            {
                "operation": "rename_columns",
                "mapping": {"revenue": "gross_revenue"},
            },
            {
                "operation": "sort_values",
                "by": ["date"],
                "ascending": False,
            },
            {
                "operation": "select_columns",
                "columns": ["date", "gross_revenue", "sales"],
            },
        ],
    )

    monkeypatch.setattr(
        "zillion.agent._run_summary_agent",
        lambda **kwargs: "Combined revenue and sales by date.",
    )
    monkeypatch.setattr(
        "zillion.agent._validate_report_spec",
        lambda warehouse, spec, adhoc_datasources=None, allow_partial=False: dict(spec),
    )

    result = execute_question_plan(warehouse, plan)

    assert result.summary == "Combined revenue and sales by date."
    assert list(result.data.columns) == ["date", "gross_revenue", "sales"]
    assert result.data.iloc[0].to_dict() == {
        "date": "2024-01-02",
        "gross_revenue": 20.0,
        "sales": 3,
    }


def test_execute_question_plan_can_keep_reports_separate(monkeypatch):
    report_a = ExecutedQuestionReport(
        index=0,
        spec={"metrics": ["revenue"]},
        result=SimpleNamespace(df=pd.DataFrame({"revenue": [10.0]}), rowcount=1),
    )
    report_b = ExecutedQuestionReport(
        index=1,
        spec={"metrics": ["sales"]},
        result=SimpleNamespace(df=pd.DataFrame({"sales": [4]}), rowcount=1),
    )

    monkeypatch.setattr(
        "zillion.agent._run_summary_agent",
        lambda **kwargs: "Kept outputs separate.",
    )
    monkeypatch.setattr(
        "zillion.agent._validate_report_spec",
        lambda warehouse, spec, adhoc_datasources=None, allow_partial=False: dict(spec),
    )

    warehouse = FakeWarehouse({})
    plan = QuestionPlan(
        question="Return both reports without merging.",
        model="gpt-5.4",
        report_specs=[report_a.spec, report_b.spec],
        combine_strategy="separate",
    )

    calls = [report_a.result, report_b.result]
    warehouse.execute = lambda **spec: calls.pop(0)
    result = execute_question_plan(warehouse, plan)

    assert result.summary == "Kept outputs separate."
    assert sorted(result.data.keys()) == ["report_0", "report_1"]
    assert list(result.data["report_0"].columns) == ["revenue"]
    assert list(result.data["report_1"].columns) == ["sales"]


def test_answer_question_defers_when_confirmation_required(monkeypatch):
    plan = QuestionPlan(
        question="Should I compare revenue across regions?",
        model="gpt-5.4",
        report_specs=[{"metrics": ["revenue"], "dimensions": ["region"]}],
        requires_confirmation=True,
        ambiguities=["Region could mean sales region or partner region."],
    )

    monkeypatch.setattr(
        "zillion.agent.plan_warehouse_question", lambda *args, **kwargs: plan
    )

    result = answer_warehouse_question(
        warehouse=SimpleNamespace(),
        question=plan.question,
        require_confirmation=True,
    )

    assert result.data is None
    assert "deferred" in result.summary.lower()
    assert result.plan.requires_confirmation is True


def test_answer_question_emits_progress_events(monkeypatch):
    raw_plan = {
        "report_specs": [
            {"metrics": ["revenue"], "dimensions": ["date"]},
            {"metrics": ["sales"], "dimensions": ["date"]},
        ],
        "combine_strategy": "merge",
        "combine_on": ["date"],
    }
    frames = {
        "revenue": pd.DataFrame(
            {"revenue": [10.0]},
            index=pd.Index(["2024-01-01"], name="date"),
        ),
        "sales": pd.DataFrame(
            {"sales": [3]},
            index=pd.Index(["2024-01-01"], name="date"),
        ),
    }
    events = []

    monkeypatch.setattr(
        "zillion.agent._run_planning_agent", lambda *args, **kwargs: raw_plan
    )
    monkeypatch.setattr(
        "zillion.agent._validate_report_spec",
        lambda warehouse, spec, adhoc_datasources=None, allow_partial=False: dict(spec),
    )
    monkeypatch.setattr(
        "zillion.agent._run_summary_agent",
        lambda **kwargs: "Summary complete.",
    )

    result = answer_warehouse_question(
        warehouse=FakeWarehouse(frames),
        question="How did revenue and sales trend by date?",
        progress_callback=events.append,
    )

    assert result.summary == "Summary complete."
    assert [event.event_type for event in events] == [
        "planning_started",
        "plan_completed",
        "report_execution_started",
        "report_execution_finished",
        "report_execution_started",
        "report_execution_finished",
        "data_combination_started",
        "summary_started",
        "answer_completed",
    ]
    assert events[-1].payload == {"deferred": False, "warnings": []}


def test_plan_question_emits_planner_tool_progress(monkeypatch, config):
    events = []

    class FakeAgent:
        def __init__(self, **kwargs):
            self.tools = kwargs["tools"]

    class FakeRunner:
        @staticmethod
        def run_sync(agent, prompt):
            assert "Question:" in prompt
            overview = agent.tools[0]()
            assert overview["metrics"]
            matches = agent.tools[1](search="revenue", field_type="metric")
            assert matches
            validated = agent.tools[2](
                spec={"metrics": ["revenue"], "dimensions": ["date"]}
            )
            assert validated["ok"] is True
            return SimpleNamespace(
                final_output={
                    "report_specs": [validated["normalized"]],
                    "combine_strategy": "auto",
                }
            )

    monkeypatch.setattr(agent_module, "agent_sdk_installed", True)
    monkeypatch.setattr(agent_module, "Agent", FakeAgent)
    monkeypatch.setattr(agent_module, "Runner", FakeRunner)
    monkeypatch.setattr(agent_module, "function_tool", lambda fn: fn)
    monkeypatch.setattr("zillion.agent._ensure_openai_api_key", lambda: None)

    warehouse = Warehouse(config=config)
    plan = plan_warehouse_question(
        warehouse,
        "revenue by date",
        progress_callback=events.append,
    )

    assert plan.report_specs
    assert [event.event_type for event in events] == [
        "planning_started",
        "warehouse_overview_loaded",
        "field_search_performed",
        "report_spec_validated",
        "plan_completed",
    ]
    assert isinstance(events[0], AgentProgressEvent)
    assert events[2].payload["search"] == "revenue"


def test_planner_instructions_default_when_no_files(monkeypatch):
    monkeypatch.setitem(agent_module.zillion_config, "AGENT_BASE_PROMPT_FILE", None)
    monkeypatch.setitem(agent_module.zillion_config, "AGENT_CUSTOM_RULES_FILE", None)

    assert _get_planner_instructions() == DEFAULT_PLANNER_INSTRUCTIONS


def test_planner_instructions_can_be_replaced_and_extended(tmp_path, monkeypatch):
    base_prompt = tmp_path / "base_prompt.txt"
    custom_rules = tmp_path / "custom_rules.txt"
    base_prompt.write_text("Base planner prompt.")
    custom_rules.write_text("- Always prefer monthly rollups when dates are vague.")

    monkeypatch.setitem(
        agent_module.zillion_config,
        "AGENT_BASE_PROMPT_FILE",
        str(base_prompt),
    )
    monkeypatch.setitem(
        agent_module.zillion_config,
        "AGENT_CUSTOM_RULES_FILE",
        str(custom_rules),
    )

    assert _get_planner_instructions() == (
        "Base planner prompt.\n\n"
        "Additional rules:\n"
        "- Always prefer monthly rollups when dates are vague."
    )


def test_agent_field_access_respects_config(config):
    config = config.copy()
    config["meta"] = {
        "agent": {
            "field_disabled_patterns": ["rpl_ma_5"],
            "field_disabled_groups": ["No Agent"],
        }
    }
    config["metrics"] = list(config["metrics"])
    config["metrics"][0] = dict(config["metrics"][0])
    config["metrics"][0]["meta"] = {"agent": {"enabled": False}}
    config["metrics"].append(
        {
            "name": "hidden_group_metric",
            "aggregation": "sum",
            "formula": "{revenue}",
            "meta": {"group": "No Agent"},
        }
    )

    warehouse = Warehouse(config=config)
    metrics = _get_agent_accessible_fields(warehouse, field_type="metric")

    assert "rpl" not in metrics
    assert "rpl_ma_5" not in metrics
    assert "hidden_group_metric" not in metrics
    assert "revenue" in metrics


def test_validate_report_spec_rejects_sql_date_expressions(config):
    warehouse = Warehouse(config=config)

    with pytest.raises(
        ValueError,
        match="Criteria must use concrete literal date/datetime values",
    ):
        agent_module._validate_report_spec(
            warehouse,
            {
                "metrics": ["revenue"],
                "dimensions": ["date"],
                "criteria": [["date", ">=", "CURRENT_DATE - 6 DAY"]],
            },
        )


def test_extract_question_metric_references_prefers_exact_longer_metric_names(config):
    warehouse = Warehouse(config=config)

    matches = _extract_question_metric_references(
        warehouse,
        "What is revenue_required_grain by campaign_name?",
    )

    assert "revenue_required_grain" in matches
    assert "revenue" not in matches


def test_plan_question_rejects_incomplete_requested_coverage(monkeypatch, config):
    warehouse = Warehouse(config=config)
    monkeypatch.setattr(
        agent_module,
        "_run_planning_agent",
        lambda *args, **kwargs: {
            "report_specs": [{"metrics": ["revenue"]}],
            "combine_strategy": "auto",
        },
    )

    with pytest.raises(ValueError, match="Question plan does not cover the request"):
        plan_warehouse_question(
            warehouse,
            "revenue and sales by date for the last 7 days",
        )


def test_plan_question_rejects_non_dict_wrangling_steps(monkeypatch, config):
    warehouse = Warehouse(config=config)
    monkeypatch.setattr(
        agent_module,
        "_run_planning_agent",
        lambda *args, **kwargs: {
            "report_specs": [{"metrics": ["revenue"], "dimensions": ["date"]}],
            "combine_strategy": "auto",
            "wrangling_steps": ["sort by date"],
        },
    )

    with pytest.raises(ValueError, match="Each wrangling step must be a dict"):
        plan_warehouse_question(
            warehouse,
            "revenue by date",
        )


@pytest.mark.agent
def test_plan_question_openai_e2e(config):
    _require_agent_test_environment()

    warehouse = Warehouse(config=config)
    plan = plan_warehouse_question(
        warehouse,
        "revenue and sales by date for the last 7 days",
        progress_callback=_print_agent_progress,
    )

    info(plan.__dict__)

    assert plan.report_specs
    metrics = _collect_plan_metrics(plan)
    dimensions = _collect_plan_dimensions(plan)
    criteria_fields = _collect_plan_criteria_fields(plan)

    assert {"revenue", "sales"}.issubset(metrics)
    assert "date" in dimensions
    assert "date" in criteria_fields
    assert isinstance(plan.rationale, str)
    assert all(isinstance(spec, dict) for spec in plan.report_specs)


@pytest.mark.agent
def test_plan_question_openai_e2e_handles_ranked_breakdown(config):
    _require_agent_test_environment()

    warehouse = Warehouse(config=config)
    plan = plan_warehouse_question(
        warehouse,
        "Show the top 5 partner_name values by revenue for the last 30 days",
        progress_callback=_print_agent_progress,
    )

    info(plan.__dict__)

    assert plan.report_specs
    metrics = _collect_plan_metrics(plan)
    dimensions = _collect_plan_dimensions(plan)
    criteria_fields = _collect_plan_criteria_fields(plan)
    limits = [spec.get("limit") for spec in plan.report_specs if spec.get("limit")]
    order_fields = {
        field_name
        for spec in plan.report_specs
        for field_name, *_ in (spec.get("order_by") or [])
    }

    assert "revenue" in metrics
    assert "partner_name" in dimensions
    assert "date" in criteria_fields
    assert limits and max(limits) <= 5
    assert order_fields & {"revenue", "partner_name"}


@pytest.mark.agent
def test_plan_question_openai_e2e_handles_required_grain_metric(config):
    _require_agent_test_environment()

    warehouse = Warehouse(config=config)
    plan = plan_warehouse_question(
        warehouse,
        "What is revenue_required_grain by campaign_name?",
        progress_callback=_print_agent_progress,
    )

    info(plan.__dict__)

    assert plan.report_specs
    metrics = _collect_plan_metrics(plan)
    dimensions = _collect_plan_dimensions(plan)

    assert "revenue_required_grain" in metrics
    assert "campaign_name" in dimensions


@pytest.mark.agent
def test_plan_question_openai_e2e_requires_multiple_reports(config):
    _require_agent_test_environment()

    warehouse = Warehouse(config=config)
    plan = plan_warehouse_question(
        warehouse,
        "Show revenue by partner_name and lead_count by campaign_name as separate reports",
        progress_callback=_print_agent_progress,
    )

    info(plan.__dict__)

    assert len(plan.report_specs) >= 2
    metrics_by_report = [set(spec.get("metrics") or []) for spec in plan.report_specs]
    dimensions_by_report = [
        set(spec.get("dimensions") or []) for spec in plan.report_specs
    ]

    assert any("revenue" in metrics for metrics in metrics_by_report)
    assert any(
        metrics & {"lead_count", "lead_count_distinct", "leads"}
        for metrics in metrics_by_report
    )
    assert any("partner_name" in dimensions for dimensions in dimensions_by_report)
    assert any("campaign_name" in dimensions for dimensions in dimensions_by_report)
    assert plan.combine_strategy in {"separate", "auto", "concat_rows"}


@pytest.mark.agent
def test_plan_question_openai_e2e_requires_combined_reports(config):
    _require_agent_test_environment()

    warehouse = Warehouse(config=config)
    plan = plan_warehouse_question(
        warehouse,
        "Use separate reports and merge them on date to compare revenue and sales trend by date for the last 7 days",
        progress_callback=_print_agent_progress,
    )

    info(plan.__dict__)

    assert len(plan.report_specs) >= 2
    metrics_by_report = [set(spec.get("metrics") or []) for spec in plan.report_specs]
    dimensions_by_report = [
        set(spec.get("dimensions") or []) for spec in plan.report_specs
    ]
    criteria_fields = _collect_plan_criteria_fields(plan)

    assert any("revenue" in metrics for metrics in metrics_by_report)
    assert any("sales" in metrics for metrics in metrics_by_report)
    assert all("date" in dimensions for dimensions in dimensions_by_report)
    assert "date" in criteria_fields
    assert plan.combine_strategy in {"merge", "join_on_index", "auto"}
    if plan.combine_strategy == "merge":
        assert "date" in plan.combine_on


@pytest.mark.agent
def test_answer_question_openai_e2e_handles_filtered_campaign_breakdown(config):
    _require_agent_test_environment()

    warehouse = Warehouse(config=config)
    result = answer_warehouse_question(
        warehouse,
        "Revenue by campaign_name for Partner A in the last 30 days",
        progress_callback=_print_agent_progress,
    )

    info(result.__dict__)
    info(result.summary)

    assert result.summary
    assert result.data is not None
    metrics = _collect_plan_metrics(result.plan)
    dimensions = _collect_plan_dimensions(result.plan)
    criteria_fields = _collect_plan_criteria_fields(result.plan)

    assert "revenue" in metrics
    assert "campaign_name" in dimensions
    assert {"partner_name", "date"}.issubset(criteria_fields)


@pytest.mark.agent
def test_answer_question_openai_e2e_handles_lead_range_question(config):
    _require_agent_test_environment()

    warehouse = Warehouse(config=config)
    result = answer_warehouse_question(
        warehouse,
        "Show lead_count by lead_id for lead_id between 1 and 5",
        progress_callback=_print_agent_progress,
    )

    info(result.__dict__)
    info(result.summary)

    assert result.summary
    assert result.data is not None
    metrics = _collect_plan_metrics(result.plan)
    dimensions = _collect_plan_dimensions(result.plan)
    criteria_fields = _collect_plan_criteria_fields(result.plan)

    assert metrics & {"lead_count", "lead_count_distinct", "leads"}
    assert "lead_id" in dimensions
    assert "lead_id" in criteria_fields


@pytest.mark.agent
def test_answer_question_openai_e2e(config):
    _require_agent_test_environment()

    warehouse = Warehouse(config=config)
    result = answer_warehouse_question(
        warehouse,
        "revenue by partner name",
        progress_callback=_print_agent_progress,
    )

    info(result.__dict__)
    info(result.summary)
    assert result.summary
    assert result.data is not None
    assert result.plan.report_specs[0]["dimensions"] == ["partner_name"]
