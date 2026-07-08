from __future__ import annotations

from genie_space_optimizer.common.config import BENCHMARK_PROMPTS
from genie_space_optimizer.optimization.evaluation import _attempt_sql_correction


def test_optimizer_and_audit_prompts_are_registered_prompt_keys() -> None:
    assert "unified_optimizer_patch" in BENCHMARK_PROMPTS
    assert "audit_summary" in BENCHMARK_PROMPTS


def test_sql_correction_prompt_receives_join_specs_context(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_call(_w, prompt, *args, **kwargs):
        captured["prompt"] = prompt
        return {"benchmarks": []}

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.evaluation._call_llm_for_scoring",
        fake_call,
    )

    prompt_template = "JOIN SPECS:\n{{ join_specs_context }}\nFIX:\n{{ benchmarks_to_fix }}"
    _attempt_sql_correction(
        None,
        {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.orders", "column_configs": []},
                    {"identifier": "cat.sch.customers", "column_configs": []},
                ]
            },
            "instructions": {
                "join_specs": [
                    {
                        "left": {"identifier": "cat.sch.orders"},
                        "right": {"identifier": "cat.sch.customers"},
                        "sql": ["orders.customer_id = customers.customer_id"],
                    }
                ]
            },
        },
        [],
        [],
        [
            {
                "question": "bad sql",
                "expected_sql": "SELECT * FROM orders",
                "validation_error": "missing column",
            }
        ],
        "cat",
        "sch",
        None,
        {},
        correction_prompt_template=prompt_template,
        correction_prompt_registry_key="benchmark_correction",
    )

    assert "orders.customer_id = customers.customer_id" in captured["prompt"]
    assert "{{ join_specs_context }}" not in captured["prompt"]


class _FakeSpan:
    def __init__(self) -> None:
        self.inputs = None
        self.outputs = None

    def set_inputs(self, value):
        self.inputs = value

    def set_outputs(self, value):
        self.outputs = value


class _FakeSpanContext:
    def __init__(self, span: _FakeSpan) -> None:
        self.span = span

    def __enter__(self):
        return self.span

    def __exit__(self, exc_type, exc, tb):
        return False


def test_optimizer_patch_prompt_links_and_records_context_metadata(monkeypatch) -> None:
    from genie_space_optimizer.optimization import unified_loop

    linked: list[str] = []
    span = _FakeSpan()

    monkeypatch.setattr(unified_loop, "_start_chain_span", lambda _name: _FakeSpanContext(span))
    monkeypatch.setattr(unified_loop, "_link_prompt_to_trace", lambda prompt_name: linked.append(prompt_name))
    captured_kwargs: dict = {}

    def fake_call_llm(_w, *, messages, **kwargs):
        captured_kwargs.update(kwargs)
        return (
            '{"lever": 1, "rationale": "r", "patches": []}',
            object(),
        )

    monkeypatch.setattr(unified_loop, "call_llm", fake_call_llm)

    unified_loop.propose_patches(
        None,
        allowed_levers=[1],
        current_config={"data_sources": {"tables": [{"identifier": "cat.sch.orders"}]}},
        eval_result={
            "rows": [
                {
                    "question_id": "q1",
                    "assessment": "BAD",
                    "question": "total revenue",
                    "expected_sql": "SELECT SUM(amount) FROM cat.sch.orders",
                }
            ]
        },
        reflections=[],
        catalog="cat",
        schema="sch",
    )

    assert linked == ["cat.sch.genie_opt_unified_optimizer_patch"]
    assert span.inputs["prompt_name"] == "cat.sch.genie_opt_unified_optimizer_patch"
    assert span.inputs["prompt_chars"] > 0
    assert span.inputs["context_chars"] > 0
    assert span.inputs["context_hash"]
    assert span.inputs["failure_ids"] == ["q1"]
    assert span.inputs["included_counts"]["assets"] >= 1
    assert span.inputs["omitted_counts"]["assets"] >= 0
    assert "messages" in span.inputs
    assert span.outputs["response_chars"] > 0
    assert captured_kwargs["response_format"] == {"type": "json_object"}


def test_audit_summary_links_registered_prompt_and_records_safe_context(monkeypatch) -> None:
    from genie_space_optimizer.optimization import publish

    linked: list[str] = []
    captured_messages = {}
    span = _FakeSpan()

    def fake_call_llm(_w, *, messages, **_kwargs):
        captured_messages["messages"] = messages
        return "summary", object()

    monkeypatch.setattr(publish, "_start_chain_span", lambda _name: _FakeSpanContext(span))
    monkeypatch.setattr(publish, "_link_prompt_to_trace", lambda prompt_name: linked.append(prompt_name))
    monkeypatch.setattr(publish, "call_llm", fake_call_llm)

    ctx = {
        "terminal_reason": "TARGET_REACHED",
        "champion_accuracy": 91.0,
        "improvement_trajectory": [{"iteration": 0, "accuracy": 80.0}],
        "patch_families": {"lever_1": 2},
        "root_cause_distribution": {"WRONG_COLUMN": 1},
    }
    summary, concern = publish.build_audit_summary(
        None,
        ctx,
        prompt_name="cat.sch.genie_opt_audit_summary",
    )

    assert summary == "summary"
    assert concern is None
    assert linked == ["cat.sch.genie_opt_audit_summary"]
    assert span.inputs["prompt_name"] == "cat.sch.genie_opt_audit_summary"
    assert span.inputs["audit_context_hash"]
    assert span.inputs["audit_context_field_count"] == len(ctx)
    assert span.inputs["improvement_trajectory_count"] == 1
    assert span.inputs["patch_family_count"] == 1
    assert span.inputs["root_cause_field_count"] == 1
    assert captured_messages["messages"][1]["content"]
