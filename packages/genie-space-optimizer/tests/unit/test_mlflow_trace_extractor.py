"""Phase 3.6 Task 3 — extract LLM calls from a synthetic MLflow trace."""
from __future__ import annotations

from unittest.mock import MagicMock

from genie_space_optimizer.optimization.mlflow_trace_extractor import (
    extract_llm_calls_from_trace,
    extract_llm_calls_from_traces,
)


def _make_span(
    *, name, span_id, span_type, parent_id=None,
    inputs=None, outputs=None, attributes=None,
    start_time_ns=0, end_time_ns=0,
):
    s = MagicMock()
    s.name = name
    s.span_id = span_id
    s.parent_id = parent_id
    s.span_type = span_type
    s.inputs = inputs or {}
    s.outputs = outputs or {}
    s.attributes = attributes or {}
    s.start_time_ns = start_time_ns
    s.end_time_ns = end_time_ns
    return s


def _make_trace(spans):
    t = MagicMock()
    t.data = MagicMock()
    t.data.spans = spans
    return t


def test_extract_stage_1_discovery_with_breadcrumbs():
    """Post-Task-2 run: parent CHAIN span carries iteration/ag/cluster."""
    parent = _make_span(
        name="stage_1_discovery",
        span_id="P1",
        span_type="CHAIN",
        inputs={
            "model": "stub",
            "temperature": 0.0,
            "iteration": 0,
            "ag_id": "",
            "cluster_id": "",
        },
        outputs={"response_chars": 100, "attempts": 1},
    )
    child = _make_span(
        name="ChatCompletion",
        span_id="C1",
        parent_id="P1",
        span_type="CHAT_MODEL",
        inputs={
            "messages": [
                {"role": "system", "content": "you are a planner"},
                {"role": "user", "content": "discover stage 1 picks"},
            ],
            "model": "stub",
            "temperature": 0.0,
        },
        outputs={
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": '{"picks":[]}',
                    },
                },
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30,
            },
        },
    )
    trace = _make_trace([parent, child])

    calls = list(extract_llm_calls_from_trace(trace))
    assert len(calls) == 1
    call = calls[0]
    assert call["span_name"] == "stage_1_discovery"
    assert call["iteration"] == 0
    assert call["ag_id"] == ""
    assert call["cluster_id"] == ""
    assert call["system_msg"] == "you are a planner"
    assert call["prompt"] == "discover stage 1 picks"
    assert call["response_text"] == '{"picks":[]}'
    assert call["response_metadata"]["model"] == "stub"
    assert call["response_metadata"]["prompt_tokens"] == 10
    assert call["response_metadata"]["completion_tokens"] == 20
    assert call["response_metadata"]["total_tokens"] == 30
    assert "prompt_sha256" in call
    assert len(call["prompt_sha256"]) == 64


def test_extract_legacy_chain_without_breadcrumbs_defaults():
    """Pre-Task-2 run (historic): parent CHAIN span has no iteration/ag/cluster."""
    parent = _make_span(
        name="lever_4_join_discovery",
        span_id="P2",
        span_type="CHAIN",
        inputs={"model": "stub", "temperature": 0.0},
        outputs={"response_chars": 200, "attempts": 1},
    )
    child = _make_span(
        name="ChatCompletion",
        span_id="C2",
        parent_id="P2",
        span_type="CHAT_MODEL",
        inputs={"messages": [{"role": "user", "content": "discover joins"}]},
        outputs={
            "choices": [{"message": {"content": '{"joins":[]}'}}],
            "usage": {
                "prompt_tokens": 5,
                "completion_tokens": 5,
                "total_tokens": 10,
            },
        },
    )
    trace = _make_trace([parent, child])

    calls = list(extract_llm_calls_from_trace(trace))
    assert len(calls) == 1
    call = calls[0]
    assert call["span_name"] == "lever_4_join_discovery"
    assert call["iteration"] == -1
    assert call["ag_id"] == ""
    assert call["cluster_id"] == ""
    assert call["system_msg"] == ""
    assert call["prompt"] == "discover joins"
    assert call["response_text"] == '{"joins":[]}'


def test_chain_span_without_chat_model_child_is_skipped():
    """A CHAIN span whose only child is non-CHAT_MODEL → not an LLM call → skip."""
    parent = _make_span(
        name="cluster_driven_example_synthesis",
        span_id="P3",
        span_type="CHAIN",
        inputs={"model": "stub"},
    )
    child = _make_span(
        name="some_helper",
        span_id="C3",
        parent_id="P3",
        span_type="TOOL",
    )
    trace = _make_trace([parent, child])
    assert list(extract_llm_calls_from_trace(trace)) == []


def test_unknown_chain_span_name_is_skipped():
    """A CHAIN span with a name not in _KNOWN_STAGES → skip (with no log)."""
    parent = _make_span(
        name="some_other_chain_we_dont_care_about",
        span_id="P4",
        span_type="CHAIN",
    )
    child = _make_span(
        name="ChatCompletion",
        span_id="C4",
        parent_id="P4",
        span_type="CHAT_MODEL",
        inputs={"messages": [{"role": "user", "content": "ignored"}]},
        outputs={"choices": [{"message": {"content": "ignored"}}]},
    )
    trace = _make_trace([parent, child])
    assert list(extract_llm_calls_from_trace(trace)) == []


def test_multiple_traces_concatenate_in_order():
    p1 = _make_span(
        name="stage_1_discovery", span_id="P1", span_type="CHAIN",
        start_time_ns=1000,
    )
    c1 = _make_span(
        name="ChatCompletion", span_id="C1", parent_id="P1",
        span_type="CHAT_MODEL",
        inputs={"messages": [{"role": "user", "content": "A"}]},
        outputs={"choices": [{"message": {"content": "a"}}]},
    )
    p2 = _make_span(
        name="lever_4_join_discovery", span_id="P2", span_type="CHAIN",
        start_time_ns=2000,
    )
    c2 = _make_span(
        name="ChatCompletion", span_id="C2", parent_id="P2",
        span_type="CHAT_MODEL",
        inputs={"messages": [{"role": "user", "content": "B"}]},
        outputs={"choices": [{"message": {"content": "b"}}]},
    )
    traces = [_make_trace([p1, c1]), _make_trace([p2, c2])]
    calls = list(extract_llm_calls_from_traces(traces))
    assert [c["span_name"] for c in calls] == [
        "stage_1_discovery", "lever_4_join_discovery",
    ]
    assert [c["prompt"] for c in calls] == ["A", "B"]
