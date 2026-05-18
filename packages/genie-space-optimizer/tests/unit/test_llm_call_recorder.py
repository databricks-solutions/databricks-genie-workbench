"""Phase 3.5 Task 1 — recorder Protocol + InMemory + binding ContextVar."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.llm_call_recorder import (
    InMemoryLLMCallRecorder,
    RecorderBinding,
    _RECORDER_BINDING,
    set_ag_binding,
    set_iteration_binding,
)


@pytest.fixture(autouse=True)
def _reset_recorder_binding():
    """Ensure each test starts from the default binding. The harness
    binding helpers (``_tape_binding_set_iteration`` /
    ``_tape_binding_set_ag``) mutate ``_RECORDER_BINDING`` without
    saving tokens, so earlier tests in the same pytest session may
    leak state into ours."""
    token = _RECORDER_BINDING.set(
        RecorderBinding(iteration=-1, ag_id="", cluster_id=""),
    )
    try:
        yield
    finally:
        _RECORDER_BINDING.reset(token)


def test_default_binding_is_unbound():
    b = _RECORDER_BINDING.get()
    assert b == RecorderBinding(iteration=-1, ag_id="", cluster_id="")


def test_set_iteration_binding_updates_iteration_only():
    token = _RECORDER_BINDING.set(RecorderBinding(-1, "", ""))
    try:
        set_iteration_binding(3)
        b = _RECORDER_BINDING.get()
        assert b.iteration == 3
        assert b.ag_id == ""
        assert b.cluster_id == ""
    finally:
        _RECORDER_BINDING.reset(token)


def test_set_ag_binding_updates_ag_and_cluster_only():
    token = _RECORDER_BINDING.set(RecorderBinding(5, "", ""))
    try:
        set_ag_binding("AG_X", cluster_id="H001")
        b = _RECORDER_BINDING.get()
        assert b.iteration == 5  # iteration preserved
        assert b.ag_id == "AG_X"
        assert b.cluster_id == "H001"
    finally:
        _RECORDER_BINDING.reset(token)


def test_in_memory_recorder_captures_binding_at_call_time():
    rec = InMemoryLLMCallRecorder()
    token = _RECORDER_BINDING.set(RecorderBinding(2, "AG_42", "H007"))
    try:
        rec.record(
            span_name="stage_1_discovery",
            system_msg="sys",
            prompt="prompt-text",
            response_text="response-text",
            response_metadata={"latency_ms": 123},
        )
    finally:
        _RECORDER_BINDING.reset(token)

    assert len(rec.calls) == 1
    call = rec.calls[0]
    assert call["span_name"] == "stage_1_discovery"
    assert call["iteration"] == 2
    assert call["ag_id"] == "AG_42"
    assert call["cluster_id"] == "H007"
    assert call["prompt"] == "prompt-text"
    assert call["response_text"] == "response-text"
    assert call["response_metadata"] == {"latency_ms": 123}
    assert len(call["prompt_sha256"]) == 64


def test_in_memory_recorder_drain_clears_buffer():
    rec = InMemoryLLMCallRecorder()
    rec.record(
        span_name="stage_1_discovery",
        system_msg="",
        prompt="p1",
        response_text="r1",
        response_metadata={},
    )
    rec.record(
        span_name="lever_4_join_discovery",
        system_msg="",
        prompt="p2",
        response_text="r2",
        response_metadata={},
    )
    drained = rec.drain()
    assert len(drained) == 2
    assert rec.calls == []
    # Second drain yields nothing.
    assert rec.drain() == []
