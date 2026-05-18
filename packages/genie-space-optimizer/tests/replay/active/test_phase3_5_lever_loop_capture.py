"""Phase 3.5 Task 3 — recorder drain attribution shape test.

This test verifies the recorder/drain plumbing pattern that
``_run_lever_loop`` uses: capture calls under bound iteration,
drain at end of loop, attribute each call to the matching iteration
snapshot in ``_replay_fixture_iterations``.

The full integration test (driving real ``_run_lever_loop`` against
a minimal fixture) is covered by the Phase 3 anchor smoke tests
which now exercise the recorder install/drain path end-to-end. This
unit test pins the attribution algorithm.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_call_recorder import (
    InMemoryLLMCallRecorder,
    RecorderBinding,
    _RECORDER_BINDING,
)


def _stub_openai_response(json_body: str):
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = json_body
    comp = MagicMock()
    comp.choices = [choice]
    comp.usage = MagicMock(prompt_tokens=1, completion_tokens=1, total_tokens=2)
    client.chat.completions.create.return_value = comp
    return client


def test_recorder_attributes_calls_to_their_iteration_via_binding():
    """Each captured call carries the iteration that was bound at
    record time. Draining and re-attributing by ``iteration`` field
    matches each call to its iteration snapshot — the algorithm
    ``_run_lever_loop`` uses to populate ``llm_call_log``."""
    rec = InMemoryLLMCallRecorder()
    binding_token = _RECORDER_BINDING.set(
        RecorderBinding(iteration=-1, ag_id="", cluster_id=""),
    )
    rec_token = optimizer._LLM_CALL_RECORDER.set(rec)
    try:
        with patch.object(
            optimizer,
            "_get_openai_client",
            return_value=_stub_openai_response('{"picks":[]}'),
        ):
            # Iteration 0
            _RECORDER_BINDING.set(RecorderBinding(0, "", ""))
            optimizer._traced_llm_call(
                w=None, system_msg="", prompt="iter0-stage1",
                span_name="stage_1_discovery", max_retries=1,
            )
            # Iteration 1 — Stage 1 + a Stage 2
            _RECORDER_BINDING.set(RecorderBinding(1, "", ""))
            optimizer._traced_llm_call(
                w=None, system_msg="", prompt="iter1-stage1",
                span_name="stage_1_discovery", max_retries=1,
            )
            _RECORDER_BINDING.set(RecorderBinding(1, "AG_X", "H001"))
            optimizer._traced_llm_call(
                w=None, system_msg="", prompt="iter1-l4",
                span_name="lever_4_join_discovery", max_retries=1,
            )
    finally:
        optimizer._LLM_CALL_RECORDER.reset(rec_token)
        _RECORDER_BINDING.reset(binding_token)

    drained = rec.drain()
    assert len(drained) == 3

    # Simulate the harness's post-loop attribution: build the
    # iteration->snapshot map and route each call by its
    # ``iteration`` field.
    iterations_data = [
        {"iteration": 0},
        {"iteration": 1},
    ]
    by_idx = {int(s["iteration"]): s for s in iterations_data}
    for call in drained:
        snap = by_idx.get(int(call["iteration"]))
        if snap is not None:
            snap.setdefault("llm_call_log", []).append(call)

    assert len(iterations_data[0]["llm_call_log"]) == 1
    assert iterations_data[0]["llm_call_log"][0]["span_name"] == "stage_1_discovery"
    assert len(iterations_data[1]["llm_call_log"]) == 2
    spans_iter1 = [c["span_name"] for c in iterations_data[1]["llm_call_log"]]
    assert spans_iter1 == ["stage_1_discovery", "lever_4_join_discovery"]
    # AG binding is preserved on the Stage-2 call.
    l4 = iterations_data[1]["llm_call_log"][1]
    assert l4["ag_id"] == "AG_X"
    assert l4["cluster_id"] == "H001"


def test_recorder_calls_with_no_matching_iteration_are_dropped():
    """Calls bound to iteration=-1 (or beyond captured set) drop
    silently — they represent one-time setup outside the loop."""
    drained = [
        {"iteration": -1, "span_name": "out_of_loop"},
        {"iteration": 0, "span_name": "in_loop"},
    ]
    iterations_data = [{"iteration": 0}]
    by_idx = {int(s["iteration"]): s for s in iterations_data}
    for call in drained:
        snap = by_idx.get(int(call["iteration"]))
        if snap is not None:
            snap.setdefault("llm_call_log", []).append(call)

    assert len(iterations_data[0]["llm_call_log"]) == 1
    assert iterations_data[0]["llm_call_log"][0]["span_name"] == "in_loop"
