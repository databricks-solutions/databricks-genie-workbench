"""Unit tests for LeverLoopReplayHarness."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization import optimizer as _opt
from genie_space_optimizer.optimization.optimizer import _LLM_CALLER_OVERRIDE
from genie_space_optimizer.optimization.tape import (
    LeverLoopTape,
    TapeEntry,
    TapeKey,
    prompt_sha256,
)
from genie_space_optimizer.optimization.tape_replay_harness import (
    LeverLoopReplayHarness,
)


def _tape_with(prompt: str, response: str, *, stage: str, iteration: int) -> LeverLoopTape:
    return LeverLoopTape(
        tape_id="t",
        source_run_id="r",
        captured_at="",
        entries=[
            TapeEntry(
                key=TapeKey(
                    stage=stage, iteration=iteration,
                    ag_id="", cluster_id="",
                    prompt_sha256=prompt_sha256(prompt),
                ),
                prompt=prompt,
                response_text=response,
                response_metadata={},
            )
        ],
    )


def test_harness_installs_and_resets_llm_override():
    tape = _tape_with("p", "r", stage="adaptive_strategy", iteration=0)
    assert _LLM_CALLER_OVERRIDE.get() is None

    with LeverLoopReplayHarness(tape=tape) as h:
        assert _LLM_CALLER_OVERRIDE.get() is not None
        h.bind_iteration(0)
        text, _ = _opt._traced_llm_call(
            w=None, system_msg="", prompt="p",
            span_name="adaptive_strategy",
        )
        assert text == "r"

    assert _LLM_CALLER_OVERRIDE.get() is None


def test_harness_captures_patch_space_config_calls():
    """The harness must intercept the Genie PATCH call so replay never
    mutates production spaces."""
    tape = _tape_with("p", "r", stage="adaptive_strategy", iteration=0)

    with LeverLoopReplayHarness(tape=tape) as h:
        from genie_space_optimizer.common import genie_client as _gc
        result = _gc.patch_space_config(
            w=None, space_id="space-123", config={"k": "v"},
        )
        assert result == {"replay": True}

    assert len(h.captured_patches) == 1
    assert h.captured_patches[0]["space_id"] == "space-123"


def test_harness_captures_write_stage_calls():
    tape = _tape_with("p", "r", stage="adaptive_strategy", iteration=0)

    with LeverLoopReplayHarness(tape=tape) as h:
        from genie_space_optimizer.optimization import state as _state
        _state.write_stage(
            spark=None, run_id="run-1", stage="X", status="STARTED",
            task_key="lever_loop", catalog="c", schema="s",
        )

    assert len(h.captured_write_stage_calls) == 1
    assert h.captured_write_stage_calls[0]["stage"] == "X"


def test_harness_routes_run_evaluation_to_tape_per_iteration():
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="", entries=[],
        evals_by_iteration={
            0: [{"question_id": "q1", "result_correctness": "yes"}],
            1: [{"question_id": "q1", "result_correctness": "no"}],
        },
    )

    with LeverLoopReplayHarness(tape=tape) as h:
        from genie_space_optimizer.optimization import evaluation as _eval

        h.bind_iteration(0)
        out0 = _eval.run_evaluation(
            space_id="s", experiment_name="e", iteration=0,
            benchmarks=[], domain="d", model_id=None,
            eval_scope="", predict_fn=None, scorers=[],
        )

        h.bind_iteration(1)
        out1 = _eval.run_evaluation(
            space_id="s", experiment_name="e", iteration=1,
            benchmarks=[], domain="d", model_id=None,
            eval_scope="", predict_fn=None, scorers=[],
        )

    assert out0["eval_rows"][0]["result_correctness"] == "yes"
    assert out1["eval_rows"][0]["result_correctness"] == "no"


def test_harness_iteration_binding_hook_invoked_by_run_lever_loop(monkeypatch):
    """harness.py exposes _TAPE_BINDING_HOOK that _run_lever_loop calls
    once per iteration. This lets the replay harness advance the
    binding without touching every LLM call site."""
    from genie_space_optimizer.optimization import harness as _harness

    invocations: list[int] = []

    def _hook(iteration: int, *, ag_id: str = "", cluster_id: str = "") -> None:
        invocations.append(iteration)

    monkeypatch.setattr(_harness, "_TAPE_BINDING_HOOK", _hook, raising=True)

    _harness._tape_binding_set_iteration(2)
    _harness._tape_binding_set_iteration(3)

    assert invocations == [2, 3]


def test_tape_binding_set_ag_invokes_hook_with_ag_id(monkeypatch):
    """harness._tape_binding_set_ag must pass ag_id/cluster_id through
    to the hook; the iteration argument is a sentinel and the
    replay-harness binding hook ignores it when ag_id is non-empty."""
    from genie_space_optimizer.optimization import harness as _harness

    invocations: list[dict] = []

    def _hook(iteration: int, *, ag_id: str = "", cluster_id: str = "") -> None:
        invocations.append({
            "iteration": iteration,
            "ag_id": ag_id,
            "cluster_id": cluster_id,
        })

    monkeypatch.setattr(_harness, "_TAPE_BINDING_HOOK", _hook, raising=True)

    _harness._tape_binding_set_ag("AG_001", cluster_id="H001")

    assert invocations == [
        {"iteration": -1, "ag_id": "AG_001", "cluster_id": "H001"},
    ]
