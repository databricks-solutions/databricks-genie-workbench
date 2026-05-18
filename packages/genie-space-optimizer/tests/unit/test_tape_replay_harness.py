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


def test_harness_serves_load_latest_full_iteration_from_tape():
    """Phase 3.6.2 E2 — replay stubs ``state.load_latest_full_iteration``
    so the lever loop's Phase A baseline read returns the tape's
    ``iteration_payloads[<max_index>]`` instead of None (the
    pre-Phase-3.6.2 behavior under MagicMock spark)."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=3,
        iteration_payloads={
            0: {
                "iteration": 1,
                "rows_json": [{"qid": "q1", "passed": False}],
                "eval_scope": "full",
                "rolled_back": False,
            },
            1: {
                "iteration": 2,
                "rows_json": [{"qid": "q1", "passed": True}],
                "eval_scope": "full",
                "rolled_back": False,
            },
        },
    )

    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import state as _state
        result = _state.load_latest_full_iteration(
            spark=None, run_id="r", catalog="c", schema="s",
        )
    assert result is not None
    # Highest iteration index wins.
    assert result["iteration"] == 2
    assert result["rows_json"][0]["passed"] is True


def test_harness_load_latest_full_iteration_respects_before_iteration():
    """``before_iteration`` filters by the 1-indexed iteration value
    in the payload (mirrors production behavior — see
    ``state.load_latest_full_iteration`` docstring)."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=3,
        iteration_payloads={
            0: {
                "iteration": 1, "rows_json": [{"qid": "q1"}],
                "eval_scope": "full", "rolled_back": False,
            },
            1: {
                "iteration": 2, "rows_json": [{"qid": "q2"}],
                "eval_scope": "full", "rolled_back": False,
            },
        },
    )
    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import state as _state
        # before_iteration=2 → only iteration 1 visible.
        result = _state.load_latest_full_iteration(
            spark=None, run_id="r", catalog="c", schema="s",
            before_iteration=2,
        )
    assert result is not None
    assert result["iteration"] == 1


def test_harness_load_latest_full_iteration_empty_payloads_returns_none():
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=2,  # legacy → no iteration_payloads
    )
    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import state as _state
        result = _state.load_latest_full_iteration(
            spark=None, run_id="r", catalog="c", schema="s",
        )
    assert result is None


def test_harness_serves_load_latest_state_iteration_includes_enrichment():
    """Phase 3.6.2 E3 — ``load_latest_state_iteration`` accepts both
    ``full`` and ``enrichment`` eval_scope. Highest payload index
    wins (tape replay has no timestamps)."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=3,
        iteration_payloads={
            0: {"iteration": 0, "eval_scope": "enrichment",
                "rolled_back": False, "rows_json": []},
            1: {"iteration": 1, "eval_scope": "full",
                "rolled_back": False, "rows_json": [{"qid": "q1"}]},
        },
    )
    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import state as _state
        result = _state.load_latest_state_iteration(
            spark=None, run_id="r", catalog="c", schema="s",
        )
    assert result is not None
    assert result["iteration"] == 1
    assert result["eval_scope"] == "full"


def test_harness_serves_load_all_full_iterations_ordered_asc():
    """All ``full`` payloads, iteration ASC."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=3,
        iteration_payloads={
            0: {"iteration": 1, "eval_scope": "full", "rolled_back": False},
            1: {"iteration": 2, "eval_scope": "enrichment",
                "rolled_back": False},  # excluded
            2: {"iteration": 3, "eval_scope": "full", "rolled_back": False},
            3: {"iteration": 4, "eval_scope": "full", "rolled_back": True},  # excluded
        },
    )
    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import state as _state
        rows = _state.load_all_full_iterations(
            spark=None, run_id="r", catalog="c", schema="s",
        )
    assert [r["iteration"] for r in rows] == [1, 3]


def test_harness_serves_load_run_with_synthetic_metadata():
    """``load_run`` synthesizes a minimal run-metadata dict.
    Downstream consumers needing richer metadata surface as the
    next stop-and-report."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="source-r",
        captured_at="2026-05-18T00:00:00Z", entries=[],
        format_version=3,
        iteration_payloads={
            0: {"iteration": 1, "eval_scope": "full", "rolled_back": False},
        },
    )
    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import state as _state
        result = _state.load_run(
            spark=None, run_id="r", catalog="c", schema="s",
        )
    assert result is not None
    assert result["run_id"] == "r"
    assert result["source_run_id"] == "source-r"
    assert result["status"] == "running"
    assert result["levers"] == []
    assert result["config_snapshot"] == {}


def test_harness_load_stages_returns_empty_dataframe():
    """Phase 3.6.2 E4 — ``state.load_stages`` empty-stub. Fires
    post-loop; not iteration-critical. Returning an empty DataFrame
    keeps the summary path quiet without pretending to have data."""
    import pandas as pd
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=3,
        iteration_payloads={
            0: {"iteration": 1, "eval_scope": "full", "rolled_back": False},
        },
    )
    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import state as _state
        df = _state.load_stages(
            spark=None, run_id="r", catalog="c", schema="s",
        )
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_harness_load_provenance_returns_empty_dataframe():
    """Phase 3.6.2 E4 — ``state.load_provenance`` empty-stub."""
    import pandas as pd
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=3,
        iteration_payloads={
            0: {"iteration": 1, "eval_scope": "full", "rolled_back": False},
        },
    )
    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import state as _state
        df = _state.load_provenance(
            spark=None, run_id="r", catalog="c", schema="s",
        )
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_harness_serves_clusters_from_iteration_payload():
    """Phase 3.6.2 E5b — clustering is upstream of lever-loop
    decision logic; tape-serve the COMPUTED clusters from the
    current iteration's payload rather than re-deriving from raw
    rows + (absent) per-row ASI metadata."""
    from genie_space_optimizer.optimization.llm_call_recorder import (
        _RECORDER_BINDING, RecorderBinding,
    )
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=3,
        iteration_payloads={
            0: {
                "iteration": 1, "eval_scope": "full", "rolled_back": False,
                "clusters": [
                    {"cluster_id": "H001", "question_ids": ["q1", "q2"],
                     "asi_failure_type": "x", "root_cause": "y"},
                ],
                "soft_clusters": [
                    {"cluster_id": "S001", "question_ids": ["q3"],
                     "asi_failure_type": "soft", "root_cause": "z"},
                ],
            },
        },
    )
    binding_token = _RECORDER_BINDING.set(
        RecorderBinding(iteration=0, ag_id="", cluster_id=""),
    )
    try:
        with LeverLoopReplayHarness(tape=tape):
            from genie_space_optimizer.optimization import optimizer as _opt
            hard = _opt.cluster_failures(
                {"rows": []}, {}, signal_type="hard",
            )
            soft = _opt.cluster_failures(
                {"rows": []}, {}, signal_type="soft",
            )
    finally:
        _RECORDER_BINDING.reset(binding_token)
    assert [c["cluster_id"] for c in hard] == ["H001"]
    assert [c["cluster_id"] for c in soft] == ["S001"]


def test_harness_cluster_failures_empty_payload_returns_empty():
    """When iteration_payloads is empty (v2 legacy tape), the
    clustering stub returns []. The downstream lever loop handles
    empty clusters gracefully (same as a real run with no
    failures)."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0", entries=[],
        format_version=2,
    )
    with LeverLoopReplayHarness(tape=tape):
        from genie_space_optimizer.optimization import optimizer as _opt
        out = _opt.cluster_failures({"rows": []}, {}, signal_type="hard")
    assert out == []


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
