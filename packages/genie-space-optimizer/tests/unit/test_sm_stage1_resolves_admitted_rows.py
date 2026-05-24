"""SM Cutover Phase 1.A — TransformerContext must carry the baseline eval rows.

Until 2026-05-23, ``run_state_machine_iteration_and_persist`` constructed
``TransformerContext`` without populating ``baseline_eval_rows``, so every
Stage 1 transformer that called ``_find_eval_row(ctx, qid)`` returned
``None`` and terminated the QID with ``no_eval_row_for_qid``. Production
admission worked but every transition was a no-op. This test pins the
plumbing fix.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "production_eval_rows.json"
)


@pytest.fixture(scope="module")
def production_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["eval_rows"]]


def test_context_baseline_eval_rows_is_populated_for_sm(
    production_rows: list[dict],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``run_state_machine_iteration_and_persist`` must thread
    ``eval_rows`` into ``TransformerContext.baseline_eval_rows``. We assert
    by intercepting ``sm.run_iteration`` and inspecting the ctx the SM
    receives — that is the actual surface every transformer reads from.
    """
    from genie_space_optimizer.optimization import optimizer as opt_mod

    captured: dict = {}

    class _FakeStateMachine:
        def run_iteration(self, initial_states, ctx):
            captured["baseline_eval_rows"] = tuple(ctx.baseline_eval_rows)
            captured["w"] = ctx.w
            return tuple()

    # ``build_production_state_machine`` is lazy-imported inside the
    # function body. Patch it where it lives — the registry module.
    from genie_space_optimizer.optimization.state_machine import (
        registry as registry_mod,
    )
    monkeypatch.setattr(
        registry_mod, "build_production_state_machine",
        lambda: _FakeStateMachine(),
    )

    opt_mod.run_state_machine_iteration_and_persist(
        eval_rows=production_rows,
        iteration=1,
        run_id="test_phase1a",
        run_root=tmp_path,
        workspace_client=None,
        forbidden_signatures=(),
    )

    assert "baseline_eval_rows" in captured
    assert len(captured["baseline_eval_rows"]) == len(production_rows)
    # Trial 13 Phase 8 — every row in baseline_eval_rows is now a
    # ``CanonicalEvalRow`` (Mapping-compatible) projected by
    # ``normalize_eval_row``. The Mapping shim keeps legacy dict
    # consumers working.
    from collections.abc import Mapping

    from genie_space_optimizer.optimization.canonical_eval_row import (
        CanonicalEvalRow,
    )
    assert all(
        isinstance(r, CanonicalEvalRow)
        for r in captured["baseline_eval_rows"]
    )
    assert all(isinstance(r, Mapping) for r in captured["baseline_eval_rows"])


def test_find_eval_row_resolves_production_shape_row_via_ctx(
    production_rows: list[dict],
) -> None:
    """Once ``baseline_eval_rows`` is populated, the canonical
    ``_find_eval_row`` lookup must resolve a production-shape qid to its
    row through the context. This is the upstream of the Stage 1
    ``no_eval_row_for_qid`` failure mode.
    """
    from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
        _find_eval_row,
    )

    class _Ctx:
        baseline_eval_rows = tuple(dict(r) for r in production_rows)

    expected_qid = "airline_ticketing_and_fare_analysis_gs_009"
    row = _find_eval_row(_Ctx(), expected_qid)
    assert row is not None
    # The resolved row must carry the qid under at least one of the keys
    # the canonical extractor handles.
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )
    candidate_qid, _src = extract_question_id(row)
    assert candidate_qid == expected_qid
