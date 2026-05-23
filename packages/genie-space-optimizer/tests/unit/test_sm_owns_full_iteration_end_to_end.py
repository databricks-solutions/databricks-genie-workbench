"""SM Cutover Phase 6 — forward-only end-to-end test.

The plan calls this "the test we have been missing for four rounds." It
proves that production-shape eval rows, when fed through the full
``run_state_machine_iteration_and_persist`` entrypoint with mocked LLM
calls and Genie API mutations, reach a typed ``APPLIED`` (or deeper)
terminal AND that the per-QID ``qstate`` + ``trajectory`` JSON files
are persisted.

Why this is the missing test:
    * Replay fixtures are normalized to ``{question_id: ...}`` at the
      top level, so prior tests never exercised the canonical QID
      extractor on the production shape (``inputs/question_id``,
      ``inputs.question_id`` dict, ``request.kwargs.question_id`` JSON
      string).
    * No prior test routed those rows all the way through the SM and
      persisted the trajectory — so the four trial postmortems all
      surfaced the same class of bug (`baseline_eval_rows` empty,
      `no_eval_row_for_qid`, parity drift) only at deploy time.

Failure mode if this test ever regresses: a postmortem will name the
ONE transformer that declined (e.g. ``SYNTHESIZE_DECLINED for
plural_top_n_collapse``) instead of "is this another plumbing seam?"
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _build_anchor_fixture() -> dict:
    """Reuse the ``gs_026`` anchor fixture — it has a full RCA card and
    expected proposal ready for the SM to consume. The QID is mapped to
    a production-shape row below."""
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "tests" / "fixtures" / "anchor_qids" / "gs_026_sum_row_number.json"
    )
    # The path above is relative to packages/genie-space-optimizer/; we
    # need it relative to this test file's parents.
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures" / "anchor_qids" / "gs_026_sum_row_number.json"
    )
    return json.loads(fixture_path.read_text())


def _build_production_shape_row(fixture: dict) -> dict:
    """Build a production-shape eval row carrying the anchor's QID via
    the MLflow-flattened ``inputs/question_id`` key (one of the three
    schemas the canonical adapter handles).
    """
    return {
        # Production shape: MLflow logs eval rows with slash-flattened
        # keys, not top-level ``question_id``. Prior versions of the
        # dispatch adapter looked up ``row.get("question_id")`` and got
        # ``""`` — the canonical-row-shape P0 admission fix routes via
        # ``extract_question_id`` which handles this.
        "inputs/question_id": fixture["qid"],
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "ground_truth_correct",
        "score": 0.0,
        "sql": fixture["baseline_sql"],
        "expected_shape": fixture["expected_shape"],
        "eval_row_id": f"row_{fixture['qid']}",
    }


def test_sm_owns_full_iteration_from_production_shape_to_applied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production-shape row → ADMIT → DIAGNOSE → CLUSTER → SYNTHESIZE
    → VALIDATE → APPLY → typed terminal with deepest_stage >= APPLIED.

    Mocks: LLM transformers (diagnose, synthesize, narrow_replacement)
    return the fixture's canned proposal. The applier is wired through
    its real transformer surface; only the underlying Genie API call is
    intercepted.
    """
    from genie_space_optimizer.optimization import optimizer as opt_mod
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )

    fixture = _build_anchor_fixture()
    row = _build_production_shape_row(fixture)

    # Stub LLM transformers via TransformerContext.extras. The
    # production registry wires diagnose/cluster/synthesize/narrow off
    # the ctx.extras dispatcher when present.
    stub_diagnose = MagicMock(return_value=fixture["rca_card"])
    stub_synth = MagicMock(return_value=fixture["expected_proposal"])
    stub_narrow = MagicMock(return_value={
        "decision": "narrow_to",
        "narrowed_patch": fixture["expected_proposal"],
        "rationale": "phase-6 e2e stub",
    })

    # We intercept the SM construction so we can inject ctx.extras and
    # capture final states without driving the real Genie API.
    from genie_space_optimizer.optimization.state_machine import (
        registry as registry_mod,
    )
    real_build = registry_mod.build_production_state_machine

    captured: dict = {}

    class _CapturingStateMachine:
        def __init__(self) -> None:
            self._inner = real_build()

        def run_iteration(self, initial_states, ctx):  # noqa: ANN001
            # Inject LLM stubs into ctx.extras so the production
            # transformers route through them.
            extras = dict(getattr(ctx, "extras", {}) or {})
            extras.setdefault("diagnose_llm", stub_diagnose)
            extras.setdefault("synthesize_llm", stub_synth)
            extras.setdefault("narrow_replacement_llm", stub_narrow)
            from dataclasses import replace
            patched_ctx = replace(ctx, extras=extras)
            final = self._inner.run_iteration(initial_states, patched_ctx)
            captured["final_states"] = final
            return final

    monkeypatch.setattr(
        registry_mod, "build_production_state_machine",
        lambda: _CapturingStateMachine(),
    )

    final_states = opt_mod.run_state_machine_iteration_and_persist(
        eval_rows=[row],
        iteration=1,
        run_id="phase6_e2e",
        run_root=tmp_path,
        workspace_client=None,
        forbidden_signatures=(),
    )

    # Plumbing contract 1 — the SM saw exactly one admitted hard QID.
    assert len(final_states) == 1, (
        f"Expected one final state for the production-shape row; got "
        f"{len(final_states)}. This is the same starvation bug the "
        f"four trial postmortems flagged."
    )

    # Plumbing contract 2 — the QID reached APPLIED (or deeper).
    deepest = final_states[0].deepest_stage_reached
    assert deepest in (
        FunnelStage.APPLIED, FunnelStage.EVALUATED, FunnelStage.ACCEPTED,
    ), (
        f"{fixture['qid']} deepest stage was {deepest!r}; expected "
        f"APPLIED+. If this regresses the postmortem MUST name the ONE "
        f"transformer whose terminal_reason ended the funnel; not "
        f"'another plumbing seam'."
    )

    # Plumbing contract 3 — persistence wrote qstate + trajectory under
    # the canonical paths ``<run_root>/iteration_<n>/qstate_<qid>.json``
    # and ``<run_root>/trajectories/trajectory_<qid>.json`` (see
    # ``state_machine/persistence.py``).
    qid_safe = fixture["qid"]
    qstate_files = list(tmp_path.glob(f"iteration_*/qstate_{qid_safe}.json"))
    trajectory_files = list(
        tmp_path.glob(f"trajectories/trajectory_{qid_safe}.json"),
    )
    assert qstate_files, (
        f"qstate JSON not persisted under {tmp_path}. The harness's "
        f"SM-first body relies on this for postmortem reconstruction."
    )
    assert trajectory_files, (
        f"trajectory JSON not persisted under {tmp_path}. Phase 7 trial "
        f"acceptance reads trajectories to compute deepest_stage_by_qid."
    )
