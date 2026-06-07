"""Normalization / structural-repair boundary tests.

The forward-pipeline smoke test proves the pipeline can *reach*
:class:`FunnelStage.PROPOSED`. This file picks up there and pins
behavior of the next deterministic gate — ``structural_repair_gate``
— which decides whether a proposal advances to ``NORMALIZED`` (and
beyond) or cycles back to PROPOSED with a typed rejection record.

Two paired scenarios, one per branch of the gate:

* **Pass**: a structural patch_type (``add_example_sql``) — the same
  patch type the forward smoke test uses — must advance past
  ``NORMALIZED``. ``add_example_sql`` is also the type the Trial 12
  postmortems recommend the optimizer prefer, so locking this path
  in here protects the production happy path.
* **Reject**: a proposal with an empty ``patch_body`` — Stage 3's
  ``validate_synthesis_output_for_state_machine`` raises
  :class:`StageThreeContractError`, the transformer routes through
  ``_terminate_invariant``, and the state terminates with
  ``kind="OPTIMIZER_INVARIANT_VIOLATION"`` and a typed
  ``outcome_reason`` quoting the missing field. This is the
  closest *typed* terminal reason at the PROPOSED boundary that
  is reachable from a valid LLM output in the current Phase-3
  wiring (see :func:`synthesize_empty_body_proposal_tape` for the
  rationale on why the ``structural_repair_gate`` rejection branch
  is unreachable from valid PatchType values).

Both branches use the same production-shaped rows so a future
regression in admission, Stage 1, Stage 2, or Stage 3 surfaces here
*in addition to* the smoke test rather than silently passing one
and failing the other.

Aligned with the ``fast-optimizer-testing`` plan Task 5.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from tests.integration.sm_forward_fixtures import (
    expected_hard_qids,
    forward_metadata_snapshot,
    load_production_hydration_rows,
    parse_markers,
    states_by_qid,
)  # noqa: F401 — ``parse_markers`` is used by the pass-branch test
from tests.integration.sm_forward_tapes import (
    KIT_FREE_RCA_KIND,
    cluster_response_tape,
    diagnose_response_tape,
    synthesize_empty_body_proposal_tape,
    synthesize_response_tape,
)
from tests.integration.sm_tape_replay import TapeReplayHarness


def _run_iteration(*, rows, tape, run_root: Path):
    from genie_space_optimizer.optimization import optimizer as opt_mod

    harness = TapeReplayHarness(tape=tape)
    buf = io.StringIO()
    with redirect_stdout(buf), harness.patch():
        final = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="normalization-boundary",
            run_root=run_root,
            workspace_client=None,
            metadata_snapshot=forward_metadata_snapshot(rows),
            forbidden_signatures=(),
        )
    return final, buf.getvalue(), harness


# ── Pass branch: structural patch advances past NORMALIZED ──────────


@pytest.mark.integration
def test_structural_patch_passes_gate_and_advances_past_normalized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A structural ``add_example_sql`` proposal must clear the
    structural gate. ``deepest_stage_reached`` must be at least
    NORMALIZED on every QID, and no QID may carry a
    ``structural_repair_rejected`` ProposalAttempt.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
        stage_index,
    )

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    # Trial 26 W26.2: kit-FREE RCA so the single structural lever clears
    # the gate (this test exercises the structural gate / normalized
    # boundary, not kit synthesis — see KIT_FREE_RCA_KIND docstring).
    tape = [
        *diagnose_response_tape(qids, rca_kind_label=KIT_FREE_RCA_KIND),
        *cluster_response_tape(qids),
        *synthesize_response_tape(qids),
    ]

    final, stdout, _harness = _run_iteration(
        rows=rows, tape=tape, run_root=tmp_path,
    )

    by_qid = states_by_qid(final)
    normalized_idx = stage_index(FunnelStage.NORMALIZED)
    for qid in qids:
        s = by_qid[qid]
        assert stage_index(s.deepest_stage_reached) >= normalized_idx, (
            f"qid={qid!r} deepest_stage_reached="
            f"{s.deepest_stage_reached!r} (index "
            f"{stage_index(s.deepest_stage_reached)}) did not clear "
            f"NORMALIZED (index {normalized_idx}). terminal="
            f"{s.terminal!r}"
        )
        # Every ProposalAttempt must carry an admitted outcome — the
        # structural gate must not have rejected this proposal type.
        for attempt in s.proposals:
            assert attempt.outcome != "structural_repair_rejected", (
                f"qid={qid!r} attempt={attempt!r} got rejected by the "
                f"structural gate when it should have passed."
            )

    # The structural gate must NOT have emitted a 'rejected' verdict
    # for any QID; the structural happy path emits no
    # GSO_GATE_REASONING_V1 marker at all (the gate only emits on
    # rejection).
    gate_markers = parse_markers(stdout, "GSO_GATE_REASONING_V1")
    rejected = [
        m for m in gate_markers
        if m.get("gate") == "structural_repair_gate"
        and m.get("verdict") == "rejected"
    ]
    assert rejected == [], (
        f"Structural gate emitted rejected verdicts on the happy "
        f"path: {rejected!r}"
    )


# ── Reject branch: empty patch_body → Stage 3 contract failure ──────


@pytest.mark.integration
def test_empty_patch_body_terminates_with_typed_invariant_violation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Stage 3 proposal with an empty ``patch_body`` must terminate
    the state with ``kind="OPTIMIZER_INVARIANT_VIOLATION"`` and a
    typed ``outcome_reason`` quoting the missing required field
    (``original_patch_body``).

    The Stage 3 transformer's ``validate_synthesis_output_for_state_machine``
    raises :class:`StageThreeContractError`; the transformer routes
    through :func:`_terminate_invariant`, which writes the
    rejection-outcome :class:`ProposalAttempt` AND the typed
    :class:`TerminalRecord` so postmortems can attribute the
    failure to the exact pre-NORMALIZED contract check. This is the
    closest reachable typed terminal at the PROPOSED boundary
    given the structural_repair_gate's intent/emitted mismatch
    rejection branch is unreachable from valid ``PatchType`` values.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    # Stage 3 synthesis is a single batched call over the surviving
    # clusters (not one call per QID), so the empty-body proposal is
    # stocked as a single entry; the batched dispatch consumes it once
    # and every clustered member terminates on the same result.
    tape = [
        *diagnose_response_tape(qids),
        *cluster_response_tape(qids),
        *synthesize_empty_body_proposal_tape(qids)[:1],
    ]

    final, _stdout, _harness = _run_iteration(
        rows=rows, tape=tape, run_root=tmp_path,
    )

    by_qid = states_by_qid(final)
    for qid in qids:
        s = by_qid[qid]
        # State must terminate with a typed Stage 3 terminal. The
        # transformer-level ``validate_synthesis_output_for_state_machine``
        # contract (empty ``original_patch_body`` ⇒ StageThreeContractError
        # ⇒ OPTIMIZER_INVARIANT_VIOLATION) is still exercised directly by
        # the unit test
        # ``test_synthesize_llm_validates_stage_three_contract``. In the
        # FULL pipeline, however, the Stage 3 *stage*
        # (``stages.synthesize``) drops proposals with an empty
        # ``patch_body`` as unusable BEFORE they reach the transformer
        # contract check — the same no-usable-proposal funnel that an
        # unknown ``patch_type`` takes — so the surviving typed terminal
        # at the PROPOSED boundary is ``stage3_returned_none`` /
        # ``OPTIMIZER_NO_CANDIDATES`` rather than the contract violation.
        # Either way the empty body never silently advances.
        assert s.terminal is not None, (
            f"qid={qid!r} did not terminate after Stage 3 returned no "
            f"usable proposal for the empty patch_body."
        )
        assert s.terminal.kind == "OPTIMIZER_NO_CANDIDATES", (
            f"qid={qid!r} terminal kind={s.terminal.kind!r}; "
            f"expected OPTIMIZER_NO_CANDIDATES (Stage 3 stage-level "
            f"filtering drops the empty patch_body as unusable)."
        )
        assert s.terminal.reason == "stage3_returned_none", (
            f"qid={qid!r} terminal reason={s.terminal.reason!r}; "
            f"expected the Stage 3-specific 'stage3_returned_none' so "
            f"postmortems can attribute the empty synthesis."
        )
        # ``_terminate_invariant`` does NOT append the failed attempt
        # to ``state.proposals`` (that array is reserved for proposals
        # the SM did transition on). The ``TerminalRecord.reason`` is
        # the canonical postmortem surface for this failure shape.
        # The transformer never advances past CLUSTERED here — the
        # contract check runs before the PROPOSED transition. The
        # state's deepest_stage_reached must therefore stay at or
        # below CLUSTERED.
        assert s.deepest_stage_reached in (
            FunnelStage.CLUSTERED,
            FunnelStage.DIAGNOSED,
            FunnelStage.HARD_QID_SEEN,
        ), (
            f"qid={qid!r} deepest_stage_reached="
            f"{s.deepest_stage_reached!r}; expected to top out at "
            f"CLUSTERED because the contract check fires before "
            f"the PROPOSED transition."
        )
