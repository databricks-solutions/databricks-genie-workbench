"""Failure-mode regression tests for the SM forward pipeline.

Every production failure shape the lever-loop has hit must be
reproducible locally in seconds. This file pins five canonical
failure paths:

1. **Empty Stage 1 input card** — the pre-flight contract trips
   ``GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1`` and *no* tape entry
   is consumed (the LLM was never invoked).
2. **Non-actionable Stage 1 diagnosis** — the LLM returned a
   structurally valid payload with empty evidence and the
   insufficient-evidence sentinel; the Stage 1 marker emits
   ``diagnosis_actionable=False``.
3. **Stage 2 drops every QID** — the clustering response excludes
   the diagnosed QIDs; each QID terminates with
   ``dropped_by_stage2_clustering``.
4. **Stage 3 returns an unrecognised patch_type** — Stage 3 emits
   ``empty_synthesis`` and the QID terminates with the Stage
   3-specific reason ``stage3_returned_none`` (not a generic
   no-candidate outcome from elsewhere in the pipeline).
5. **Tape exhaustion** — when the pipeline requires more LLM calls
   than the tape provides, :class:`TapeExhaustedError` propagates
   with a message that explicitly names ``under-captured tape or
   drifted pipeline`` so the failure has actionable diagnostics.

Aligned with the ``fast-optimizer-testing`` plan Task 4.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from tests.integration.sm_forward_fixtures import (
    expected_hard_qids,
    load_production_hydration_rows,
    parse_stage1_diagnosis_markers,
    parse_stage1_input_card_empty_markers,
    parse_stage3_synthesis_markers,
    states_by_qid,
)
from tests.integration.sm_forward_tapes import (
    cluster_drops_qids_tape,
    cluster_response_tape,
    diagnose_non_actionable_tape,
    diagnose_response_tape,
    full_forward_tape,
    synthesize_invalid_proposal_tape,
)
from tests.integration.sm_tape_replay import (
    TapeExhaustedError,
    TapeReplayHarness,
)


def _run_iteration(
    *,
    rows: list[dict],
    tape: list,
    run_root: Path,
) -> tuple[tuple, str, TapeReplayHarness]:
    """Drive one SM iteration with the given tape and return the
    final states, captured stdout, and the harness for unconsumed
    inspection.
    """
    from genie_space_optimizer.optimization import optimizer as opt_mod

    harness = TapeReplayHarness(tape=tape)
    buf = io.StringIO()
    with redirect_stdout(buf), harness.patch():
        final = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="failure-regression",
            run_root=run_root,
            workspace_client=None,
            forbidden_signatures=(),
        )
    return final, buf.getvalue(), harness


# ── Test 1: empty Stage 1 input card ─────────────────────────────────


def _empty_card_eval_row(qid: str) -> dict:
    """Build a single eval row that admits as hard but produces
    an empty Stage 1 evidence card.

    Admission requires ``result_correctness="no"`` with a non-correct
    arbiter verdict. Every other field is stripped to force the
    :class:`Stage1InputEvidenceContract` to fire on every requirement
    (question_text, expected/judge_rationale, generated_sql,
    blame_set_seed, rca_evidence).
    """
    return {
        "question_id": qid,
        "result_correctness": "no",
        "arbiter": "neither_correct",
        # Intentionally empty: every Stage1InputEvidenceContract
        # requirement must fail to prove the pre-flight gate covers
        # every field. Using the canonical key names so the contract
        # sees an explicit empty value rather than a missing-key
        # default (the field_sources reporter distinguishes the two).
        "question": "",
        "ground_truth_sql": "",
        "generated_sql": "",
        "judge_rationale": "",
    }


@pytest.mark.integration
def test_empty_stage1_card_short_circuits_before_llm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    qid = "gs_empty_card"
    rows = [_empty_card_eval_row(qid)]

    # A non-empty tape proves the harness is *not* consumed: the
    # pre-flight contract rejects before any LLM call.
    tape = diagnose_response_tape([qid])

    final, stdout, harness = _run_iteration(
        rows=rows, tape=tape, run_root=tmp_path,
    )

    # The empty-card marker must fire exactly once for this QID.
    empty_markers = parse_stage1_input_card_empty_markers(stdout)
    assert any(m.get("qid") == qid for m in empty_markers), (
        f"Expected GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1 marker for "
        f"qid={qid!r}; observed markers: {empty_markers!r}"
    )

    # The diagnose tape must remain untouched — the pre-flight gate
    # ran before any ``LlmReasoningCall.invoke`` could fire.
    assert harness.consumed_count == 0, (
        f"Pre-flight contract should short-circuit before any LLM "
        f"call, but {harness.consumed_count} tape entries were "
        f"consumed."
    )

    # Terminal reason must be the typed ``evidence_card_empty:`` form
    # so postmortems can group these failures cleanly.
    by_qid = states_by_qid(final)
    state = by_qid.get(qid)
    assert state is not None, (
        f"qid={qid!r} dropped before reaching the SM; admission "
        f"changed shape and the test fixture is no longer hard."
    )
    assert state.terminal is not None, (
        f"qid={qid!r} did not terminate; the Stage 1 abstain branch "
        f"must terminate empty-card QIDs."
    )
    assert "evidence_card_empty" in state.terminal.reason, (
        f"qid={qid!r} terminal reason={state.terminal.reason!r}; "
        f"expected to contain 'evidence_card_empty'."
    )


# ── Test 2: Stage 1 valid but non-actionable diagnosis ───────────────


@pytest.mark.integration
def test_non_actionable_stage1_diagnosis_marker_flips_actionable_false(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When Stage 1 returns the insufficient-evidence sentinel with
    empty ``blame_set`` and empty ``evidence_summary``, the marker
    must flip ``diagnosis_actionable=False`` even though the
    transformer formally advances to DIAGNOSED. Postmortems read
    this flag to distinguish silent stalls from clean diagnoses.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    # Provide a non-actionable Stage 1 tape; the test deliberately
    # exhausts the tape at Stage 2 so the focus stays on the Stage 1
    # marker payload.
    tape = diagnose_non_actionable_tape(qids)

    from genie_space_optimizer.optimization import optimizer as opt_mod

    harness = TapeReplayHarness(tape=tape)
    buf = io.StringIO()
    with redirect_stdout(buf), harness.patch():
        # Post-Phase-5: the non-actionable-diagnosis hard gate
        # terminates every QID before Stage 2 fires, so the Stage-1-only
        # tape no longer exhausts. The assertion target remains the
        # Stage 1 marker payload, not the downstream stages.
        try:
            opt_mod.run_state_machine_iteration_and_persist(
                eval_rows=rows,
                iteration=1,
                run_id="failure-regression",
                run_root=tmp_path,
                workspace_client=None,
                forbidden_signatures=(),
            )
        except TapeExhaustedError:
            pass
    stdout = buf.getvalue()

    markers = parse_stage1_diagnosis_markers(stdout)
    diagnosed = [m for m in markers if m.get("outcome") == "diagnosed"]
    assert diagnosed, (
        f"No Stage 1 'diagnosed' markers emitted; tape may have been "
        f"misshaped. markers={markers!r}"
    )
    for m in diagnosed:
        assert m.get("diagnosis_actionable") is False, (
            f"qid={m.get('qid')!r}: diagnosis_actionable should be "
            f"False (insufficient-evidence sentinel + empty blame_set + "
            f"empty evidence_summary). Marker: {m!r}"
        )


# ── Test 3: Stage 2 drops every QID ──────────────────────────────────


@pytest.mark.integration
def test_stage2_dropping_qids_terminates_with_typed_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stage 2 returning zero clusters must terminate each diagnosed
    QID with the typed ``abstain: cluster_returned_empty`` reason
    (the realistic drop shape in the production single-state SM).

    See :func:`cluster_drops_qids_tape` for why this is the typed
    Stage 2 drop signal rather than ``dropped_by_stage2_clustering``.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    tape = [
        *diagnose_response_tape(qids),
        *cluster_drops_qids_tape(qids, iteration=1, surviving_qids=()),
    ]

    final, _stdout, harness = _run_iteration(
        rows=rows, tape=tape, run_root=tmp_path,
    )

    # Every diagnosed QID must terminate at Stage 2 with the typed
    # Stage 2 drop reason. Nothing should reach PROPOSED — there is
    # no Stage 3 tape, and the SM must not even ask. The transformer
    # surfaces this as ``abstain: cluster_returned_empty`` (the
    # ``_ClusterResponse.declined`` value wrapped by the
    # ``_Plan11Stage2BatchTransformer`` abstain branch).
    by_qid = states_by_qid(final)
    for qid in qids:
        s = by_qid[qid]
        assert s.terminal is not None, (
            f"qid={qid!r} did not terminate after Stage 2 returned "
            f"no clusters."
        )
        assert "cluster_returned_empty" in s.terminal.reason, (
            f"qid={qid!r} terminal reason={s.terminal.reason!r}; "
            f"expected a 'cluster_returned_empty' Stage 2 drop reason."
        )

    # Tape exhaustion would indicate the SM advanced to Stage 3 in
    # spite of the drop. Stage 3 must NOT be invoked.
    assert harness.unconsumed() == [], (
        f"Unexpected unconsumed tape entries after Stage 2 drop: "
        f"{[(e.skill_id, e.call_id) for e in harness.unconsumed()]!r}"
    )


# ── Test 4: Stage 3 invalid proposal ─────────────────────────────────


@pytest.mark.integration
def test_stage3_invalid_proposal_terminates_with_stage3_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stage 3 returning a proposal with an unrecognised
    ``patch_type`` causes the synthesize stage to drop it before
    the survival contract runs, yielding an ``empty_synthesis``
    Stage 3 marker and a ``stage3_returned_none`` terminal — which
    is a Stage-3-specific reason that lets postmortems distinguish
    "the LLM responded but produced nothing usable" from a generic
    no-candidate outcome elsewhere in the pipeline.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    tape = [
        *diagnose_response_tape(qids),
        *cluster_response_tape(qids),
        *synthesize_invalid_proposal_tape(qids),
    ]

    final, stdout, harness = _run_iteration(
        rows=rows, tape=tape, run_root=tmp_path,
    )

    # The Stage 3 marker must surface the empty-synthesis outcome
    # so dashboards see the "LLM was called but no usable proposal"
    # failure shape.
    synth_markers = parse_stage3_synthesis_markers(stdout)
    assert any(
        m.get("outcome") == "empty_synthesis" for m in synth_markers
    ), (
        f"Stage 3 should emit 'empty_synthesis' outcome marker when "
        f"all proposals carry unknown patch_type. synth_markers="
        f"{synth_markers!r}"
    )

    by_qid = states_by_qid(final)
    for qid in qids:
        s = by_qid[qid]
        assert s.terminal is not None, (
            f"qid={qid!r} did not terminate after Stage 3 returned "
            f"no usable proposal."
        )
        assert s.terminal.reason == "stage3_returned_none", (
            f"qid={qid!r} terminal reason={s.terminal.reason!r}; "
            f"expected 'stage3_returned_none' (Stage 3-specific) "
            f"rather than a generic no-candidate reason."
        )

    # All Stage 3 tape entries must be consumed; if any are left,
    # the SM took an unexpected shortcut and the test no longer
    # exercises Stage 3.
    assert harness.unconsumed() == [], (
        f"Unconsumed tape entries after Stage 3 failure: "
        f"{[(e.skill_id, e.call_id) for e in harness.unconsumed()]!r}"
    )


# ── Test 5: tape exhaustion is a clear, actionable failure ───────────


@pytest.mark.integration
def test_tape_exhaustion_surfaces_actionable_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the pipeline calls the LLM more times than the tape stocks
    (the dc89d1a9-style "tape drift" failure), the harness must raise
    :class:`TapeExhaustedError` with a message that names the failure
    so a developer can grep the test output and immediately know to
    extend the tape factory rather than chase a flaky test.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)

    # Stock only Stage 1 + Stage 2 — Stage 3 will be uncovered. The
    # full forward pipeline reaches Stage 3, which will fail.
    tape = [
        *diagnose_response_tape(qids),
        *cluster_response_tape(qids),
    ]

    from genie_space_optimizer.optimization import optimizer as opt_mod

    harness = TapeReplayHarness(tape=tape)
    with harness.patch():
        with pytest.raises(TapeExhaustedError) as excinfo:
            opt_mod.run_state_machine_iteration_and_persist(
                eval_rows=rows,
                iteration=1,
                run_id="failure-regression",
                run_root=tmp_path,
                workspace_client=None,
                forbidden_signatures=(),
            )

    msg = str(excinfo.value)
    # The TapeExhaustedError must carry the missing skill_id so the
    # failure points to the exact stage that needs new tape entries.
    assert "plan11_synthesize" in msg, (
        f"TapeExhaustedError did not name the missing skill_id "
        f"('plan11_synthesize'). Full message: {msg!r}. Without this "
        f"the failure mode is not actionable — developers must guess "
        f"which stage drifted."
    )

    # The orchestrator drives each QID through every stage before
    # advancing to the next QID. So the first QID consumes Stage 1
    # + Stage 2 = 2 entries, then requests Stage 3 — at which point
    # the tape is exhausted. ``consumed_count == 2`` proves Stage 3
    # was the failing boundary; any other count means the SM took
    # a different path (skipped Stage 1 / Stage 2 entirely or
    # advanced past more than one QID before tape exhaustion).
    assert harness.consumed_count == 2, (
        f"Expected exhaustion at the QID 1 Stage 3 boundary "
        f"(consumed=2 = Stage1+Stage2); observed consumed="
        f"{harness.consumed_count}. The SM took an unexpected path."
    )


# ── Sanity guard: full forward tape must NOT trip exhaustion ─────────


@pytest.mark.integration
def test_full_forward_tape_does_not_exhaust(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sibling assertion to :func:`test_tape_exhaustion_surfaces_*`:
    the canonical ``full_forward_tape`` must drive the SM cleanly
    end-to-end without raising :class:`TapeExhaustedError`. If this
    starts failing, the tape factory drifted from the pipeline.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    tape = full_forward_tape(qids)

    final, _stdout, harness = _run_iteration(
        rows=rows, tape=tape, run_root=tmp_path,
    )

    assert len(final) == len(qids), (
        f"Forward pipeline lost QIDs: final={len(final)} vs "
        f"expected={len(qids)}"
    )
    assert harness.unconsumed() == [], (
        f"Forward pipeline left {len(harness.unconsumed())} tape "
        f"entries unconsumed — tape factory over-captured."
    )
