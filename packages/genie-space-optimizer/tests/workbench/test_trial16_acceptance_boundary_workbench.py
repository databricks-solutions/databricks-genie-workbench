"""Trial 16 Workbench v1.6 — close the acceptance-boundary loop locally.

Up to Trial 15 the workbench could drive a hard QID
``HARD_QID_SEEN → APPLIED`` but the post-apply eval was a no-op stub
(returned the baseline score) and ``ctx.post_apply_eval_rows`` was
empty, so ``acceptance_gate`` could never accept (target unchanged) and
its collateral assessment never had baseline ↔ post rows to compare.
The workbench v1.6 plan added:

  * ``WorkbenchInputBundle.post_apply_eval_tape`` — canned post-apply
    eval rows the workbench's stub joins to via the canonical
    ``extract_question_id`` helper.
  * ``ctx.post_apply_eval_rows`` populated from the same tape so
    ``acceptance_gate._assess_collateral`` can detect regressions.
  * Funnel-report ``accepted_count`` / ``rolled_back_count`` counters
    so operators see the acceptance boundary at a glance.
  * Marker pattern for ``applier_gate verdict=rejected`` because the
    RC3 termination path now emits it for every applier no-op.

This module covers the 4 scenarios the plan lists for the
``HARD_QID_SEEN → ACCEPTED`` pipeline. Trial 18
(``GSO_TRIAL18_ACCEPTANCE_OVERHAUL``, default-ON) reshaped the
no-behavioural-gain boundary: instead of rolling back, the gate keeps
the already-applied config live in the ``kept_insufficient`` lane
(funnel stage ACCEPTED, but not counted as a gain) and emits an
``insufficient_repair_signature``:
    1. Tape with target-fixed row → acceptance_gate ACCEPTS (true gain).
    2. Tape with target-unchanged row → kept_insufficient → ACCEPTED.
    3. Tape with a non-matching row ordered before the target row
       (pins canonical-QID join, not index-join).
    4. Empty tape → kept_insufficient → ACCEPTED (post == pre baseline).

Each scenario builds on the committed production-replay corpus so the
Stage 1/2/3 LLM tapes see real-shaped rows (question text, blame set,
rca evidence) but the runtime stays under a second.

These tests run in ``sm-tape`` mode with the Stage-1/2/3 forward tapes
already used by the workbench V1.5 acceptance suite, so they remain
hermetic — no Databricks workspace, no MLflow, no live LLM.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

import pytest

from local_lever_workbench.input_bundle import from_production_replay
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
    summarize_stage_progress,
)
from local_lever_workbench.models import (
    WorkbenchInputBundle,
    WorkbenchRunConfig,
)
from local_lever_workbench.report import _accepted_rolled_back_counts


# ── Fixture helpers ─────────────────────────────────────────────────


# The Trial 16 scenarios reuse the committed production-replay corpus
# as the base bundle (so the SM Stage-1/2/3 LLM tapes see a real-shaped
# row with question_text + blame_set + rca_evidence). Each scenario
# then layers a ``post_apply_eval_tape`` on top to drive the
# acceptance-boundary path. ``gs_009`` is the canonical hard qid from
# postmortem 98ec8950 with the LIMIT-vs-RANK pattern, used across the
# existing workbench v1.5 acceptance suite. The corpus loader splits
# filenames on ``__`` so we filter by short suffix; the loaded
# ``WorkbenchHardCase.qid`` carries the sanitized canonical form.
_REPLAY_QID_FILTER = "gs_009"
_REPLAY_QID = "domain_a_gs_009"
_SPACE_ID = "trial16-workbench-space"


def _post_row(qid: str, *, correctness: float, sql: str = "") -> dict:
    """Post-apply row shape mirroring the production MLflow-flattened
    eval row so ``ctx.post_apply_eval_rows`` + the workbench stub
    consume it identically to a live eval result.
    """
    return {
        "question_id": qid,
        "inputs/question_id": qid,
        "generated_sql": sql or f"SELECT 1 -- {qid}",
        "feedback/result_correctness/value": correctness,
        "eval_row_id": f"workbench-trial16-{qid}",
    }


def _bundle_from_replay(
    *,
    qids: tuple[str, ...] = (_REPLAY_QID_FILTER,),
    post_apply_eval_tape: tuple[Mapping[str, object], ...] = (),
) -> WorkbenchInputBundle:
    """Build a bundle by re-using the committed production-replay
    corpus and layering a ``post_apply_eval_tape`` on top.

    The replay corpus carries production-shaped rows (question_text,
    blame_set, rca_evidence, judge_rationale) so Stage 1 admits them
    without firing ``evidence_card_empty``. The tape is the new
    Trial 16 v1.6 carrier that drives ``acceptance_gate`` past the
    target-fixed / collateral-regression / target-unchanged boundary.
    """
    base = from_production_replay(qids=qids)
    return WorkbenchInputBundle(
        provenance=base.provenance,
        space_id=base.space_id,
        hard_cases=base.hard_cases,
        metadata_snapshot=base.metadata_snapshot,
        post_apply_eval_tape=post_apply_eval_tape,
    )


# Stage 1 + 2 + 3 forward tapes already used in the V1.5 acceptance
# suite — re-importing keeps this module hermetic.
def _serialize_tape(entries: Iterable, path: Path) -> Path:
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "kind": e.kind,
                    "skill_id": e.skill_id,
                    "call_id": e.call_id,
                    "iteration": e.iteration,
                    "qid": e.qid,
                    "parsed_output": e.parsed_output,
                    "raw_text": e.raw_text,
                    "tokens_input": e.tokens_input,
                    "tokens_output": e.tokens_output,
                    "duration_ms": e.duration_ms,
                    "exception_class": e.exception_class,
                    "exception_message": e.exception_message,
                }
            )
            for e in entries
        )
    )
    return path


def _full_pipeline_tape(qids: tuple[str, ...], tmp_path: Path) -> Path:
    """Build a forward tape covering Stage 1 / 2 / 3 for ``qids``.
    Tapes carry 5 entries per stage so iteration retries do not run
    out of pre-canned responses.
    """
    from tests.integration.sm_forward_tapes import (
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )
    tape: list = []
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)
    return _serialize_tape(tape, tmp_path / "forward.jsonl")


def _run_workbench(
    bundle: WorkbenchInputBundle, tmp_path: Path,
) -> tuple:
    """Run one workbench iteration in sm-tape mode and return
    ``(artifacts, progress)``. Caller asserts on these."""
    tape_path = _full_pipeline_tape(bundle.hard_qids, tmp_path)
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "bundle.json",
        output_dir=tmp_path / "out",
        llm_mode=LLM_MODE_TAPE,
        apply_mode="fake-record",
        tape_path=tape_path,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)
    progress = summarize_stage_progress(artifacts)
    return artifacts, progress


def _accepted_for(artifacts, qid: str):
    """Return the ``AcceptanceDecisionRecord`` for ``qid`` from the SM
    final states, or ``None`` if the qid never reached the gate. The
    Trial 18 ``kept_insufficient`` decision (and its
    ``insufficient_repair_signature``) is only carried on this record —
    ``StageProgress`` exposes deepest_stage/terminal_reason only.
    """
    for s in artifacts.final_states:
        if str(s.qid) == qid:
            return getattr(s, "accepted", None)
    return None


# ── Scenarios ───────────────────────────────────────────────────────


@pytest.mark.workbench
@pytest.mark.integration
def test_workbench_tape_with_target_fixed_row_reaches_accepted(
    tmp_path: Path,
) -> None:
    """Scenario 1 — tape says the target qid is fixed post-apply
    (1.0 vs 0.0 baseline) and there are no collateral rows, so
    ``acceptance_gate`` accepts and the state lands at ACCEPTED.

    This is the production-equivalent green path: synthesize → apply
    → eval → accept. Up to Trial 15 this was unreachable from the
    workbench because the stub always returned the baseline score.
    """
    bundle = _bundle_from_replay(
        post_apply_eval_tape=(
            _post_row(_REPLAY_QID, correctness=1.0, sql="SELECT POST -- fixed"),
        ),
    )
    artifacts, progress = _run_workbench(bundle, tmp_path)

    deepest = {p.qid: p.deepest_stage for p in progress}
    # The hard qid must reach a stage past APPLIED. ``accepted`` is the
    # green path; ``terminated`` with ``target_unchanged`` would mean
    # the gate fired but didn't take the tape's post-score. We assert
    # the green path here; the rolled-back scenarios cover the other.
    assert deepest.get(_REPLAY_QID) == "accepted", (
        f"qid {_REPLAY_QID!r} should reach ACCEPTED with target_fixed "
        f"tape, got deepest_stage={deepest!r}, terminal_reasons="
        f"{[p.terminal_reason for p in progress]!r}, stdout tail:\n"
        f"{artifacts.stdout_text.splitlines()[-10:]}"
    )
    accepted, _ = _accepted_rolled_back_counts(progress)
    assert accepted >= 1


@pytest.mark.workbench
@pytest.mark.integration
def test_workbench_tape_with_target_unchanged_row_rolls_back(
    tmp_path: Path,
) -> None:
    """Scenario 2 — tape returns the same score the baseline had
    (0.0 → 0.0).

    Up to Trial 17 ``acceptance_gate`` rolled this back with
    ``target_unchanged: post_score <= pre_score``. Trial 18
    (``GSO_TRIAL18_ACCEPTANCE_OVERHAUL``, default-ON) supersedes that:
    a patch that lands cleanly but shows no behavioural gain is kept
    live in the ``kept_insufficient`` lane (funnel stage ACCEPTED, but
    NOT counted as a win) and the gate emits an
    ``insufficient_repair_signature`` carrying the pivot tokens the
    strategist consumes next iteration.

    Production analog: applier deployed the patch but the eval did not
    improve — the config stays live and the strategist is told to
    pivot rather than re-try the same lever.
    """
    bundle = _bundle_from_replay(
        post_apply_eval_tape=(
            _post_row(
                _REPLAY_QID, correctness=0.0,
                sql="SELECT POST -- unchanged",
            ),
        ),
    )
    artifacts, progress = _run_workbench(bundle, tmp_path)

    deepest = {p.qid: p.deepest_stage for p in progress}
    assert deepest.get(_REPLAY_QID) == "accepted", (
        f"Trial 18 keeps the applied config live in the kept_insufficient "
        f"lane (ACCEPTED); got deepest_stage={deepest.get(_REPLAY_QID)!r}, "
        f"terminal_reason="
        f"{next((p.terminal_reason for p in progress if p.qid == _REPLAY_QID), '')!r}"
    )
    acc = _accepted_for(artifacts, _REPLAY_QID)
    assert acc is not None and acc.decision == "kept_insufficient", (
        f"expected kept_insufficient decision, got {acc!r}"
    )
    sig = acc.insufficient_repair_signature or ""
    assert "insufficient" in sig, f"sig missing insufficient token: {sig!r}"
    assert "lever-" in sig, f"sig missing lever token: {sig!r}"
    # kept_insufficient is ACCEPTED-not-a-gain: the report's accepted
    # counter ticks; it is no longer surfaced as a rollback.
    accepted, _ = _accepted_rolled_back_counts(progress)
    assert accepted >= 1


@pytest.mark.workbench
@pytest.mark.integration
def test_workbench_tape_joins_by_canonical_qid_not_index(
    tmp_path: Path,
) -> None:
    """Scenario 3 — the tape carries two rows in arbitrary order,
    and the workbench must join them to the target QID by canonical
    QID via :func:`extract_question_id`, not by tape index. This
    pins the RC2-class fix (canonical QID lookup) at the workbench
    boundary so we don't regress the join key on future refactors.

    Setup: tape order is [non-matching-qid, target-qid]. The
    workbench stub + ``ctx.post_apply_eval_rows`` must surface the
    target-qid row to ``acceptance_gate``, which we observe via the
    typed predicate-inputs marker (``post_apply_score``).
    """
    target_qid = _REPLAY_QID

    bundle = _bundle_from_replay(
        post_apply_eval_tape=(
            # Non-matching row first — if the workbench joined by
            # index this would shadow the real target row.
            _post_row("never_matches_xyz", correctness=0.5),
            _post_row(target_qid, correctness=1.0, sql="SELECT POST"),
        ),
    )

    artifacts, progress = _run_workbench(bundle, tmp_path)

    # If the workbench joined by tape index, the target qid would
    # pick up the non-matching row's score (0.5) and ``target_unchanged``
    # would fire (post=0.5 ≤ pre rebuilt from same row = 1.0 ⇒ reject,
    # or in any case not "accepted"). Canonical-QID join routes the
    # 1.0 tape row to the target ⇒ acceptance_gate sees
    # target_fixed=True ⇒ deepest_stage=accepted.
    deepest = {p.qid: p.deepest_stage for p in progress}
    assert deepest.get(target_qid) == "accepted", (
        f"workbench must join tape rows by canonical QID (not by "
        f"index); expected qid {target_qid!r} to reach ACCEPTED via "
        f"the matching 1.0 tape row, got deepest_stage="
        f"{deepest.get(target_qid)!r}, terminal_reason="
        f"{next((p.terminal_reason for p in progress if p.qid == target_qid), '')!r}"
    )


@pytest.mark.workbench
@pytest.mark.integration
def test_workbench_with_empty_tape_falls_back_to_target_unchanged(
    tmp_path: Path,
) -> None:
    """Scenario 4 — bundle has no ``post_apply_eval_tape``; the
    workbench stub returns the baseline score and
    ``ctx.post_apply_eval_rows`` is empty (post == pre).

    Up to Trial 17 this rolled back with ``target_unchanged:``. Under
    Trial 18 (``GSO_TRIAL18_ACCEPTANCE_OVERHAUL``, default-ON) the
    empty-tape / no-behavioural-delta case is the canonical
    ``kept_insufficient`` lane: the already-applied config is kept live
    (ACCEPTED, not a gain) and the gate emits an
    ``insufficient_repair_signature`` with ``behavior=unchanged``.
    """
    bundle = _bundle_from_replay(
        post_apply_eval_tape=(),
    )
    artifacts, progress = _run_workbench(bundle, tmp_path)

    deepest = {p.qid: p.deepest_stage for p in progress}
    assert _REPLAY_QID in deepest
    assert deepest.get(_REPLAY_QID) == "accepted", (
        f"Trial 18 keeps the empty-tape config live in the "
        f"kept_insufficient lane (ACCEPTED); got "
        f"deepest_stage={deepest.get(_REPLAY_QID)!r}, "
        f"terminal_reason="
        f"{next((p.terminal_reason for p in progress if p.qid == _REPLAY_QID), '')!r}"
    )
    acc = _accepted_for(artifacts, _REPLAY_QID)
    assert acc is not None and acc.decision == "kept_insufficient", (
        f"expected kept_insufficient decision, got {acc!r}"
    )
    sig = acc.insufficient_repair_signature or ""
    assert "insufficient" in sig, f"sig missing insufficient token: {sig!r}"
    accepted, _ = _accepted_rolled_back_counts(progress)
    assert accepted >= 1
