"""Phase 5 — Stage 3 ``empty_synthesis`` as a typed terminal contract.

Trial 12 (run ``dc89d1a9-...``) shadow Stage 3 distribution::

    plan11_stage3_outcomes: {empty_synthesis: 6}

Every actionable Stage 2 cluster reached Stage 3 and the LLM returned
``{"proposals": []}`` — a structurally valid envelope with zero
proposals. There is no downstream signal: no ``GSO_PATCH_OUTCOME_V1``
line is emitted because no patch was attempted; the SM completes the
iteration; the postmortem has nothing to attribute the zero-applied
result to. This is the load-bearing silent-decline mode the harness
has been blind to.

Two layers:

* :func:`test_empty_synthesis_emits_today_stage3_synthesis_marker` —
  pins the marker shape that is emitted today (``outcome="empty_synthesis"``).
  Passes today, guards against accidental marker removal.
* :func:`test_empty_synthesis_should_terminate_with_typed_no_candidate_marker`
  — pins the target contract: every actionable cluster + empty Stage 3
  must terminate with a typed ``stage3_silent_decline`` reason AND
  emit a ``GSO_PATCH_OUTCOME_V1`` line with ``outcome=
  "stage3_silent_decline"`` (or a sibling no-candidate string). Fails
  today (XFAIL strict); the follow-up PR removes the marker once the
  Stage 3 transformer routes empty proposal lists through the typed
  terminator path.
* :func:`test_empty_synthesis_should_carry_typed_archetype_coverage_reason`
  — pins the next-most-likely Trial 13 surprise: Stage 3 may be reached
  and still return zero proposals because no repair archetype applies.
  The synthesis marker must name that reason and preserve cluster/QID
  coverage instead of emitting an untyped ``empty_synthesis`` shell.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from tests.integration.sm_forward_fixtures import (
    expected_hard_qids,
    load_production_hydration_rows,
    parse_patch_outcome_markers,
    parse_stage3_synthesis_markers,
    states_by_qid,
)
from tests.integration.sm_forward_tapes import (
    cluster_response_tape,
    diagnose_response_tape,
    synthesize_empty_synthesis_for_actionable_cluster_tape,
)
from tests.integration.sm_tape_replay import (
    TapeExhaustedError,
    TapeReplayHarness,
)

_ACCEPTED_SYNTHESIS_EMPTY_REASONS = {
    "no_applicable_archetype",
    "all_candidates_unsafe",
    "prompt_constraint_collision",
    "parse_returned_zero",
}


def _rows_with_hard_verdicts(rows: list[dict]) -> list[dict]:
    """Ensure this Stage 3 replay enters the SM hard-QID lane.

    The shared shape-ladder fixture has changed over time and can omit
    result verdicts while still carrying ``_expected_hard`` metadata.
    Stage 3 tests are about synthesis, not admission, so stamp the
    expected-hard rows locally instead of depending on fixture minutiae.
    """
    normalized: list[dict] = []
    for row in rows:
        copy = dict(row)
        if copy.get("_expected_hard"):
            copy.setdefault("result_correctness", "no")
            copy.setdefault("arbiter", "ground_truth_correct")
        normalized.append(copy)
    return normalized


def _run_with_actionable_upstream_and_empty_stage3(
    *,
    rows: list[dict],
    qids: tuple[str, ...],
    run_root: Path,
) -> tuple[str, tuple]:
    """Drive one SM iteration: actionable Stage 1+2, empty Stage 3.

    Returns the captured stdout and the tuple of final
    ``QuestionStateInIteration`` instances. Tape exhaustion is
    swallowed so the assertion in the caller can describe what went
    wrong without secondary failure noise.
    """
    tape = [
        *diagnose_response_tape(qids),
        *cluster_response_tape(qids),
        *synthesize_empty_synthesis_for_actionable_cluster_tape(qids),
    ]
    harness = TapeReplayHarness(tape=tape)

    from genie_space_optimizer.optimization import optimizer as opt_mod

    buf = io.StringIO()
    final_states: tuple = ()
    with redirect_stdout(buf), harness.patch():
        try:
            final_states = opt_mod.run_state_machine_iteration_and_persist(
                eval_rows=_rows_with_hard_verdicts(rows),
                iteration=1,
                run_id="stage3-empty-synthesis-gate",
                run_root=run_root,
                workspace_client=None,
                forbidden_signatures=(),
            )
        except TapeExhaustedError:
            pass
    return buf.getvalue(), final_states


# ── Today's contract (passes today) ───────────────────────────────────


@pytest.mark.integration
def test_empty_synthesis_emits_today_stage3_synthesis_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stage 3 must emit a ``GSO_PLAN11_STAGE3_SYNTHESIS_V1`` marker
    with ``outcome="empty_synthesis"`` (or equivalent) when the LLM
    returns ``{"proposals": []}``.

    The marker is the only signal postmortems have today to attribute
    a zero-patch run to Stage 3 rather than to an upstream stage. The
    test pins the marker so a future "fix" can't accidentally strip it
    while reshaping the transformer.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    stdout, _ = _run_with_actionable_upstream_and_empty_stage3(
        rows=rows, qids=qids, run_root=tmp_path
    )

    stage3 = parse_stage3_synthesis_markers(stdout)
    assert stage3, (
        "Stage 3 emitted no synthesis marker — the SM probably never "
        "reached Stage 3. Inspect upstream tape entries."
    )
    # Allow either ``empty_synthesis`` (the production label) or
    # ``stage3_returned_none`` (the legacy in-process label) — both
    # are acceptable for *this* test (the marker exists at all);
    # the target-contract test below pins the typed reason precisely.
    accepted_outcomes = {"empty_synthesis", "stage3_returned_none"}
    bad: list[dict] = []
    for marker in stage3:
        if marker.get("outcome") not in accepted_outcomes:
            bad.append(marker)
    assert not bad, (
        f"Stage 3 markers carried unexpected outcomes for the "
        f"empty-proposals tape:\n  {bad!r}\n"
        f"Expected outcome in {sorted(accepted_outcomes)}."
    )


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Stage 3 empty_synthesis markers do not yet carry "
        "synthesis_empty_reason or target_qids_union coverage when "
        "proposals=[]. Trial 13 Phase 7 requires a typed reason from "
        "{no_applicable_archetype, all_candidates_unsafe, "
        "prompt_constraint_collision, parse_returned_zero} so the next "
        "run cannot rediscover an un-attributed archetype gap in "
        "production."
    ),
)
def test_empty_synthesis_should_carry_typed_archetype_coverage_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Target contract — empty synthesis must name *why* no proposal was
    emitted and must still identify the covered cluster/QIDs.

    The likely post-Stage-1 surprise is not "we never reached Stage 3";
    it is "Stage 3 had an actionable cluster but no repair archetype
    fit." That must surface as ``synthesis_empty_reason=
    "no_applicable_archetype"`` (or another closed-vocabulary sibling),
    with QID coverage preserved for postmortem attribution.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    stdout, _ = _run_with_actionable_upstream_and_empty_stage3(
        rows=rows, qids=qids, run_root=tmp_path
    )

    stage3 = parse_stage3_synthesis_markers(stdout)
    empty_markers = [
        marker for marker in stage3 if marker.get("outcome") == "empty_synthesis"
    ]
    assert empty_markers, (
        "Expected at least one Stage 3 empty_synthesis marker from the "
        "empty-proposals tape; upstream stages may not have reached synthesis."
    )

    qid_set = set(qids)
    bad_reasons: list[dict] = []
    bad_coverage: list[dict] = []
    for marker in empty_markers:
        reason = str(marker.get("synthesis_empty_reason") or "")
        if reason not in _ACCEPTED_SYNTHESIS_EMPTY_REASONS:
            bad_reasons.append(marker)
        covered = {str(q) for q in (marker.get("target_qids_union") or [])}
        if covered != qid_set:
            bad_coverage.append(marker)

    assert not bad_reasons and not bad_coverage, (
        "Stage 3 empty_synthesis marker lacks typed archetype coverage.\n"
        f"  accepted reasons : {sorted(_ACCEPTED_SYNTHESIS_EMPTY_REASONS)}\n"
        f"  markers with bad reason: {bad_reasons!r}\n"
        f"  markers with bad QID coverage: {bad_coverage!r}\n"
        f"  expected QIDs: {sorted(qid_set)}"
    )


# ── Target contract (XFAIL today; flips green when typed terminal lands) ──


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "No Stage 3 'silent-decline' terminator + no_candidate "
        "GSO_PATCH_OUTCOME_V1 emission today. Trial 12 saw 6/6 "
        "actionable clusters land in empty_synthesis with zero "
        "GSO_PATCH_OUTCOME_V1 markers and no typed terminal — the "
        "iteration completed and the postmortem had nothing to "
        "attribute. The follow-up PR must route empty proposal "
        "lists through a typed terminator (reason="
        "'stage3_silent_decline') and emit a GSO_PATCH_OUTCOME_V1 "
        "line with outcome='stage3_silent_decline'. Remove this "
        "xfail marker once the routing lands."
    ),
)
def test_empty_synthesis_should_terminate_with_typed_no_candidate_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Target contract — Stage 3 returning ``{"proposals": []}`` must
    terminate every cluster member with a typed no-candidate reason
    AND emit a ``GSO_PATCH_OUTCOME_V1`` marker carrying the same
    string so postmortems can rank Stage 3 silent declines against
    every other Stage 3 outcome.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    stdout, final_states = _run_with_actionable_upstream_and_empty_stage3(
        rows=rows, qids=qids, run_root=tmp_path
    )

    if not final_states:
        raise AssertionError(
            "Tape exhausted before final_states populated; the SM "
            "never reached Stage 3 termination. The contract cannot "
            "be evaluated until the upstream tape covers Stage 1+2."
        )

    accepted_terminal_fragments = (
        "stage3_silent_decline",
        "empty_synthesis",
    )
    by_qid = states_by_qid(final_states)
    silent_advances: list[str] = []
    wrong_reason: list[tuple[str, str]] = []
    for qid in qids:
        state = by_qid.get(qid)
        assert state is not None, f"qid={qid!r} dropped from final states"
        terminal = state.terminal
        if terminal is None:
            silent_advances.append(qid)
            continue
        if not any(
            fragment in (terminal.reason or "")
            for fragment in accepted_terminal_fragments
        ):
            wrong_reason.append((qid, terminal.reason or "<empty>"))

    outcomes = parse_patch_outcome_markers(stdout)
    outcome_strings = {m.get("outcome") for m in outcomes}
    missing_outcome_marker = not (
        outcome_strings
        & {"stage3_silent_decline", "empty_synthesis", "no_candidates"}
    )

    assert not silent_advances and not wrong_reason and not missing_outcome_marker, (
        "Stage 3 empty-synthesis contract violated.\n"
        f"  QIDs with no terminal record (silent advance): "
        f"{silent_advances}\n"
        f"  QIDs with wrong terminal reason: {wrong_reason}\n"
        f"  GSO_PATCH_OUTCOME_V1 outcomes seen: "
        f"{sorted(s for s in outcome_strings if s)}\n"
        "Expected every QID to terminate with a Stage 3 silent-decline "
        "reason AND a matching GSO_PATCH_OUTCOME_V1 emission."
    )
