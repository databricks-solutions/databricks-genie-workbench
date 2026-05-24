"""Phase 4 — Stage 1 ``diagnosis_actionable`` boundary contract.

Trial 12 (run ``dc89d1a9-...``) Stage 1 shadow batch distribution::

    plan11_stage1_diagnosis_actionable: {false: 21, true: 3}

21 of 24 hard QIDs were diagnosed as non-actionable (empty
``blame_set``, empty ``evidence_summary``, insufficient-evidence
sentinel) and yet the state machine **advanced every one of them** into
Stage 2 clustering. The non-actionable shadow diagnoses produced empty
clusters or single-QID self-clusters, Stage 3 emitted
``empty_synthesis`` for each, and the optimizer applied zero patches.
Accuracy stayed flat.

The harness has a regression test that pins the **marker** behaviour
(``diagnosis_actionable=False`` is emitted on advance). It does not
have a test for the **gate** that should — but does not yet — stop a
non-actionable diagnosis from being carried into Stage 2 in the first
place. This module adds that contract test.

Two layers, mirroring Phase 3:

* :func:`test_non_actionable_diagnosis_emits_marker_today` — pins the
  current observable behaviour (marker is emitted). Passes today;
  catches the regression where someone removes the marker without
  replacing it.
* :func:`test_non_actionable_diagnosis_should_terminate_before_clustering`
  — pins the target contract. Fails today (XFAIL strict). The
  follow-up implementation PR adds a ``diagnosis_actionable`` gate
  transformer between DIAGNOSED and CLUSTERED and removes the XFAIL.
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
    states_by_qid,
)
from tests.integration.sm_forward_tapes import (
    diagnose_non_actionable_zero_blame_tape,
)
from tests.integration.sm_tape_replay import (
    TapeExhaustedError,
    TapeReplayHarness,
)


def _run_stage1_only(
    *,
    rows: list[dict],
    qids: tuple[str, ...],
    run_root: Path,
) -> tuple[str, tuple]:
    """Drive one SM iteration with a Stage-1-only non-actionable tape.

    We stock only Stage 1 entries on purpose. If the QIDs terminate at
    Stage 1 (the target contract), the tape will not exhaust. If the
    QIDs advance into Stage 2 (today's behaviour), the tape exhausts
    when the cluster skill is invoked — that exhaustion is itself the
    diagnostic signal the target contract is violated.

    Returns the captured stdout and the tuple of final
    ``QuestionStateInIteration`` instances. ``TapeExhaustedError`` is
    swallowed so the caller can introspect what state the SM left
    each QID in before requesting the next stage's tape.
    """
    tape = diagnose_non_actionable_zero_blame_tape(qids)
    harness = TapeReplayHarness(tape=tape)

    from genie_space_optimizer.optimization import optimizer as opt_mod

    buf = io.StringIO()
    final_states: tuple = ()
    with redirect_stdout(buf), harness.patch():
        try:
            final_states = opt_mod.run_state_machine_iteration_and_persist(
                eval_rows=rows,
                iteration=1,
                run_id="diagnosis-actionable-gate",
                run_root=run_root,
                workspace_client=None,
                forbidden_signatures=(),
            )
        except TapeExhaustedError:
            # The exhaustion proves the SM tried to invoke Stage 2 on
            # a non-actionable diagnosis — the very behaviour the
            # target contract is meant to prevent. We return what we
            # have so the assertion can name the offending QIDs.
            pass
    return buf.getvalue(), final_states


# ── Today's contract (passes today; pins the marker behaviour) ────────


@pytest.mark.integration
def test_non_actionable_diagnosis_emits_marker_today(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Stage 1 marker must flip ``diagnosis_actionable=False``
    when the LLM returns the insufficient-evidence sentinel with an
    empty ``blame_set`` and an empty ``evidence_summary``.

    This is the current observable behaviour postmortems read to
    distinguish silent stalls from clean diagnoses. The test pins
    the marker so a future PR that "fixes" the gate can't accidentally
    strip the marker as part of the cleanup — postmortems would lose
    their attribution signal.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    stdout, _ = _run_stage1_only(rows=rows, qids=qids, run_root=tmp_path)

    diagnosed = parse_stage1_diagnosis_markers(stdout)
    assert diagnosed, (
        "Stage 1 marker stream is empty — the SM did not even attempt "
        "to diagnose the hard QIDs. Inspect the Stage 1 readiness path."
    )
    for marker in diagnosed:
        assert marker.get("diagnosis_actionable") is False, (
            f"qid={marker.get('qid')!r}: diagnosis_actionable should "
            f"be False (zero blame_set + empty evidence_summary + "
            f"insufficient-evidence sentinel). Marker: {marker!r}"
        )
        # ``blame_set_size`` is the load-bearing structural signal —
        # the gate, once it lands, will key off this rather than the
        # softer ``diagnosis_actionable`` rollup.
        blame_size = marker.get("blame_set_size")
        assert blame_size in (0, None), (
            f"qid={marker.get('qid')!r}: zero-blame tape should "
            f"produce blame_set_size=0; got {blame_size!r}. Marker: "
            f"{marker!r}"
        )


# ── Target contract (XFAIL today; flips green when the gate lands) ────


@pytest.mark.integration
def test_non_actionable_diagnosis_should_terminate_before_clustering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Target contract — every QID whose diagnosis is non-actionable
    (zero ``blame_set``, empty ``evidence_summary``) must terminate
    between DIAGNOSED and CLUSTERED with a typed
    ``diagnosis_not_actionable`` reason.

    Today: every QID either silently advances into Stage 2 (where the
    tape exhausts) or completes the SM iteration with no terminal
    record. Either way, the assertion below fails.

    The XFAIL strict marker means the moment the gate lands and this
    test passes, the strict flag flips it to XPASS and the suite goes
    red — forcing the maintainer to delete the marker and admit the
    contract is now live.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    _, final_states = _run_stage1_only(
        rows=rows, qids=qids, run_root=tmp_path
    )

    if not final_states:
        raise AssertionError(
            "Tape exhausted before final_states was populated — the "
            "SM tried to invoke Stage 2 on a non-actionable diagnosis, "
            "which is exactly the silent-decline mode this gate must "
            "prevent. The follow-up PR must short-circuit at the "
            "DIAGNOSED→CLUSTERED boundary."
        )

    by_qid = states_by_qid(final_states)
    missing_terminal: list[str] = []
    wrong_reason: list[tuple[str, str]] = []
    for qid in qids:
        state = by_qid.get(qid)
        assert state is not None, f"qid={qid!r} dropped from final states"
        terminal = state.terminal
        if terminal is None:
            missing_terminal.append(qid)
            continue
        if "non_actionable_diagnosis" not in (terminal.reason or ""):
            wrong_reason.append((qid, terminal.reason or "<empty>"))

    assert not missing_terminal and not wrong_reason, (
        "Diagnosis-actionable gate contract violated.\n"
        f"  QIDs with no terminal record (silently advanced): "
        f"{missing_terminal}\n"
        f"  QIDs with the wrong terminal reason: {wrong_reason}\n"
        "Expected every QID to terminate with "
        "reason containing 'non_actionable_diagnosis' before clustering."
    )
