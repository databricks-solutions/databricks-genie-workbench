"""Grep-guard for the run-end replay-validation wiring.

Three anchors:

1. Run-level default-init lives near ``_replay_fixture_json: str = ""``
   so a Phase-A failure leaves a clean ``None`` for the post-Phase-H
   ``_emit_contract_health_summary`` call to consume (i.e., the
   classifier treats it as ``skipped``).

2. The Phase A try-block calls ``run_replay(...)`` after
   ``serialize_replay_fixture(`` and before the outer Phase A
   ``except Exception:`` so the assignment is reachable on the
   happy path. We pin the textual neighborhood; the actual call
   shape is exercised by the unit tests in
   ``test_run_end_replay_validation.py``.

3. The post-Phase-H ``_emit_contract_health_summary`` call still
   reads ``locals().get("_run_end_replay_validation")``. This pins
   against drift in the bundle-status wiring fix.
"""
from __future__ import annotations

import pathlib


HARNESS = pathlib.Path(
    "src/genie_space_optimizer/optimization/harness.py"
)


def test_run_level_default_init_lives_near_replay_fixture_json() -> None:
    text = HARNESS.read_text(encoding="utf-8")
    fixture_json_anchor = '_replay_fixture_json: str = ""'
    init_anchor = "_run_end_replay_validation"
    assert fixture_json_anchor in text, (
        "_replay_fixture_json default-init missing; harness layout drifted"
    )
    fj_idx = text.find(fixture_json_anchor)
    init_idx = text.find(init_anchor)
    assert init_idx != -1, (
        "_run_end_replay_validation default-init missing; the post-"
        "Phase-H emission will see None for all runs and contract_health "
        "will fall back to is_valid=True"
    )
    assert 0 < init_idx - fj_idx < 1000, (
        f"_run_end_replay_validation default-init is {init_idx - fj_idx} "
        f"chars from _replay_fixture_json init; expected within 1000 "
        f"chars (same run-level state neighborhood)"
    )


def test_run_replay_call_inside_phase_a_block() -> None:
    text = HARNESS.read_text(encoding="utf-8")
    serialize_anchor = "_replay_fixture_json = serialize_replay_fixture("
    run_replay_call_anchor = "run_replay("
    phase_a_outer_except_anchor = (
        "        logger.warning(\n"
        '            "Phase A: replay-fixture export failed (non-fatal)",'
    )
    assert serialize_anchor in text, "Phase A serialize call missing"
    assert phase_a_outer_except_anchor in text, (
        "Phase A outer except clause missing; harness layout drifted"
    )

    s_idx = text.find(serialize_anchor)
    e_idx = text.find(phase_a_outer_except_anchor)

    rr_idx = text.find(run_replay_call_anchor, s_idx, e_idx)
    assert rr_idx != -1, (
        "run_replay(...) call missing inside Phase A try block. "
        "Without it, _run_end_replay_validation stays None and "
        "GSO_CONTRACT_HEALTH_V1 cannot report replay violations."
    )

    assignment_anchor = "_run_end_replay_validation = {"
    a_idx = text.find(assignment_anchor, s_idx, e_idx)
    assert a_idx != -1, (
        "_run_end_replay_validation = { ... } assignment missing inside "
        "Phase A try block. The run_replay result must be projected "
        "onto the {is_valid, violation_count} dict shape that the "
        "classifier consumes."
    )
    assert a_idx > s_idx, (
        "assignment must come after serialize_replay_fixture"
    )


def test_post_phase_h_emission_reads_run_end_replay_validation() -> None:
    text = HARNESS.read_text(encoding="utf-8")
    emit_read_anchor = (
        'replay_validation=locals().get("_run_end_replay_validation")'
    )
    assert emit_read_anchor in text, (
        "Relocated _emit_contract_health_summary call no longer reads "
        "locals().get('_run_end_replay_validation'); the bundle-status "
        "fix wiring has drifted or this validation plan's wiring is wrong"
    )
