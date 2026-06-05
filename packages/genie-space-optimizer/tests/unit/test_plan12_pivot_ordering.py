"""Trial 18 Step 5 — Plan 12 pivot ordering observability.

Postmortem context (run e94376a3): the strategist's
``GSO_PLAN12_AG_PIVOT_DECIDED_V1`` marker fired with
``pivot_applied=True`` for ``AG_DECOMPOSED_H002``, but the iteration
aborted on an earlier terminal AG and the pivoted AG never reached
the lever loop. The marker overstates the evidence: it reports a
"pivot applied" as if the pivoted AG actually ran. Postmortem
readers can't distinguish a pivot that ran from a pivot that was
mutated-but-skipped.

Trial 18's fix is **honesty-not-execution**: keep the existing
DECIDED marker as the planning-time record, but add a sibling
``GSO_PLAN12_PIVOT_SKIPPED_V1`` marker emitted at the iteration
boundary for every pivoted AG that was never consumed. The new
marker carries:

  * ``ag_id`` — the pivoted AG that didn't execute.
  * ``reason`` — typed (today only
    ``pivot_skipped_due_to_abort_ordering``).

Postmortem skill: when a DECIDED row with ``pivot_applied=True``
exists for an ``ag_id`` AND a SKIPPED row exists for the same
``ag_id`` in the same iteration, the postmortem must NOT treat the
pivot as tested evidence.
"""
from __future__ import annotations

import json


def _parse_markers(out: str, prefix: str) -> list[dict]:
    rows: list[dict] = []
    for line in out.splitlines():
        if line.startswith(prefix + " "):
            rows.append(json.loads(line.partition(" ")[2]))
    return rows


# ----------------------------------------------------------------------
# Marker shape — the SKIPPED marker is a typed, named event.
# ----------------------------------------------------------------------


def test_plan12_pivot_skipped_marker_shape():
    """Marker carries optimization_run_id, iteration, ag_id, cluster_id,
    and a typed ``reason`` field. Mirrors the DECIDED marker shape so
    postmortem joins by ``(run_id, iteration, ag_id)`` are trivial.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan12_pivot_skipped_marker,
    )

    line = plan12_pivot_skipped_marker(
        optimization_run_id="r_t18_s5",
        iteration=4,
        ag_id="AG_DECOMPOSED_H002",
        cluster_id="H002",
        reason="pivot_skipped_due_to_abort_ordering",
    )
    assert line.startswith("GSO_PLAN12_PIVOT_SKIPPED_V1 ")
    payload = json.loads(line.partition(" ")[2])
    assert payload == {
        "optimization_run_id": "r_t18_s5",
        "iteration": 4,
        "ag_id": "AG_DECOMPOSED_H002",
        "cluster_id": "H002",
        "reason": "pivot_skipped_due_to_abort_ordering",
    }


# ----------------------------------------------------------------------
# Helper: emit SKIPPED markers for pivoted-but-unexecuted AGs.
# ----------------------------------------------------------------------


def test_pivot_marked_applied_only_if_executed(capsys):
    """A pivoted AG that was not in ``executed_ag_ids`` produces a
    ``GSO_PLAN12_PIVOT_SKIPPED_V1`` marker with reason
    ``pivot_skipped_due_to_abort_ordering``. The pivoted AG that DID
    execute does NOT produce a SKIPPED marker.
    """
    from genie_space_optimizer.optimization.plan12_pivot_observability import (
        emit_plan12_pivot_skipped_for_unexecuted,
    )

    emitted = emit_plan12_pivot_skipped_for_unexecuted(
        optimization_run_id="r_t18_s5",
        iteration=4,
        pivoted_ag_ids={
            "AG_DECOMPOSED_H001": "H001",  # executed
            "AG_DECOMPOSED_H002": "H002",  # skipped — iteration aborted earlier
        },
        executed_ag_ids={"AG_DECOMPOSED_H001"},
    )
    assert emitted == ("AG_DECOMPOSED_H002",)

    rows = _parse_markers(
        capsys.readouterr().out, "GSO_PLAN12_PIVOT_SKIPPED_V1",
    )
    assert len(rows) == 1
    skipped = rows[0]
    assert skipped["ag_id"] == "AG_DECOMPOSED_H002"
    assert skipped["cluster_id"] == "H002"
    assert skipped["reason"] == "pivot_skipped_due_to_abort_ordering"


def test_no_pivoted_ags_emits_nothing(capsys):
    """Empty pivoted set → no markers, no work."""
    from genie_space_optimizer.optimization.plan12_pivot_observability import (
        emit_plan12_pivot_skipped_for_unexecuted,
    )

    emitted = emit_plan12_pivot_skipped_for_unexecuted(
        optimization_run_id="r",
        iteration=1,
        pivoted_ag_ids={},
        executed_ag_ids={"AG_X"},
    )
    assert emitted == ()
    assert capsys.readouterr().out == ""


def test_all_pivoted_ags_executed_emits_nothing(capsys):
    """When every pivoted AG ran, no SKIPPED markers fire."""
    from genie_space_optimizer.optimization.plan12_pivot_observability import (
        emit_plan12_pivot_skipped_for_unexecuted,
    )

    emitted = emit_plan12_pivot_skipped_for_unexecuted(
        optimization_run_id="r",
        iteration=1,
        pivoted_ag_ids={"AG_A": "H_A", "AG_B": "H_B"},
        executed_ag_ids={"AG_A", "AG_B"},
    )
    assert emitted == ()
    rows = _parse_markers(
        capsys.readouterr().out, "GSO_PLAN12_PIVOT_SKIPPED_V1",
    )
    assert rows == []


def test_emits_one_marker_per_unexecuted_pivot(capsys):
    """When multiple pivoted AGs were skipped, one marker per AG fires
    in stable AG-id order so postmortem renderers see deterministic
    output."""
    from genie_space_optimizer.optimization.plan12_pivot_observability import (
        emit_plan12_pivot_skipped_for_unexecuted,
    )

    emitted = emit_plan12_pivot_skipped_for_unexecuted(
        optimization_run_id="r",
        iteration=2,
        pivoted_ag_ids={
            "AG_DECOMPOSED_H002": "H002",
            "AG_DECOMPOSED_H003": "H003",
            "AG_DECOMPOSED_H001": "H001",
        },
        executed_ag_ids=set(),
    )
    # Stable sorted order so postmortem replay is deterministic.
    assert emitted == (
        "AG_DECOMPOSED_H001",
        "AG_DECOMPOSED_H002",
        "AG_DECOMPOSED_H003",
    )
    rows = _parse_markers(
        capsys.readouterr().out, "GSO_PLAN12_PIVOT_SKIPPED_V1",
    )
    assert len(rows) == 3


# ----------------------------------------------------------------------
# Flag-off rollback — Trial 18 marker is gated on
# GSO_TRIAL18_ACCEPTANCE_OVERHAUL so emergency rollback restores the
# pre-Trial-18 silence (no SKIPPED markers).
# ----------------------------------------------------------------------


def test_flag_off_suppresses_skipped_marker(capsys, monkeypatch):
    monkeypatch.setenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", "0")
    from genie_space_optimizer.optimization.plan12_pivot_observability import (
        emit_plan12_pivot_skipped_for_unexecuted,
    )

    emitted = emit_plan12_pivot_skipped_for_unexecuted(
        optimization_run_id="r",
        iteration=1,
        pivoted_ag_ids={"AG_X": "H_X"},
        executed_ag_ids=set(),
    )
    assert emitted == ()
    rows = _parse_markers(
        capsys.readouterr().out, "GSO_PLAN12_PIVOT_SKIPPED_V1",
    )
    assert rows == []
