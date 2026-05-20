"""Plan 9 Task 9.1 — harness-level GSO_PLAN5_ANCHOR_ACTIVATION_V1
marker tests.

Verifies:
  1. ActivationStatus has the three new harness-level values.
  2. emit_plan5_activation produces marker lines with each new
     status when called with that status.
  3. The marker_line payload carries status, ag_id, iteration,
     and a non-empty reason for every drop-path emit.

The actual harness-side wire-in is exercised by the focused
keyword sweep (-k "activation_marker or plan9_activation") plus
the existing test_plan9_activation_markers_emit.py.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.plan9_activation_markers import (
    ActivationStatus,
    emit_plan5_activation,
)


def test_activation_status_has_three_new_harness_values():
    """ActivationStatus must expose the harness-level vocabulary so
    postmortem readers can attribute every AG-iteration pair."""
    assert ActivationStatus.ANCHOR_FORBIDDEN_SET_DROPPED.value == (
        "anchor_forbidden_set_dropped"
    )
    assert ActivationStatus.ANCHOR_COLLISION_GUARD_DROPPED.value == (
        "anchor_collision_guard_dropped"
    )
    assert ActivationStatus.ANCHOR_ENTERED_PLAN5_DISPATCH.value == (
        "anchor_entered_plan5_dispatch"
    )


def test_activation_status_now_has_eight_total_values():
    """Five T8 values + three T9.1 values = eight."""
    assert len(set(ActivationStatus)) == 8


def test_emit_anchor_forbidden_set_dropped_writes_marker():
    buf = io.StringIO()
    with redirect_stdout(buf):
        emit_plan5_activation(
            run_id="run_t91",
            iteration=3,
            ag_id="AG_H001",
            cluster_id="",
            status=ActivationStatus.ANCHOR_FORBIDDEN_SET_DROPPED,
            reason="matches_forbidden_signature",
        )
    output = buf.getvalue()
    assert "GSO_PLAN5_ANCHOR_ACTIVATION_V1" in output
    assert "anchor_forbidden_set_dropped" in output
    assert "AG_H001" in output
    assert "matches_forbidden_signature" in output


def test_emit_anchor_collision_guard_dropped_writes_marker():
    buf = io.StringIO()
    with redirect_stdout(buf):
        emit_plan5_activation(
            run_id="run_t91",
            iteration=4,
            ag_id="AG_H002",
            cluster_id="",
            status=ActivationStatus.ANCHOR_COLLISION_GUARD_DROPPED,
            reason="root_cause_axis_collision",
        )
    output = buf.getvalue()
    assert "GSO_PLAN5_ANCHOR_ACTIVATION_V1" in output
    assert "anchor_collision_guard_dropped" in output
    assert "AG_H002" in output
    assert "root_cause_axis_collision" in output


def test_emit_anchor_entered_plan5_dispatch_writes_marker():
    buf = io.StringIO()
    with redirect_stdout(buf):
        emit_plan5_activation(
            run_id="run_t91",
            iteration=5,
            ag_id="AG_H003",
            cluster_id="c_h003",
            status=ActivationStatus.ANCHOR_ENTERED_PLAN5_DISPATCH,
            reason="generate_proposals_from_strategy_invoked",
        )
    output = buf.getvalue()
    assert "GSO_PLAN5_ANCHOR_ACTIVATION_V1" in output
    assert "anchor_entered_plan5_dispatch" in output
    assert "AG_H003" in output
    assert "c_h003" in output
