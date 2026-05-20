"""Plan 9 Task 8 — PLAN5_ANCHOR_ACTIVATION_V1 marker emitter.

Verifies the ActivationStatus enum has the five required values
and that emit_plan5_activation produces the expected stdout line.
"""
import io
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.plan9_activation_markers import (
    ActivationStatus,
    emit_plan5_activation,
)


def test_activation_status_has_five_values():
    assert set(ActivationStatus) == {
        ActivationStatus.PLAN5_INTENT_INVOKED,
        ActivationStatus.PLAN5_INTENT_DECLINED,
        ActivationStatus.PLAN5_INTENT_VALIDATOR_REJECTED,
        ActivationStatus.PLAN5_INTENT_ROUTED,
        ActivationStatus.PLAN5_INTENT_MATERIALIZED,
    }


def test_emit_plan5_activation_writes_marker_line():
    buf = io.StringIO()
    with redirect_stdout(buf):
        emit_plan5_activation(
            run_id="run_test",
            iteration=2,
            ag_id="AG_H001",
            cluster_id="c_h001",
            status=ActivationStatus.PLAN5_INTENT_MATERIALIZED,
            reason="patch_body materialized to add_example_sql",
            patch_type="add_example_sql",
            intent_id="intent_h001_001",
        )
    output = buf.getvalue()
    assert "GSO_PLAN5_ANCHOR_ACTIVATION_V1" in output
    assert "AG_H001" in output
    assert "plan5_intent_materialized" in output
    assert "intent_h001_001" in output


def test_emit_plan5_activation_decline_includes_reason():
    buf = io.StringIO()
    with redirect_stdout(buf):
        emit_plan5_activation(
            run_id="run_test",
            iteration=1,
            ag_id="AG_H002",
            cluster_id="c_h002",
            status=ActivationStatus.PLAN5_INTENT_DECLINED,
            reason="llm_returned_abstain",
        )
    output = buf.getvalue()
    assert "plan5_intent_declined" in output
    assert "llm_returned_abstain" in output
