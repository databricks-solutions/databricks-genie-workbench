"""Regression tests for Phase C1 rollback-reason classifier.

Every producer prefix currently emitted by the lever-loop code must map
to a deterministic :class:`RollbackClass`. A new, unrecognised reason
must fall to ``OTHER`` rather than silently contaminating the
``INFRA_FAILURE`` budget.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.rollback_class import (
    RollbackClass,
    classify_rollback_reason,
)


@pytest.mark.parametrize(
    "reason,expected",
    [
        # Content-regression gates.
        ("slice_gate: result_correctness", RollbackClass.CONTENT_REGRESSION),
        ("p0_gate: 3 failures",            RollbackClass.CONTENT_REGRESSION),
        ("full_eval: schema_accuracy",     RollbackClass.CONTENT_REGRESSION),
        # Schema-fatal payload rejections.
        (
            "patch_deploy_failed: Invalid serialized_space: Cannot find "
            "field: failure_clusters in message X",
            RollbackClass.SCHEMA_FAILURE,
        ),
        (
            "patch_deploy_failed: Invalid serialized_space",
            RollbackClass.SCHEMA_FAILURE,
        ),
        # Non-schema deploy failures are infra.
        (
            "patch_deploy_failed: 500 Internal Server Error",
            RollbackClass.INFRA_FAILURE,
        ),
        (
            "patch_deploy_failed: Connection reset by peer",
            RollbackClass.INFRA_FAILURE,
        ),
        # Escalations and other skips.
        ("escalation:flag_for_review",                     RollbackClass.OTHER),
        ("escalation:gt_repair (delegated to arbiter)",    RollbackClass.OTHER),
        ("no_proposals",                                    RollbackClass.OTHER),
        # Collision guard from Phase D2.
        ("ag_collision_with_forbidden_set",                 RollbackClass.OTHER),
        # Unknown / empty.
        (None,                                              RollbackClass.OTHER),
        ("",                                                RollbackClass.OTHER),
        ("unknown",                                         RollbackClass.OTHER),
        ("something_nobody_has_produced_yet",               RollbackClass.OTHER),
    ],
)
def test_classify_rollback_reason(reason, expected) -> None:
    assert classify_rollback_reason(reason) == expected


def test_schema_failure_takes_precedence_over_patch_deploy_prefix() -> None:
    """``patch_deploy_failed:`` with a schema-failure substring inside must
    classify as SCHEMA_FAILURE, not INFRA_FAILURE — the inner match is
    the deterministic signal."""
    assert (
        classify_rollback_reason(
            "patch_deploy_failed: Invalid serialized_space: Cannot find field: x",
        )
        == RollbackClass.SCHEMA_FAILURE
    )


def test_propagation_failure_reserved_but_unproduced() -> None:
    """PROPAGATION_FAILURE lives in the enum for future use but nothing
    in the current codebase should produce it. If a test ever breaks
    here, someone added a producer without updating the plan."""
    # Sentinel: no string we classify should map to PROPAGATION_FAILURE.
    for sample in ("propagation_failed:", "steady_state_timeout"):
        assert classify_rollback_reason(sample) != RollbackClass.PROPAGATION_FAILURE
