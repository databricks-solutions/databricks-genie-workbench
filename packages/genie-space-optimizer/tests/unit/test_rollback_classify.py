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
        # Cycle 13 — was OTHER pre-C13; now NO_ACTION.
        ("no_proposals",                                    RollbackClass.NO_ACTION),
        # Cycle 13 — was OTHER pre-C13 (default fall-through); now
        # NO_ACTION via explicit exact-match branch.
        ("ag_collision_with_forbidden_set",                 RollbackClass.NO_ACTION),
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


# ── Cycle 14B-T2: ACCEPTED_WITH_DEBT enum value ──────────────────────


def test_rollback_class_has_accepted_with_debt() -> None:
    assert RollbackClass.ACCEPTED_WITH_DEBT.value == "accepted_with_debt"


def test_classify_accepted_with_debt_prefix() -> None:
    assert (
        classify_rollback_reason("accepted_with_debt:gs_018")
        is RollbackClass.ACCEPTED_WITH_DEBT
    )


def test_classify_accepted_with_debt_bare_string() -> None:
    assert (
        classify_rollback_reason("accepted_with_debt")
        is RollbackClass.ACCEPTED_WITH_DEBT
    )


def test_classify_unknown_still_returns_other() -> None:
    """The new value must NOT broaden OTHER's catch-all behavior."""
    assert classify_rollback_reason("brand_new_label") is RollbackClass.OTHER


# ── Cycle 14B-T3: MULTI_PATCH_REGRESSION enum value ──────────────────


def test_rollback_class_has_multi_patch_regression() -> None:
    assert RollbackClass.MULTI_PATCH_REGRESSION.value == "multi_patch_regression"


def test_classify_multi_patch_regression_with_qid_list() -> None:
    assert (
        classify_rollback_reason("multi_patch_regression:gs_018,gs_004")
        is RollbackClass.MULTI_PATCH_REGRESSION
    )


def test_classify_multi_patch_regression_bare_string() -> None:
    assert (
        classify_rollback_reason("multi_patch_regression")
        is RollbackClass.MULTI_PATCH_REGRESSION
    )


# ── Cycle 13: NO_ACTION enum value ───────────────────────────────────


def test_rollback_class_has_no_action() -> None:
    assert RollbackClass.NO_ACTION.value == "no_action"


def test_classify_no_proposals_is_no_action() -> None:
    """Cycle 13 — `no_proposals` reflections route to NO_ACTION
    (was OTHER pre-C13). The change is byte-stable on flag-off because
    the forbidden-set admission predicate gates on NO_ACTION explicitly."""
    assert (
        classify_rollback_reason("no_proposals") is RollbackClass.NO_ACTION
    )


def test_classify_ag_collision_is_no_action() -> None:
    """Cycle 13 — `ag_collision_with_forbidden_set` reflections also
    route to NO_ACTION. Same byte-stability argument as no_proposals."""
    assert (
        classify_rollback_reason("ag_collision_with_forbidden_set")
        is RollbackClass.NO_ACTION
    )


def test_no_action_does_not_steal_existing_other_cases() -> None:
    """The new NO_ACTION value must not contaminate strings that
    classify as OTHER today (escalations, blank reasons, unknown
    labels)."""
    for sample in (
        "escalation:flag_for_review",
        "escalation:gt_repair (delegated to arbiter)",
        "",
        None,
        "unknown",
        "something_nobody_has_produced_yet",
    ):
        assert classify_rollback_reason(sample) is RollbackClass.OTHER
