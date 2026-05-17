"""Phase 3 (2026-05-16) — Run B H002 iter-2 gate-drop recovery
active replay.

Run B's iter-2 selected ``AG_DECOMPOSED_H002`` after iter-1's H001
NO_APPLIED_PATCHES terminal. H002's RCA: missing filter on
``tkt_payment.PAYMENT_CURRENCY_CD = 'USD'`` for currency-scoped
revenue questions. Lever: L6 (add_sql_snippet_filter).

Counterfactual scan stamped the broad L6 predicate with
``passing_dependents=["airline_gs_004", "airline_gs_005",
"airline_gs_007"]`` (other passing questions that touch
``tkt_payment``) and ``high_collateral_risk=True``. Blast-radius
rejected with ``high_collateral_risk_flagged``.

Before Phase 3: ``try_narrow_replacement`` produced a narrowed
candidate but carried the stamps forward; retest re-rejected for
the same reason; iter-3 re-selected the same broad shape.

After Phase 3 + Phase 0.5 + Phase 2:
* Phase 0.5 Task 3 (``GSO_SQL_SHAPE_OVERLAP_GATE=1`` default-on)
  may itself prevent the drop by tightening the dependent-counting
  predicate. If so, the broad patch survives blast-radius directly
  (path A).
* Phase 3 Task 1 (strip stale stamps) ensures that IF the broad
  patch is still dropped, the narrow replacement's retest evaluates
  fresh and either passes or produces a typed failure (path B).

The user-spec contract is OR: at least one of the two paths must
yield a recovery (applied patch); the test passes if either does.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.auto_narrow_replacement import (
    try_narrow_replacement,
)
from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    build_narrow_l6_replacement,
)
from genie_space_optimizer.optimization.proposal_grounding import (
    patch_blast_radius_is_safe,
)


# Run B H002 iter-2 fixture constants.
H002_TARGET_QID = "airline_gs_023"
H002_PASSING_DEPENDENTS = (
    "airline_gs_004", "airline_gs_005", "airline_gs_007",
)
H002_BROAD_PREDICATE = "tkt_payment.PAYMENT_CURRENCY_CD = 'USD'"
H002_TARGET_TABLE = "tkt_payment"
H002_ROOT_CAUSE = "missing_filter_dimension"


def _broad_l6_filter_with_stamps() -> dict:
    """The broad L6 patch as the counterfactual scan stamps it."""
    return {
        "proposal_id": "P_L6_H002_BROAD",
        "patch_type": "add_sql_snippet_filter",
        "target": H002_TARGET_TABLE,
        "where_predicate": H002_BROAD_PREDICATE,
        "qid_predicate_column": "query_id",
        "rca_id": "RCA_H002",
        "root_cause": H002_ROOT_CAUSE,
        "high_collateral_risk": True,
        "passing_dependents": list(H002_PASSING_DEPENDENTS),
    }


def _broad_patch_blast_radius_verdict() -> dict:
    """Run the actual blast-radius gate on the broad patch."""
    return patch_blast_radius_is_safe(
        _broad_l6_filter_with_stamps(),
        ag_target_qids=(H002_TARGET_QID,),
        max_outside_target=0,
        live_hard_qids=(H002_TARGET_QID,),
    )


def _narrow_replacement_retest_verdict() -> dict:
    """Synthesise the narrow replacement and run the retest path
    the harness uses at harness.py:~25881."""
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stamps(),
        ag_target_qids=(H002_TARGET_QID,),
        root_cause=H002_ROOT_CAUSE,
    )
    assert replacement is not None, (
        "build_narrow_l6_replacement must produce a non-None "
        "replacement for a broad L6 filter with a where_predicate; "
        "the H002 iter-2 fixture's predicate is non-empty."
    )
    return patch_blast_radius_is_safe(
        replacement,
        ag_target_qids=(H002_TARGET_QID,),
        max_outside_target=0,
        live_hard_qids=(H002_TARGET_QID,),
    )


def test_recovery_either_path_succeeds():
    """The exit-criterion OR: at least one of (a) broad patch
    survives blast-radius, OR (b) narrow replacement retest succeeds.

    Per the user spec: "the L6 filter on
    tkt_payment.PAYMENT_CURRENCY_CD survives blast-radius with the
    SQL-shape gate on, OR a narrow replacement is produced AND its
    retest uses fresh stamps."
    """
    broad_verdict = _broad_patch_blast_radius_verdict()
    if broad_verdict["safe"] is True:
        # Path A: SQL-shape gate prevented the rejection. No narrow
        # replacement needed.
        return

    narrow_verdict = _narrow_replacement_retest_verdict()
    assert narrow_verdict["safe"] is True, (
        f"Neither recovery path succeeded.\n"
        f"  broad blast-radius verdict: {broad_verdict!r}\n"
        f"  narrow retest verdict: {narrow_verdict!r}\n"
        f"This means iter-3 would re-select the identical broad "
        f"shape because no survivor exists. Phase 3 Task 1's "
        f"strip is the load-bearing fix; verify it's in place."
    )


def test_narrow_retest_succeeds_when_stamps_are_stripped():
    """Direct assertion of Phase 3's recovery contract: the narrow
    replacement retest uses fresh stamps (i.e., the absence of
    stale stamps) and reports ``safe=True``."""
    verdict = _narrow_replacement_retest_verdict()
    assert verdict["safe"] is True, (
        f"Narrow replacement retest failed despite the strip. "
        f"Got: {verdict!r}. Most likely cause: the narrowed "
        f"predicate is still detected as over-broad by a stamp "
        f"path the strip doesn't cover."
    )


def test_narrow_replacement_carries_no_collateral_stamps():
    """Defensive: the replacement returned by
    ``build_narrow_l6_replacement`` must have zero of the four
    Phase 3 stale stamps."""
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stamps(),
        ag_target_qids=(H002_TARGET_QID,),
        root_cause=H002_ROOT_CAUSE,
    )
    assert replacement is not None
    forbidden = (
        "high_collateral_risk",
        "high_collateral_risk_flagged",
        "passing_dependents",
        "passing_dependents_outside_target",
    )
    leaked = [k for k in forbidden if k in replacement]
    assert not leaked, (
        f"Stale stamps leaked through into the replacement: "
        f"{leaked}. Phase 3 Task 1's strip is incomplete."
    )


def test_narrow_replacement_keeps_predicate_payload():
    """The narrowed predicate must include the original broad clause
    AND the query_id scope for the H002 target QID."""
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stamps(),
        ag_target_qids=(H002_TARGET_QID,),
        root_cause=H002_ROOT_CAUSE,
    )
    assert replacement is not None
    predicate = str(replacement.get("where_predicate") or "")
    assert "tkt_payment.PAYMENT_CURRENCY_CD" in predicate, (
        f"Replacement predicate dropped the original broad clause. "
        f"Got: {predicate!r}"
    )
    assert H002_TARGET_QID in predicate, (
        f"Replacement predicate missing the query_id scope for "
        f"the H002 target QID. Got: {predicate!r}"
    )


def test_try_narrow_replacement_orchestrator_reports_attempted():
    """End-to-end through ``try_narrow_replacement``: the orchestrator
    must report ``attempted=True`` and surface a non-None
    ``replacement_patch`` for the H002 fixture.

    The orchestrator passes the dropped patch via
    ``original_dropped_patch=`` and the cluster's target_qids must
    be plumbed through the lambda since the orchestrator does not
    pass them directly.
    """
    broad = _broad_l6_filter_with_stamps()

    def _l6_callback(*, cluster, rca_card, protected_dependents,
                     original_dropped_patch):
        return build_narrow_l6_replacement(
            original_patch=dict(original_dropped_patch),
            ag_target_qids=tuple(
                str(q) for q in (cluster or {}).get("target_qids") or ()
            ),
            root_cause=str((rca_card or {}).get("root_cause") or ""),
            protected_dependents=protected_dependents,
        )

    result = try_narrow_replacement(
        dropped_patches=[
            {**broad, "drop_reason": "high_collateral_risk_flagged"},
        ],
        outside_target_qids=H002_PASSING_DEPENDENTS,
        cluster={
            "cluster_id": "H002",
            "target_qids": [H002_TARGET_QID],
            "root_cause": H002_ROOT_CAUSE,
        },
        rca_card={
            "root_cause": H002_ROOT_CAUSE,
            "blame_set": [H002_TARGET_TABLE],
        },
        synthesis_callable_l6=_l6_callback,
        synthesis_callable_l5=lambda **_: None,
    )
    assert result.attempted is True
    assert result.replacement_patch is not None
    # The orchestrator-produced replacement must also be stamp-free.
    assert "high_collateral_risk" not in result.replacement_patch
    assert "passing_dependents" not in result.replacement_patch
