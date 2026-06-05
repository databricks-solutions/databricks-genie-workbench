"""Phase 3 (2026-05-16) — ``build_narrow_l6_replacement`` must strip
the stale counterfactual-scan stamps that drove the rejection of the
ORIGINAL broad patch, so the retest at
``harness.py:25813-25828`` evaluates the narrowed predicate against
fresh dependency data — not the same stamps that already failed.

The collateral-RISK stamps (``high_collateral_risk``,
``high_collateral_risk_flagged``, ``passing_dependents_outside_target``)
are dropped entirely. ``passing_dependents`` is RE-STAMPED to a fresh
empty list: the narrowed predicate is ``query_id``-scoped to the target
QID(s), so it has no passing dependents outside target. Under Trial 20
E2 (``GSO_TRIAL20_BLAST_RADIUS_MANDATORY``, default-on) leaving the
field absent would trip ``passing_dependents_missing`` and kill the
recovery path, so the fresh empty stamp is load-bearing.

Bug anchor: ``cluster_driven_synthesis.py:~1511-1523`` previously
returned ``{**original_patch, "where_predicate": narrowed, ...}`` which
carried forward ``high_collateral_risk: True`` and
``passing_dependents: [...]`` unchanged. Stamps the retest reads:
``proposal_grounding.py:556, 562``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    build_narrow_l6_replacement,
)


def _broad_l6_filter_with_stale_stamps() -> dict:
    """The shape ``_t24_counterfactual_scan`` produces on a broad
    ``add_sql_snippet_filter`` patch that gets dropped at
    blast-radius. Mirrors the H002 iter-2 case in Run B."""
    return {
        "proposal_id": "P_L6_H002_BROAD",
        "patch_type": "add_sql_snippet_filter",
        "target": "tkt_payment",
        "where_predicate": "tkt_payment.PAYMENT_CURRENCY_CD = 'USD'",
        "qid_predicate_column": "query_id",
        "rca_id": "RCA_H002",
        "root_cause": "missing_filter_dimension",
        # Stamps from _t24_counterfactual_scan:
        "high_collateral_risk": True,
        "high_collateral_risk_flagged": True,
        "passing_dependents": [
            "airline_gs_004", "airline_gs_005", "airline_gs_007",
        ],
        "passing_dependents_outside_target": [
            "airline_gs_004", "airline_gs_005", "airline_gs_007",
        ],
    }


def test_replacement_drops_high_collateral_risk_stamp():
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stale_stamps(),
        ag_target_qids=("airline_gs_023",),
        root_cause="missing_filter_dimension",
    )
    assert replacement is not None, (
        "Expected a non-None narrow replacement for a broad L6 "
        "filter with a where_predicate."
    )
    assert "high_collateral_risk" not in replacement, (
        f"Stale 'high_collateral_risk' stamp leaked through. "
        f"Replacement keys: {sorted(replacement.keys())}"
    )


def test_replacement_drops_high_collateral_risk_flagged_stamp():
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stale_stamps(),
        ag_target_qids=("airline_gs_023",),
        root_cause="missing_filter_dimension",
    )
    assert "high_collateral_risk_flagged" not in replacement


def test_replacement_drops_passing_dependents_stamp():
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stale_stamps(),
        ag_target_qids=("airline_gs_023",),
        root_cause="missing_filter_dimension",
    )
    # The STALE broad list must be gone; the builder re-stamps a FRESH
    # empty ``passing_dependents`` (query_id-scoped ⇒ no outside
    # dependents) so Trial 20 E2's mandatory-stamp gate evaluates the
    # narrowed candidate as safe instead of rejecting
    # ``passing_dependents_missing``. ``patch_blast_radius_is_safe``
    # reads this field directly.
    assert replacement.get("passing_dependents", []) == [], (
        f"Expected a fresh empty 'passing_dependents', not the broad "
        f"patch's stale list. Got: {replacement.get('passing_dependents')!r}"
    )


def test_replacement_drops_passing_dependents_outside_target_stamp():
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stale_stamps(),
        ag_target_qids=("airline_gs_023",),
        root_cause="missing_filter_dimension",
    )
    assert "passing_dependents_outside_target" not in replacement


def test_replacement_keeps_payload_fields():
    """The non-stamp fields must survive unchanged — the narrowed
    predicate, the inherited rca_id / root_cause, the proposal_id
    derivation, and the qid_predicate_column."""
    original = _broad_l6_filter_with_stale_stamps()
    replacement = build_narrow_l6_replacement(
        original_patch=original,
        ag_target_qids=("airline_gs_023",),
        root_cause="missing_filter_dimension",
    )
    assert replacement["patch_type"] == "add_sql_snippet_filter"
    assert replacement["target"] == "tkt_payment"
    assert replacement["rca_id"] == "RCA_H002"
    assert replacement["root_cause"] == "missing_filter_dimension"
    assert replacement["qid_predicate_column"] == "query_id"
    assert "tkt_payment.PAYMENT_CURRENCY_CD" in replacement["where_predicate"]
    assert "airline_gs_023" in replacement["where_predicate"]
    assert replacement["derived_from"] == "P_L6_H002_BROAD"
    assert replacement["proposal_id"] == "P_L6_H002_BROAD#NARROW"
    assert replacement["narrow_target_qids"] == ("airline_gs_023",)


def test_replacement_passes_blast_radius_when_only_stale_stamps_failed_it():
    """End-to-end behavioral check: a replacement that previously
    failed BLAST_RADIUS only because of carried-forward stamps must
    pass when the stamps are stripped and re-evaluated against fresh
    dependency data.

    The builder strips the STALE ``passing_dependents`` and re-stamps a
    FRESH empty list (the narrowed predicate targets only
    ``airline_gs_023``, so its footprint has no passing dependents
    outside target). Under Trial 20 E2 (default-on) a *missing* stamp is
    treated as a plumbing regression and fails closed; the fresh empty
    stamp is what makes the gate return safe.
    """
    from genie_space_optimizer.optimization.proposal_grounding import (
        patch_blast_radius_is_safe,
    )
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stale_stamps(),
        ag_target_qids=("airline_gs_023",),
        root_cause="missing_filter_dimension",
    )
    # The stale broad list is gone; the builder re-stamped a fresh
    # empty footprint, so the gate runs directly on the replacement.
    assert replacement.get("passing_dependents", []) == []
    verdict = patch_blast_radius_is_safe(
        replacement,
        ag_target_qids=("airline_gs_023",),
        max_outside_target=0,
        live_hard_qids=("airline_gs_023",),
    )
    assert verdict["safe"] is True, (
        f"Narrowed replacement must pass blast-radius after the "
        f"strip + fresh re-stamp. Got verdict={verdict!r}"
    )


def test_strip_does_not_mutate_original_patch():
    """The helper must be pure — the caller's ``original_patch``
    dict must NOT have its stamps stripped as a side effect."""
    original = _broad_l6_filter_with_stale_stamps()
    _ = build_narrow_l6_replacement(
        original_patch=original,
        ag_target_qids=("airline_gs_023",),
        root_cause="missing_filter_dimension",
    )
    assert original["high_collateral_risk"] is True
    assert "passing_dependents" in original
    assert original["passing_dependents"] == [
        "airline_gs_004", "airline_gs_005", "airline_gs_007",
    ]


def test_non_l6_patch_still_returns_none():
    """Regression guard: the strip must NOT widen the helper's
    applicability — non-L6 patch types still return None."""
    patch = {
        **_broad_l6_filter_with_stale_stamps(),
        "patch_type": "add_example_sql",
        "where_predicate": "",
    }
    replacement = build_narrow_l6_replacement(
        original_patch=patch,
        ag_target_qids=("airline_gs_023",),
        root_cause="missing_filter_dimension",
    )
    assert replacement is None


def test_empty_target_qids_still_returns_none():
    """Regression guard: empty target_qids still returns None."""
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_with_stale_stamps(),
        ag_target_qids=(),
        root_cause="missing_filter_dimension",
    )
    assert replacement is None
