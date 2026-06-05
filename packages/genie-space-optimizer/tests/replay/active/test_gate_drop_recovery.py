"""Phase 4 Test 3 (2026-05-16) — gate-drop recovery active replay
against Run B (59a173d3) H002 cluster iter 2/4.

User-spec contract (Phase 3 exit criterion):
  Run B's H002 iter 2 fixture replays to either an applied patch
  or a retired signature, never to "repeat the same shape in
  iter 4".

This test reads the H002 cluster identity from the fixture
(``cluster_id="H002"``, ``root_cause="missing_filter"``, target QID
``airline_ticketing_and_fare_analysis_gs_024``) and drives the
Phase 3 producer-consumer chain:

    build_narrow_l6_replacement      (Phase 3 with strip)
        ↓
    patch_blast_radius_is_safe       (existing consumer)

The narrowed replacement must (a) carry NO stale stamps after
the strip, AND (b) pass the blast-radius retest.

Unlike Phase 3's own ``test_run_b_h002_iter_2_l6_recovery.py``
which uses hand-picked constants, this test derives the cluster
identity from the postmortem fixture so a future fixture re-export
that changes the target QID surfaces here too.

**Failure mode on current main (before Phase 3):**
  The narrow replacement carries forward ``high_collateral_risk=
  True`` and ``passing_dependents=[...]`` from the broad patch's
  stamps; ``patch_blast_radius_is_safe`` re-rejects with
  ``high_collateral_risk_flagged``.

**Pass mode after Phase 3 + Trial 20 E2 (current state):**
  ``_strip_blast_radius_stamps`` drops the collateral-risk stamps and
  ``build_narrow_l6_replacement`` re-stamps a fresh empty
  ``passing_dependents=[]`` (the query_id-scoped predicate has no
  outside dependents). Under Trial 20 E2 (``GSO_TRIAL20_BLAST_RADIUS_
  MANDATORY``, default-on) the retest reports ``{"safe": True,
  "reason": "no_passing_dependents_outside_target"}``. Without the
  re-stamp the missing field would trip ``passing_dependents_missing``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    build_narrow_l6_replacement,
)
from genie_space_optimizer.optimization.proposal_grounding import (
    patch_blast_radius_is_safe,
)

from tests.replay.active._postmortem_fixtures import (
    get_cluster_by_id,
    get_iteration,
    load_run_b_59a173d3,
)


# Run B (59a173d3) H002 cluster — root_cause is ``missing_filter``
# per the fixture; the canonical airline blame asset for missing-
# filter scenarios is the bookings fact table.
RUN_B_H002_EXPECTED_ROOT_CAUSE = "missing_filter"
RUN_B_H002_BLAME_TABLE = "catalog.airline.fact_bookings"


def _h002_cluster_from_fixture() -> dict:
    """Return Run B iter-1's H002 cluster record from the fixture."""
    it1 = get_iteration(load_run_b_59a173d3(), 1)
    return get_cluster_by_id(it1, "H002")


def _broad_l6_filter_for_h002() -> dict:
    """Construct the broad L6 filter shape that Phase 3's audit
    documents would be generated for the H002 missing-filter RCA.

    The fixture doesn't record the broad-patch shape (the postmortem
    captures the AG_PIPELINE stalemate, not the patch-level
    synthesis). We construct the shape by mirroring the H002 cluster
    identity (target QID, root cause) and the canonical L6
    ``add_sql_snippet_filter`` patch_type with the
    counterfactual-scan stamps the harness would have written."""
    cluster = _h002_cluster_from_fixture()
    target_qids = tuple(
        str(q) for q in (cluster.get("question_ids") or []) if str(q)
    )
    return {
        "proposal_id": "P_L6_H002_BROAD",
        "patch_type": "add_sql_snippet_filter",
        "target": RUN_B_H002_BLAME_TABLE,
        "where_predicate": (
            "fact_bookings.booking_status = 'confirmed'"
        ),
        "qid_predicate_column": "query_id",
        "rca_id": "RCA_H002",
        "root_cause": str(cluster.get("root_cause") or ""),
        # Stamps that ``_t24_counterfactual_scan`` would write:
        "high_collateral_risk": True,
        "high_collateral_risk_flagged": True,
        "passing_dependents": [
            "airline_ticketing_and_fare_analysis_gs_004",
            "airline_ticketing_and_fare_analysis_gs_005",
            "airline_ticketing_and_fare_analysis_gs_007",
        ],
        "passing_dependents_outside_target": [
            "airline_ticketing_and_fare_analysis_gs_004",
            "airline_ticketing_and_fare_analysis_gs_005",
            "airline_ticketing_and_fare_analysis_gs_007",
        ],
        "_target_qids_for_synthesis": target_qids,
    }


def test_h002_cluster_identity_matches_fixture():
    """Sanity check: the H002 cluster in Run B iter 1 has
    ``root_cause=missing_filter`` and one airline target QID."""
    cluster = _h002_cluster_from_fixture()
    assert str(cluster.get("root_cause") or "") == RUN_B_H002_EXPECTED_ROOT_CAUSE
    target_qids = list(cluster.get("question_ids") or [])
    assert len(target_qids) == 1
    assert target_qids[0].startswith("airline_ticketing_and_fare_analysis_gs_")


def test_broad_patch_fails_blast_radius_pre_fix():
    """The broad patch with stale stamps must FAIL blast-radius via
    ``high_collateral_risk_flagged``. This pins the bug Phase 3
    fixes — if blast-radius unexpectedly passes here, the stamp
    logic has drifted and the rest of this test is invalid."""
    cluster = _h002_cluster_from_fixture()
    target_qids = tuple(
        str(q) for q in (cluster.get("question_ids") or []) if str(q)
    )
    verdict = patch_blast_radius_is_safe(
        _broad_l6_filter_for_h002(),
        ag_target_qids=target_qids,
        max_outside_target=0,
        live_hard_qids=target_qids,
    )
    assert verdict["safe"] is False, (
        f"Broad patch with stale stamps unexpectedly passed "
        f"blast-radius. verdict={verdict!r}. The stamp logic has "
        f"drifted; Phase 4 Test 3 is invalid until the stamps are "
        f"confirmed to trigger the rejection."
    )
    assert verdict.get("reason") == "high_collateral_risk_flagged"


def test_narrow_replacement_drops_stale_stamps():
    """Phase 3 producer step 1: ``build_narrow_l6_replacement``
    produces a replacement carrying NO stale counterfactual-scan
    stamps."""
    cluster = _h002_cluster_from_fixture()
    target_qids = tuple(
        str(q) for q in (cluster.get("question_ids") or []) if str(q)
    )
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_for_h002(),
        ag_target_qids=target_qids,
        root_cause=str(cluster.get("root_cause") or ""),
    )
    assert replacement is not None, (
        "build_narrow_l6_replacement returned None for a broad L6 "
        "filter with a where_predicate; expected a non-None narrow "
        "variant."
    )

    # The three collateral-RISK stamps must be fully stripped.
    forbidden = (
        "high_collateral_risk",
        "high_collateral_risk_flagged",
        "passing_dependents_outside_target",
    )
    leaked = [k for k in forbidden if k in replacement]
    assert not leaked, (
        f"Phase 3 strip leaked stale stamps: {leaked}. "
        f"Replacement keys: {sorted(replacement.keys())}"
    )
    # ``passing_dependents`` is RE-STAMPED fresh (empty) rather than
    # left absent, so Trial 20 E2's mandatory-stamp gate evaluates the
    # query_id-scoped predicate as safe instead of rejecting it as
    # ``passing_dependents_missing``. It must NOT carry the broad
    # patch's stale dependent list.
    assert replacement.get("passing_dependents", []) == [], (
        f"Narrow replacement must carry a FRESH empty passing_dependents "
        f"(query_id-scoped ⇒ no outside dependents), not the broad "
        f"patch's stale list. Got: "
        f"{replacement.get('passing_dependents')!r}"
    )


def test_narrow_replacement_passes_retest_after_strip():
    """Phase 3 producer-consumer chain step 2: the narrow replacement,
    fed to ``patch_blast_radius_is_safe``, reports ``safe=True``."""
    cluster = _h002_cluster_from_fixture()
    target_qids = tuple(
        str(q) for q in (cluster.get("question_ids") or []) if str(q)
    )
    replacement = build_narrow_l6_replacement(
        original_patch=_broad_l6_filter_for_h002(),
        ag_target_qids=target_qids,
        root_cause=str(cluster.get("root_cause") or ""),
    )
    verdict = patch_blast_radius_is_safe(
        replacement,
        ag_target_qids=target_qids,
        max_outside_target=0,
        live_hard_qids=target_qids,
    )
    assert verdict["safe"] is True, (
        f"Narrow replacement retest FAILED: {verdict!r}. "
        f"Phase 3's strip should have made this pass."
    )


def test_h002_recovery_contract_holds_end_to_end():
    """The user-spec exit assertion: the H002 fixture replays to
    either an applied patch (narrow retest passes) or a retired
    signature — never to repeating the same broad shape in iter 4.

    This test exercises the applied-patch branch — the strip-enabled
    narrow replacement passes blast-radius and would be spliced
    back into ``_blast_kept`` per ``harness.py:~25881``.
    """
    cluster = _h002_cluster_from_fixture()
    target_qids = tuple(
        str(q) for q in (cluster.get("question_ids") or []) if str(q)
    )

    broad = _broad_l6_filter_for_h002()
    replacement = build_narrow_l6_replacement(
        original_patch=broad,
        ag_target_qids=target_qids,
        root_cause=str(cluster.get("root_cause") or ""),
    )
    retest = patch_blast_radius_is_safe(
        replacement,
        ag_target_qids=target_qids,
        max_outside_target=0,
        live_hard_qids=target_qids,
    )

    assert retest["safe"] is True, (
        f"H002 recovery contract violated: neither the broad patch "
        f"survived nor the narrow replacement's retest passed. "
        f"retest={retest!r}. Iter 4 would re-select the same broad "
        f"shape."
    )

    # Defensive: the narrowed predicate must still cover the H002
    # target QID via the query_id scope.
    predicate = str(replacement.get("where_predicate") or "")
    assert target_qids[0] in predicate, (
        f"Narrow predicate dropped the H002 target QID scope. "
        f"predicate={predicate!r}"
    )
