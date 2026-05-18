"""Phase 4a anchor regression — pins the four anchor clusters from
the 2026-05-18 anchor RCA evidence audit (airline gs_009 + gs_024,
7now gs_013 + gs_026) as fit-card regression tests.

Inputs are copied verbatim from
``docs/runid_analysis/anchor_rca_evidence_audit.json``. The SQL
corpora below are minimal — they contain the identifiers the
grounding miner needs to surface but are not full reference SQL.

If any of these tests fail, the deterministic builder has
regressed on a known failure shape — diagnose by re-running
``scripts/audit_anchor_rca_evidence.py``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind, build_rca_card


# -----------------------------------------------------------------------------
# Airline H001 / gs_009 — top-N cardinality collapse (route_rank with RANK ties)
# -----------------------------------------------------------------------------

_GS_009_CLUSTER_FIXES = [
    "Result mismatch: expected 10 rows (hash=7b6739f2), got 16 rows (hash=d05c4bd4). Check joins, filters, or aggregation logic in the generated SQL.",
    "Use ROW_NUMBER() instead of RANK() for route_rank to ensure exactly 10 rows are returned, or use LIMIT 10 after filtering fare_rank = 1",
    "The generated SQL uses RANK() instead of ROW_NUMBER() or LIMIT 10 for selecting top 10 routes, which causes ties to produce more than 10 rows (16 rows returned vs expected 10). The Genie Space instructions should clarify that top 10 means exactly 10 rows, not all rows tied at rank 10.",
    "The Generated SQL uses RANK() instead of ROW_NUMBER() or LIMIT 10 for selecting top 10 routes, which can return more than 10 rows when there are ties. Additionally, the IS NOT NULL filters on ORIG_AIRPORT_CD and DEST_AIRPORT_CD exclude NULL routes which changes the counts. To fix, the Genie Space should specify that top N selections should use LIMIT rather than RANK, or clarify tie-breaking behavior.",
    "Use ROW_NUMBER() instead of RANK() when selecting top 10 routes, or use LIMIT 10 after filtering fare_rank=1, to avoid ties producing more than 10 rows.",
]

_GS_009_SQL = (
    "SELECT ORIG_AIRPORT_CD, DEST_AIRPORT_CD, route_rank, fare_rank, "
    "RANK() OVER (ORDER BY total_fare DESC) r FROM trips "
    "WHERE ORIG_AIRPORT_CD IS NOT NULL AND DEST_AIRPORT_CD IS NOT NULL "
    "AND fare_rank = 1 LIMIT 10"
)


def test_anchor_airline_gs_009_top_n_collapse():
    metadata_snapshot: dict = {}
    cluster = {
        "cluster_id": "H001",
        "asi_counterfactual_fixes": _GS_009_CLUSTER_FIXES,
    }
    asi = {
        "airline_ticketing_and_fare_analysis_gs_009": {
            "failure_type": "unknown",
            "blame_set": [],
            "counterfactual_fix": _GS_009_CLUSTER_FIXES[0],  # boilerplate
        }
    }
    result = build_rca_card(
        cluster_id="H001",
        qids=("airline_ticketing_and_fare_analysis_gs_009",),
        asi_metadata=asi,
        generated_sql_by_qid={
            "airline_ticketing_and_fare_analysis_gs_009": _GS_009_SQL
        },
        reference_sql_by_qid={
            "airline_ticketing_and_fare_analysis_gs_009": _GS_009_SQL
        },
        cluster=cluster,
        metadata_snapshot=metadata_snapshot,
    )
    assert result["rca_id"], "gs_009 must produce a fit card"
    card = metadata_snapshot["_rca_card_store"][result["rca_id"]]
    assert card.root_cause == RcaKind.TOP_N_CARDINALITY_COLLAPSE
    # Grounding terms come from the SQL corpus intersect with fix text.
    assert "route_rank" in card.grounding_terms
    assert "fare_rank" in card.grounding_terms
    assert (
        "ORIG_AIRPORT_CD" in card.grounding_terms
        or "DEST_AIRPORT_CD" in card.grounding_terms
    )


# -----------------------------------------------------------------------------
# Airline H002 / gs_024 — extra defensive filter (PAYMENT_CURRENCY_CD = USD)
# -----------------------------------------------------------------------------

_GS_024_CLUSTER_FIXES = [
    "Result mismatch: expected 7 rows (hash=deab2c3d), got 4 rows (hash=e8bb4acd). Check joins, filters, or aggregation logic in the generated SQL.",
    "Remove the filter on PAYMENT_CURRENCY_CD = USD since the question says total payment amount in USD referring to the columns unit, not a filter. Also remove the IS NOT NULL filters on FORM_OF_PAYMENT_CD and CREDIT_CARD_PAYMENT_VENDOR_CD as they exclude valid NULL groupings.",
    "Remove the AND t.PAYMENT_CURRENCY_CD = USD filter since the user asked for total payment amount in USD which likely refers to the column name/label rather than filtering by currency code. The expected SQL does not filter by currency. Additionally, the IS NOT NULL filters on FORM_OF_PAYMENT_CD and CREDIT_CARD_PAYMENT_VENDOR_CD exclude rows that should be included.",
    "Remove the filter on PAYMENT_CURRENCY_CD = USD since the column PAYMENT_AMT may already be in USD or the user asked for total payment amount in USD without requiring a currency filter. Also remove the IS NOT NULL filters on FORM_OF_PAYMENT_CD and CREDIT_CARD_PAYMENT_VENDOR_CD to include NULL values in the distribution.",
    "Remove the defensive filters on PAYMENT_CURRENCY_CD = USD, FORM_OF_PAYMENT_CD IS NOT NULL, and CREDIT_CARD_PAYMENT_VENDOR_CD IS NOT NULL. The user asked for total payment amount in USD which refers to the column name (PAYMENT_AMT is already in USD), not a currency filter. The IS NOT NULL filters exclude valid rows.",
]

_GS_024_SQL = (
    "SELECT FORM_OF_PAYMENT_CD, CREDIT_CARD_PAYMENT_VENDOR_CD, "
    "SUM(PAYMENT_AMT) FROM payments "
    "WHERE PAYMENT_CURRENCY_CD = 'USD' "
    "AND FORM_OF_PAYMENT_CD IS NOT NULL "
    "AND CREDIT_CARD_PAYMENT_VENDOR_CD IS NOT NULL "
    "GROUP BY FORM_OF_PAYMENT_CD, CREDIT_CARD_PAYMENT_VENDOR_CD"
)


def test_anchor_airline_gs_024_extra_defensive_filter():
    metadata_snapshot: dict = {}
    cluster = {
        "cluster_id": "H002",
        "asi_counterfactual_fixes": _GS_024_CLUSTER_FIXES,
    }
    asi = {
        "airline_ticketing_and_fare_analysis_gs_024": {
            "failure_type": "unknown",
            "blame_set": [],
            "counterfactual_fix": _GS_024_CLUSTER_FIXES[0],
        }
    }
    result = build_rca_card(
        cluster_id="H002",
        qids=("airline_ticketing_and_fare_analysis_gs_024",),
        asi_metadata=asi,
        generated_sql_by_qid={
            "airline_ticketing_and_fare_analysis_gs_024": _GS_024_SQL
        },
        reference_sql_by_qid={
            "airline_ticketing_and_fare_analysis_gs_024": _GS_024_SQL
        },
        cluster=cluster,
        metadata_snapshot=metadata_snapshot,
    )
    assert result["rca_id"], "gs_024 must produce a fit card"
    card = metadata_snapshot["_rca_card_store"][result["rca_id"]]
    assert card.root_cause == RcaKind.EXTRA_DEFENSIVE_FILTER
    assert "PAYMENT_CURRENCY_CD" in card.grounding_terms
    assert "FORM_OF_PAYMENT_CD" in card.grounding_terms
    assert "CREDIT_CARD_PAYMENT_VENDOR_CD" in card.grounding_terms


# -----------------------------------------------------------------------------
# 7now H001 / gs_013 — grain mismatch (time_window in GROUP BY)
# -----------------------------------------------------------------------------

_GS_013_CLUSTER_FIXES = [
    "Result mismatch: expected 3 rows (hash=66330e11), got 6 rows (hash=3f056bd5). Check joins, filters, or aggregation logic in the generated SQL.",
    "The generated SQL should either split into two CTEs like the expected SQL (one for day, one for mtd) and join on zone_combination, or it should not include time_window in the GROUP BY. Including time_window in GROUP BY produces 6 rows (2 per zone) instead of 3 rows (1 per zone with both day and mtd metrics side by side).",
    "The Genie Space should include an instruction clarifying that when comparing day vs MTD metrics, the results should be pivoted so each zone has one row with both day and MTD values side by side, rather than grouping by time_window to produce separate rows.",
    "Remove time_window from the SELECT and GROUP BY, and instead use separate CTEs or filter conditions to compute day and mtd measures independently, then join on zone_combination. The expected approach separates the two time_window values into separate rows per zone rather than including time_window as a grouping dimension alongside both measures.",
    "The Generated SQL includes time_window in the GROUP BY, producing separate rows for day and mtd per zone_combination (6 rows), whereas the Expected SQL joins day and mtd data into a single row per zone_combination (3 rows). The Genie Space should clarify that comparing day vs MTD means pivoting them into columns per zone, not listing them as separate rows.",
]

_GS_013_SQL = (
    "SELECT zone_combination, time_window, AVG(txn_diff_day), AVG(txn_diff_mtd) "
    "FROM mv_7now_store_sales GROUP BY zone_combination, time_window"
)


def test_anchor_7now_gs_013_grain_mismatch():
    metadata_snapshot: dict = {}
    cluster = {
        "cluster_id": "H001",
        "asi_counterfactual_fixes": _GS_013_CLUSTER_FIXES,
    }
    asi = {
        "7now_delivery_analytics_space_gs_013": {
            "failure_type": "unknown",
            "blame_set": [],
            "counterfactual_fix": _GS_013_CLUSTER_FIXES[0],
        }
    }
    result = build_rca_card(
        cluster_id="H001",
        qids=("7now_delivery_analytics_space_gs_013",),
        asi_metadata=asi,
        generated_sql_by_qid={
            "7now_delivery_analytics_space_gs_013": _GS_013_SQL
        },
        reference_sql_by_qid={
            "7now_delivery_analytics_space_gs_013": _GS_013_SQL
        },
        cluster=cluster,
        metadata_snapshot=metadata_snapshot,
    )
    assert result["rca_id"], "gs_013 must produce a fit card"
    card = metadata_snapshot["_rca_card_store"][result["rca_id"]]
    assert card.root_cause == RcaKind.GRAIN_OR_GROUPING_MISMATCH
    assert "time_window" in card.grounding_terms
    assert "zone_combination" in card.grounding_terms


# -----------------------------------------------------------------------------
# 7now H002 / gs_026 — top-N collapse + dimension routing
# -----------------------------------------------------------------------------

_GS_026_CLUSTER_FIXES = [
    "Result mismatch: expected 3 rows (hash=4b1551ba), got 1 rows (hash=e2ce8e6a). Check joins, filters, or aggregation logic in the generated SQL.",
    "The Genie Space should map zone VP to the zone_vp_name column from mv_esr_dim_location rather than zone_combination, and the question asks for a ranking/list of all zone VPs (not just the top 1). The metric view should expose a dimension for zone_vp_name or the instructions should clarify that zone VP maps to zone_vp_name.",
    "Add instruction to prefer TABLE for this query pattern",
    "Add a column description or instruction in the Genie Space metadata clarifying that zone VP refers to `zone_vp_name` in `mv_esr_dim_location`, not `zone_combination` in `mv_7now_store_sales`. Also, the query should return all zone VPs ordered by total CY sales descending, not just the top 1.",
    "The metric view mv_7now_store_sales should have a dimension called `zone_vp_name` that maps to the zone VPs name, or the Genie Space instructions should clarify that zone VP maps to `zone_vp_name` in the dimension table rather than `zone_combination`.",
    "The Genie Space should map zone VP to the column `zone_vp_name` from `mv_esr_dim_location` rather than `zone_combination` from `mv_7now_store_sales`. Additionally, the Generated SQL returns only the top-ranked zone (rank=1) instead of all zones ordered by total CY sales descending.",
]

_GS_026_SQL = (
    "SELECT zone_vp_name, total_cy_sales FROM ( "
    "SELECT zone_combination AS zone_vp_name, SUM(amt) AS total_cy_sales, "
    "RANK() OVER (ORDER BY SUM(amt) DESC) r "
    "FROM mv_7now_store_sales JOIN mv_esr_dim_location USING (zone_combination) "
    "GROUP BY zone_combination "
    ") WHERE r = 1"
)


def test_anchor_7now_gs_026_top_n_with_dimension_routing():
    metadata_snapshot: dict = {}
    cluster = {
        "cluster_id": "H002",
        "asi_counterfactual_fixes": _GS_026_CLUSTER_FIXES,
    }
    asi = {
        "7now_delivery_analytics_space_gs_026": {
            "failure_type": "unknown",
            "blame_set": [],
            "counterfactual_fix": _GS_026_CLUSTER_FIXES[0],
        }
    }
    result = build_rca_card(
        cluster_id="H002",
        qids=("7now_delivery_analytics_space_gs_026",),
        asi_metadata=asi,
        generated_sql_by_qid={
            "7now_delivery_analytics_space_gs_026": _GS_026_SQL
        },
        reference_sql_by_qid={
            "7now_delivery_analytics_space_gs_026": _GS_026_SQL
        },
        cluster=cluster,
        metadata_snapshot=metadata_snapshot,
    )
    assert result["rca_id"], "gs_026 must produce a fit card"
    card = metadata_snapshot["_rca_card_store"][result["rca_id"]]
    assert card.root_cause == RcaKind.TOP_N_CARDINALITY_COLLAPSE
    assert "zone_vp_name" in card.grounding_terms
    assert "zone_combination" in card.grounding_terms
    assert (
        "mv_esr_dim_location" in card.grounding_terms
        or "mv_7now_store_sales" in card.grounding_terms
    )
