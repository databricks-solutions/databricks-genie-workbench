"""Regression replay: the 7now iteration-1 high-blast-radius bundle.

Iteration 1 shipped 3 patches (column description, add_instruction,
add_sql_snippet_filter). The filter snippet was flagged by the
counterfactual scan with 10+ passing dependents and broke q001
(previously a soft failure) into a hard failure. This frozen replay
asserts the new gate drops the broad SQL filter snippet before the
patch cap so q001 cannot regress.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.proposal_grounding import (
    patch_blast_radius_is_safe,
)


def _patches_for_replay() -> list[dict]:
    """Mimic the cap-input shape from the 7now iter-1 log."""
    return [
        {
            "proposal_id": "P001#1",
            "type": "update_column_description",
            "target": "prashanth_subrahmanyam_catalog.sales_reports.mv_7now_fact_sales",
            "column": "time_window",
            "passing_dependents": [
                "q002", "q004", "q006", "q008", "q030",
                "q014", "q018", "q022", "q010", "q017",
            ],
            "high_collateral_risk": True,
        },
        {
            "proposal_id": "P006#1",
            "type": "add_instruction",
            "target": "H002",
            "passing_dependents": [],
        },
        {
            "proposal_id": "P007#1",
            "type": "add_sql_snippet_filter",
            "target": "prashanth_subrahmanyam_catalog.sales_reports.mv_7now_fact_sales",
            "passing_dependents": [
                "q001", "q002", "q004", "q006", "q008",
                "q030", "q014", "q018", "q022", "q010",
            ],
            "high_collateral_risk": True,
        },
    ]


def test_iteration1_blast_radius_gate_drops_broad_filter_and_column(
    monkeypatch,
) -> None:
    """Legacy uniform-rejection regression replay — pinned with the
    lever-aware flag explicitly disabled. The default-on lever-aware
    contract (non-semantic patches downgrade to a warning) is pinned
    by ``test_blast_radius_lever_aware.py``."""
    monkeypatch.setenv("GSO_LEVER_AWARE_BLAST_RADIUS", "0")
    target_qids = ("q009", "q021")
    decisions = [
        (
            patch.get("proposal_id"),
            patch_blast_radius_is_safe(
                patch,
                ag_target_qids=target_qids,
                max_outside_target=0,
            ),
        )
        for patch in _patches_for_replay()
    ]
    by_id = {pid: d for pid, d in decisions}
    assert by_id["P006#1"]["safe"] is True
    assert by_id["P001#1"]["safe"] is False
    assert by_id["P001#1"]["reason"] == "high_collateral_risk_flagged"
    assert "q001" not in by_id["P001#1"]["passing_dependents_outside_target"]
    assert by_id["P007#1"]["safe"] is False
    assert by_id["P007#1"]["reason"] == "high_collateral_risk_flagged"
    assert "q001" in by_id["P007#1"]["passing_dependents_outside_target"]


def test_replay_bundle_after_blast_radius_gate_excludes_broad_filter_snippet(
    monkeypatch,
) -> None:
    """Simulate the harness-style filter loop and assert the
    survivors. Pinned to the legacy uniform-rejection contract;
    lever-aware behaviour is pinned by
    ``test_blast_radius_lever_aware.py``."""
    monkeypatch.setenv("GSO_LEVER_AWARE_BLAST_RADIUS", "0")
    target_qids = ("q009", "q021")
    patches = _patches_for_replay()

    survivors = []
    dropped_reasons = {}
    for p in patches:
        decision = patch_blast_radius_is_safe(
            p, ag_target_qids=target_qids, max_outside_target=0,
        )
        if decision["safe"]:
            survivors.append(p)
        else:
            dropped_reasons[p["proposal_id"]] = decision["reason"]

    survivor_ids = sorted(s["proposal_id"] for s in survivors)
    assert "P006#1" in survivor_ids, "low-risk add_instruction must survive"
    assert "P007#1" not in survivor_ids, (
        "broad add_sql_snippet_filter must be rejected — this is the "
        "exact patch that broke q001 in the 7now iter-1 run"
    )
    assert "P001#1" not in survivor_ids, (
        "high-risk update_column_description on a 10+ dependent table "
        "must also be rejected"
    )
    assert dropped_reasons["P007#1"] == "high_collateral_risk_flagged"
    assert dropped_reasons["P001#1"] == "high_collateral_risk_flagged"
