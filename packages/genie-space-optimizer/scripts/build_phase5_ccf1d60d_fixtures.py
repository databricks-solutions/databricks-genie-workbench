"""Hand-authored derivation of Phase 5 ccf1d60d fixtures.

Source: docs/runid_analysis/ccf1d60d-d686-467b-bafa-1640131b4393/postmortem.md
and postmortem.json. The per-stage evidence files under
`evidence/mlflow/<run>/gso_postmortem_bundle/iterations/iter_NN/stages/`
do not include the canonical `04_rca_card_assembly`,
`06_proposal_generation`, or `08_applyability` directories the
original plan-step-4 script expected, so the fixtures below are
hand-authored from the postmortem narrative (see F2/F3 in
postmortem.md and `summary` / `iteration_summary` in postmortem.json).

This is a `python -c`-replaceable one-shot. Output JSONs are committed
to git so CI never re-runs the script. It exists so a maintainer can
regenerate the fixtures after a postmortem refresh.
"""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
OUT_ROOT = (
    REPO / "packages/genie-space-optimizer/tests/replay/fixtures/phase5"
)
RUN_ID = "ccf1d60d-d686-467b-bafa-1640131b4393"
SOURCE_TASK = "576427031888570"  # selected_latest lever_loop task per postmortem.json


def build_iter1() -> dict:
    """Iter 1: AG1 targets gs_026; candidate gains aggregate but is rolled
    back because gs_026 stayed hard and gs_021 regressed out of target.

    Source: postmortem.md F2/F3 + postmortem.json iteration_summary[0].
    """
    return {
        "run_id": RUN_ID,
        "source_task": SOURCE_TASK,
        "_source": (
            "hand-authored from postmortem.md F2/F3 and "
            "postmortem.json iteration_summary[0]"
        ),
        "iteration": 1,
        "ag_id_selected": "AG1",
        "target_qids": ["gs_026"],
        "baseline_post_arbiter": 87.0,
        "candidate_post_arbiter": 90.9,
        "baseline_pre_arbiter": 56.5,
        "candidate_pre_arbiter": 63.6,
        "target_fixed_qids": [],
        "target_still_hard_qids": ["gs_026"],
        "out_of_target_regressed_qids": ["gs_021"],
        "accepted_in_recorded_run": False,
        "reason_code_in_recorded_run": "target_qids_not_improved",
        "applied_patch_count": 3,
        "proposal_count": 6,
    }


def build_iter2() -> dict:
    """Iter 2: same AG1 family re-selected, but 0 proposals emitted
    (the loop is stuck in an empty-proposal stall while H002/H003
    remain uncovered).

    Source: postmortem.md F5 + postmortem.json iteration_summary[1].
    """
    return {
        "run_id": RUN_ID,
        "source_task": SOURCE_TASK,
        "_source": (
            "hand-authored from postmortem.md F5 and "
            "postmortem.json iteration_summary[1]"
        ),
        "iteration": 2,
        "ag_id_selected": "AG1",
        "iter1_ag_id_selected": "AG1",
        "target_qids": ["gs_026"],
        "proposal_count": 0,
        "terminal_reason_in_recorded_run": "proposal_generation_empty",
        "coverage_gap_uncovered_clusters": ["H002", "H003"],
    }


def build_iter3() -> dict:
    """Iter 3: AG1 still selected; cluster_formation surfaces H001/H002/H003
    with H001 carrying the hard-failure ASI for gs_026 and H002/H003
    representing the still-uncovered gs_021 / gs_001 clusters.

    Source: postmortem.md F1/F5 + evidence iter_03 cluster_formation/output.json
    (3 clusters: H001/gs_026, H002/gs_021, H003/gs_001).
    """
    return {
        "run_id": RUN_ID,
        "source_task": SOURCE_TASK,
        "_source": (
            "hand-authored from postmortem.md F1/F5 cross-referenced with "
            "evidence iter_03 cluster_formation/output.json"
        ),
        "iteration": 3,
        "ag_id_selected": "AG1",
        "target_qids": ["gs_026"],
        "proposal_count": 0,
        "candidate_clusters": [
            {
                "cluster_id": "H001",
                "question_ids": ["gs_026"],
                "is_hard_failure": True,
            },
            {
                "cluster_id": "H002",
                "question_ids": ["gs_021"],
                "is_hard_failure": True,
            },
            {
                "cluster_id": "H003",
                "question_ids": ["gs_001"],
                "is_hard_failure": True,
            },
        ],
        "clusters_with_hard_asi": ["H001", "H002", "H003"],
        "soft_signal_clusters": [],
        "quarantined_qids": [
            "gs_009",
            "gs_019",
            "gs_021",
            "gs_026",
        ],
        "terminal_reason_in_recorded_run": "proposal_generation_empty",
    }


def build_rca_card_gs026() -> dict:
    """RCA card for gs_026 (H001 cluster).

    Source: postmortem.md F3 ("gs_026 still needs a structural SQL-shape
    repair"), repair_planner/ccf1d60d_gs026_repair_kit.json (canonical
    root_cause/grounding_terms), and rca_card/ccf1d60d_gs026_cluster.json.
    intended_patch_shape is "structural" per the F3 finding text and
    matches `test_rca_provisional_card.py::test_rca_provisional_card`
    expectations for SQL-shape root causes.
    """
    return {
        "run_id": RUN_ID,
        "source_task": SOURCE_TASK,
        "_source": (
            "hand-authored from postmortem.md F3 cross-referenced with "
            "repair_planner/ccf1d60d_gs026_repair_kit.json and "
            "rca_card/ccf1d60d_gs026_cluster.json"
        ),
        "target_qid": "gs_026",
        "cluster_id": "H001",
        "intended_patch_shape": "structural",
        "root_cause": "top_n_cardinality_collapse",
        "directives": [
            "preserve plural top-N cardinality",
            "route to mv_esr_dim_location for zone_vp_name",
            "do not collapse to RANK = 1",
        ],
        "grounding_terms": [
            "plural_top_n_collapse",
            "rank_eq_1",
            "zone_vp_name",
            "zone_combination",
        ],
        "allowed_patch_families": [
            "cardinality_preserving_top_n_guidance",
        ],
        "forbidden_patch_families": [
            "avoid_unrequested_defensive_filters",
            "filter_logic_guidance",
        ],
    }


def build_iter1_surviving_patches() -> dict:
    """The 3 patches that survived applyability gating in iter 1 (all
    metadata/instruction-shaped — the one L6 SQL-expression patch was
    dropped for broad collateral risk).

    Source: postmortem.md F3 ("two Lever 1 zone VP metadata patches and
    one Lever 5 asset-routing instruction") and the iteration_summary
    "Proposal / Gate Outcome" row in the postmortem table (line 61).

    Wrapped in a dict with a `run_id` so the Phase 5 fixture-shape
    parametrized test that calls `payload.get("run_id")` does not crash
    on this file. Downstream tasks read `payload["patches"]`.
    """
    patches = [
        {
            "patch_id": "L1_zone_vp_name_description",
            "patch_type": "update_column_description",
            "lever": "L1",
            "scope": {
                "column": "zone_vp_name",
                "table": "mv_esr_dim_location",
            },
            "rationale": (
                "Add description clarifying zone_vp_name is the zone VP "
                "column for ranking questions."
            ),
        },
        {
            "patch_id": "L1_zone_vp_name_synonym",
            "patch_type": "add_column_synonym",
            "lever": "L1",
            "scope": {
                "column": "zone_vp_name",
                "table": "mv_esr_dim_location",
            },
            "rationale": (
                "Add synonym so Genie maps 'zone VP' to zone_vp_name "
                "instead of zone_combination."
            ),
        },
        {
            "patch_id": "L5_asset_routing_instruction",
            "patch_type": "add_sql_snippet",
            "lever": "L5",
            "scope": {
                "instruction_kind": "asset_routing",
            },
            "rationale": (
                "Add ASSET ROUTING instruction to prefer TABLE for zone "
                "VP queries."
            ),
        },
    ]
    return {
        "run_id": RUN_ID,
        "source_task": SOURCE_TASK,
        "_source": (
            "hand-authored from postmortem.md F3 and iteration_summary "
            "row 1 (3 applied patches: zone_vp_name description, "
            "synonym, ASSET ROUTING instruction)"
        ),
        "_note": (
            "L6 SQL-expression patch was dropped for high collateral "
            "risk; it does NOT appear in the surviving set."
        ),
        "patches": patches,
    }


def main() -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "ccf1d60d_iter1.json").write_text(
        json.dumps(build_iter1(), indent=2, sort_keys=True) + "\n"
    )
    (OUT_ROOT / "ccf1d60d_iter2.json").write_text(
        json.dumps(build_iter2(), indent=2, sort_keys=True) + "\n"
    )
    (OUT_ROOT / "ccf1d60d_iter3.json").write_text(
        json.dumps(build_iter3(), indent=2, sort_keys=True) + "\n"
    )
    (OUT_ROOT / "ccf1d60d_rca_card_gs026.json").write_text(
        json.dumps(build_rca_card_gs026(), indent=2, sort_keys=True) + "\n"
    )
    (OUT_ROOT / "ccf1d60d_iter1_surviving_patches.json").write_text(
        json.dumps(
            build_iter1_surviving_patches(), indent=2, sort_keys=True
        )
        + "\n"
    )
    print("wrote 5 fixtures to", OUT_ROOT)


if __name__ == "__main__":
    main()
