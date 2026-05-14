"""Hand-authored derivation of Phase 5 31ecd96f fixtures.

Source: docs/runid_analysis/31ecd96f-5d56-4b5a-af8e-38e9e5c549af/postmortem.md
and the key_postmortem_facts JSON sidecar. The 31ecd96f postmortem
spans multiple lever_loop tasks; this script encodes the
directive-mismatch + collateral-drop variant (task 527064244842391).

This is a `python -c`-replaceable one-shot. Output JSONs are committed
to git so CI never re-runs the script.
"""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
OUT_ROOT = (
    REPO / "packages/genie-space-optimizer/tests/replay/fixtures/phase5"
)
RUN_ID = "31ecd96f-5d56-4b5a-af8e-38e9e5c549af"
SOURCE_TASK = "527064244842391"


def build_iter1_h001() -> dict:
    return {
        "run_id": RUN_ID,
        "source_task": SOURCE_TASK,
        "_source": "hand-authored from postmortem.md",
        "iteration": 1,
        "ag_id": "H001_gs009",
        "target_qids": ["gs_009"],
        "directives": {
            "L5": {
                "outcome": "no_structural_candidate",
                "reason": "no_metric_view_pattern_match",
            },
            "L6": {
                "outcome": "available",
                "patch_type_candidate": "add_metric_view",
            },
        },
        "rendered_proposal_kinds": ["L6_metric_view"],
        "violation": (
            "renderer emitted an L6 proposal while L5 directive said "
            "no_structural_candidate; the L6 must be its own AG (H002), "
            "not folded under H001."
        ),
    }


def build_iter2_collateral() -> dict:
    return {
        "run_id": RUN_ID,
        "source_task": SOURCE_TASK,
        "_source": "hand-authored from postmortem.md",
        "iteration": 2,
        "ag_id": "H002_gs024",
        "target_qids": ["gs_024"],
        "applied_patches": [],
        "dropped_patches": [
            {
                "patch_id": "L6_tkt_payment_revenue",
                "patch_type": "add_sql_snippet_measure",
                "target_table": "catalog.schema.tkt_payment",
                "drop_reason": "high_collateral_risk_flagged",
                "dropped_for_dependents": ["gs_003"],
            }
        ],
        "expected_narrow_replacement_call": {
            "helper": "build_narrow_l6_replacement",
            "protected_dependents": ["gs_003"],
        },
    }


def build_alternation() -> dict:
    return {
        "run_id": RUN_ID,
        "source_task": SOURCE_TASK,
        "_source": "hand-authored from postmortem.md",
        "ag_selection_sequence": ["H002", "H001", "H002"],
        "terminal_reason_sequence_for_h002": [
            "no_applied_patches",
            "no_applied_patches",
        ],
        "iteration_indices": [2, 3, 4],
        "h002_target_qids": ["gs_024"],
    }


def main() -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "31ecd96f_iter1_h001.json").write_text(
        json.dumps(build_iter1_h001(), indent=2, sort_keys=True)
    )
    (OUT_ROOT / "31ecd96f_iter2_collateral.json").write_text(
        json.dumps(build_iter2_collateral(), indent=2, sort_keys=True)
    )
    (OUT_ROOT / "31ecd96f_iter2_iter4_alternation.json").write_text(
        json.dumps(build_alternation(), indent=2, sort_keys=True)
    )
    print("wrote 3 fixtures to", OUT_ROOT)


if __name__ == "__main__":
    main()
