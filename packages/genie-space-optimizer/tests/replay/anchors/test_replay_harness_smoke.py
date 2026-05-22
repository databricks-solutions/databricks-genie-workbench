"""Smoke test: replay harness loads an anchor fixture and produces a trajectory."""
import json
from pathlib import Path

from tests.replay.anchors.replay_harness import (
    AnchorReplayResult,
    run_anchor_replay,
)


def test_harness_returns_trajectory_for_dummy_fixture(tmp_path: Path):
    fixture_payload = {
        "qid": "gs_dummy",
        "eval_rows": [
            {
                "question_id": "gs_dummy",
                "feedback/result_correctness/value": "no",
                "feedback/arbiter/value": "wrong",
                "sql": "SELECT 1",
                "expected_shape": "ROW_NUMBER",
            }
        ],
        "expected_evidence_kind": "plural_top_n_collapse",
        "mocked_diagnosis": {
            "rca_kind_label": "plural_top_n_collapse",
            "evidence_summary": "top-N collapsed",
            "observed_failure": "1 row instead of N",
            "expected_sql_shape": "ROW_NUMBER over COUNT",
            "confidence": "high",
            "rca_card_id": "rca_dummy",
        },
        "mocked_proposal": {
            "intent_id": "intent_dummy",
            "patch_type": "add_sql_snippet_expression",
            "target_objects": ["t"],
            "target_qids": ["gs_dummy"],
            "rca_card_id": "rca_dummy",
            "causal_target": "ROW_NUMBER",
            "original_patch_body": "ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC)",
        },
    }
    fixture_path = tmp_path / "dummy_baseline.json"
    fixture_path.write_text(json.dumps(fixture_payload))

    result: AnchorReplayResult = run_anchor_replay(fixture_path)
    assert result.trajectory is not None
    assert result.trajectory.qid == "gs_dummy"
    assert result.trajectory.deepest_stage_ever.value in (
        "clustered", "proposed", "normalized", "applyable", "applied", "evaluated", "accepted",
    )
