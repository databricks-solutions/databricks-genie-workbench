"""Unit test for ``scripts.replay_runid_fixture.replay_fixture_to_disk``."""
from __future__ import annotations

import json
from pathlib import Path

import pytest


_MINIMAL_FIXTURE = {
    "fixture_id": "test_fixture_v1",
    "iterations": [
        {
            "iteration": 1,
            "eval_rows": [
                {
                    "question_id": "q1",
                    "result_correctness": "yes",
                    "arbiter": "both_correct",
                }
            ],
            "clusters": [],
            "soft_clusters": [],
            "strategist_response": {"action_groups": []},
            "ag_outcomes": {},
            "post_eval_passing_qids": ["q1"],
            "journey_validation": None,
            "decision_records": [],
        }
    ],
}


def test_replay_fixture_to_disk_writes_four_files(tmp_path: Path) -> None:
    from genie_space_optimizer.scripts.replay_runid_fixture import (
        replay_fixture_to_disk,
    )

    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(json.dumps(_MINIMAL_FIXTURE))
    out = tmp_path / "analysis"

    result = replay_fixture_to_disk(fixture_path=fixture_path, analysis_dir=out)

    assert (out / "journey_validation.json").exists()
    assert (out / "canonical_journey.json").exists()
    assert (out / "canonical_decisions.json").exists()
    assert (out / "operator_transcript.md").exists()

    jv = json.loads((out / "journey_validation.json").read_text())
    assert jv["is_valid"] is True
    assert jv["violations"] == []

    canonical = (out / "canonical_journey.json").read_text().strip()
    assert canonical.startswith("[") and canonical.endswith("]")

    transcript = (out / "operator_transcript.md").read_text()
    assert "iterations: 1" in transcript
    assert "violations: 0" in transcript

    assert result.validation.is_valid


def test_replay_fixture_missing_file_raises(tmp_path: Path) -> None:
    from genie_space_optimizer.scripts.replay_runid_fixture import (
        replay_fixture_to_disk,
    )

    with pytest.raises(FileNotFoundError, match="fixture not found"):
        replay_fixture_to_disk(
            fixture_path=tmp_path / "missing.json",
            analysis_dir=tmp_path / "analysis",
        )
