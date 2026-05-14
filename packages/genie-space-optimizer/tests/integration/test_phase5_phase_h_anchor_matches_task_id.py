"""Phase 5 synthetic gate: evidence-bundle rejects stale Phase H anchors.

Maps to user-text `test_phase_h_anchor_matches_task_id`.
Anchors the Phase 0.1 fix that closes the stale-anchor bug from
prior postmortems.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from genie_space_optimizer.tools.evidence_bundle import (
    EvidenceBundleStatus,
    resolve_phase_h_anchor,
)


def _write_phase_h_artifact(
    tmp_path: Path, *, task_run_id: str, payload_marker: str
) -> Path:
    """Write a synthetic Phase H replay fixture tagged with task_run_id."""
    artifact = {
        "marker": payload_marker,
        "lever_loop_task_run_id": task_run_id,
        "iterations": [{"iteration": 1, "accepted": True}],
    }
    p = tmp_path / f"phase_h_{task_run_id}.json"
    p.write_text(json.dumps(artifact))
    return p


def test_phase_h_anchor_match_succeeds(tmp_path: Path) -> None:
    artifact = _write_phase_h_artifact(
        tmp_path,
        task_run_id="task_X",
        payload_marker="PHASE_A_REPLAY_FIXTURE_JSON_1",
    )
    status = resolve_phase_h_anchor(
        artifact_path=artifact,
        resolved_task_run_id="task_X",
    )
    assert status == EvidenceBundleStatus.HEALTHY


def test_phase_h_anchor_mismatch_emits_stale_anchor(tmp_path: Path) -> None:
    artifact = _write_phase_h_artifact(
        tmp_path,
        task_run_id="task_X",
        payload_marker="PHASE_A_REPLAY_FIXTURE_JSON_1",
    )

    with pytest.raises(
        Exception, match="STALE_ANCHOR"
    ) as exc_info:
        resolve_phase_h_anchor(
            artifact_path=artifact,
            resolved_task_run_id="task_Y",
        )

    assert "task_X" in str(exc_info.value)
    assert "task_Y" in str(exc_info.value)


def test_phase_h_anchor_missing_artifact_emits_stale_anchor(
    tmp_path: Path,
) -> None:
    nonexistent = tmp_path / "does_not_exist.json"

    with pytest.raises(Exception, match="STALE_ANCHOR"):
        resolve_phase_h_anchor(
            artifact_path=nonexistent,
            resolved_task_run_id="task_Z",
        )
