"""Unit test for Plan 5 _upload_trial_captures_to_phase_h_anchor helper.

The helper:
  * Reads the four GSO sink default paths.
  * For each path that exists on disk, calls
    MlflowClient.log_artifact(anchor_run_id, local_path,
    artifact_path="gso_trial_captures").
  * No-ops when the anchor run id is None or empty.
  * No-ops when the file does not exist.
  * Never raises; logs warnings on failure.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest


def test_upload_skips_when_anchor_is_none(tmp_path):
    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id=None,
        capture_paths=[str(tmp_path / "x.ndjson")],
        client=client,
    )
    client.log_artifact.assert_not_called()


def test_upload_skips_when_anchor_is_empty(tmp_path):
    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id="",
        capture_paths=[str(tmp_path / "x.ndjson")],
        client=client,
    )
    client.log_artifact.assert_not_called()


def test_upload_skips_missing_files(tmp_path):
    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id="anchor-1",
        capture_paths=[str(tmp_path / "missing.ndjson")],
        client=client,
    )
    client.log_artifact.assert_not_called()


def test_upload_uploads_existing_files_under_correct_artifact_path(tmp_path):
    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    p1 = tmp_path / "narrowing_v1.ndjson"
    p1.write_text('{"hello": "world"}\n', encoding="utf-8")
    p2 = tmp_path / "raw_evidence_v1.ndjson"
    p2.write_text('{"plan": 4}\n', encoding="utf-8")
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id="anchor-1",
        capture_paths=[str(p1), str(p2)],
        client=client,
    )
    assert client.log_artifact.call_count == 2
    calls = client.log_artifact.call_args_list
    # Check positional + kwarg combinations because MlflowClient's API
    # takes (run_id, local_path, artifact_path).
    seen_local = {c.args[1] if len(c.args) > 1 else c.kwargs["local_path"]
                  for c in calls}
    seen_artifact_dirs = {
        (c.args[2] if len(c.args) > 2 else c.kwargs.get("artifact_path"))
        for c in calls
    }
    seen_run_ids = {c.args[0] if c.args else c.kwargs["run_id"] for c in calls}
    assert seen_local == {str(p1), str(p2)}
    assert seen_artifact_dirs == {"gso_trial_captures"}
    assert seen_run_ids == {"anchor-1"}


def test_upload_swallows_log_artifact_exceptions(tmp_path, caplog):
    from genie_space_optimizer.optimization.harness import (
        _upload_trial_captures_to_phase_h_anchor,
    )
    client = MagicMock()
    client.log_artifact.side_effect = RuntimeError("network down")
    p1 = tmp_path / "narrowing_v1.ndjson"
    p1.write_text("{}\n", encoding="utf-8")
    # MUST NOT raise — observability never breaks the optimizer.
    _upload_trial_captures_to_phase_h_anchor(
        anchor_run_id="anchor-1",
        capture_paths=[str(p1)],
        client=client,
    )
    # And the failure is logged.
    assert any("upload failed" in r.message.lower() for r in caplog.records)


def test_collect_capture_paths_from_summaries_returns_all_four():
    """Plan 5 — collect the four sink_path strings from the four
    dump_*_capture_summary() return values into a single list."""
    from genie_space_optimizer.optimization.harness import (
        _collect_trial_capture_paths,
    )
    paths = _collect_trial_capture_paths(
        narrowing_summary={"sink_path": "/tmp/gso_trial_captures/narrowing_v1.ndjson"},
        lever5_summary={"sink_path": "/tmp/gso_trial_captures/lever5_split_v1.ndjson"},
        three_stage_summary={"sink_path": "/tmp/gso_trial_captures/three_stage_v1.ndjson"},
        raw_evidence_summary={"sink_path": "/tmp/gso_trial_captures/raw_evidence_v1.ndjson"},
    )
    assert paths == [
        "/tmp/gso_trial_captures/narrowing_v1.ndjson",
        "/tmp/gso_trial_captures/lever5_split_v1.ndjson",
        "/tmp/gso_trial_captures/three_stage_v1.ndjson",
        "/tmp/gso_trial_captures/raw_evidence_v1.ndjson",
    ]


def test_collect_capture_paths_skips_none_summaries():
    """If a dump_* call swallowed an exception and returned None, that
    plan's path is omitted from the upload list (helpfully)."""
    from genie_space_optimizer.optimization.harness import (
        _collect_trial_capture_paths,
    )
    paths = _collect_trial_capture_paths(
        narrowing_summary=None,
        lever5_summary={"sink_path": "/tmp/gso_trial_captures/lever5_split_v1.ndjson"},
        three_stage_summary={"sink_path": ""},
        raw_evidence_summary={"sink_path": None},
    )
    assert paths == ["/tmp/gso_trial_captures/lever5_split_v1.ndjson"]
