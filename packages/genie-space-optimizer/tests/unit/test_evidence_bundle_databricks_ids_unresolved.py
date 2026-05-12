"""When the parsed ``GSO_RUN_MANIFEST_V*`` marker carries any
``"unknown"`` ID, the evidence bundler must record a
``DATABRICKS_IDS_UNRESOLVED`` ``MissingPiece`` so the postmortem
skill surfaces the resolver gap automatically.

Mirrors the captured failure shape from May-12 anchors
(31ecd96f-…, ccf1d60d-…) where every parent-bundle manifest had
all three IDs as the sentinel.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from genie_space_optimizer.tools.evidence_bundle import build_bundle
from genie_space_optimizer.tools.evidence_layout import (
    MissingPieceKind,
)


def _make_runners(stdout_text: str):
    db = mock.MagicMock()
    db.get_run.return_value = {
        "tasks": [
            {
                "task_key": "lever_loop",
                "run_id": "555",
                "state": {"result_state": "SUCCESS"},
            }
        ]
    }
    db.get_run_output.return_value = {"notebook_output": {"result": stdout_text}}

    ml = mock.MagicMock()
    ml.audit.return_value = {"anchor_run_id": "anchor-1", "sibling_runs": []}
    ml.download_artifacts.return_value = []

    return db, ml


def test_sentinel_databricks_ids_emit_missing_piece(tmp_path: Path) -> None:
    """Manifest carries all-sentinel IDs → bundler appends a
    ``DATABRICKS_IDS_UNRESOLVED`` ``MissingPiece``."""
    sentinel_marker = (
        'GSO_RUN_MANIFEST_V1 {"databricks_job_id":"unknown",'
        '"databricks_parent_run_id":"unknown","event":"start",'
        '"lever_loop_task_run_id":"unknown",'
        '"mlflow_experiment_id":"exp","optimization_run_id":"opt-1",'
        '"space_id":"space-1"}'
    )
    db, ml = _make_runners(sentinel_marker)

    result = build_bundle(
        job_id="42",
        run_id="100",
        profile="DEFAULT",
        output_root=tmp_path,
        databricks_runner=db,
        mlflow_runner=ml,
    )
    kinds = [piece.kind for piece in result.manifest.missing_pieces]
    assert MissingPieceKind.DATABRICKS_IDS_UNRESOLVED in kinds


def test_partial_sentinel_databricks_ids_still_emit_missing_piece(
    tmp_path: Path,
) -> None:
    """Even one sentinel field triggers the MissingPiece."""
    partial_marker = (
        'GSO_RUN_MANIFEST_V1 {"databricks_job_id":"42",'
        '"databricks_parent_run_id":"100",'
        '"event":"start","lever_loop_task_run_id":"unknown",'
        '"mlflow_experiment_id":"exp","optimization_run_id":"opt-2",'
        '"space_id":"space-1"}'
    )
    db, ml = _make_runners(partial_marker)

    result = build_bundle(
        job_id="42",
        run_id="100",
        profile="DEFAULT",
        output_root=tmp_path,
        databricks_runner=db,
        mlflow_runner=ml,
    )
    kinds = [piece.kind for piece in result.manifest.missing_pieces]
    assert MissingPieceKind.DATABRICKS_IDS_UNRESOLVED in kinds


def test_no_missing_piece_when_databricks_ids_fully_resolved(
    tmp_path: Path,
) -> None:
    """All three IDs non-sentinel → no MissingPiece emitted."""
    full_marker = (
        'GSO_RUN_MANIFEST_V1 {"databricks_job_id":"42",'
        '"databricks_parent_run_id":"100","event":"start",'
        '"lever_loop_task_run_id":"555",'
        '"mlflow_experiment_id":"exp","optimization_run_id":"opt-3",'
        '"space_id":"space-1"}'
    )
    db, ml = _make_runners(full_marker)

    result = build_bundle(
        job_id="42",
        run_id="100",
        profile="DEFAULT",
        output_root=tmp_path,
        databricks_runner=db,
        mlflow_runner=ml,
    )
    kinds = [piece.kind for piece in result.manifest.missing_pieces]
    assert MissingPieceKind.DATABRICKS_IDS_UNRESOLVED not in kinds
