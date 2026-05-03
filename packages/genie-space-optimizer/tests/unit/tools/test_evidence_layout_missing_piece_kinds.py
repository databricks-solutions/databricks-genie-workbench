"""Pin enum values so renames are caught by CI before they reach disk."""
from __future__ import annotations

from genie_space_optimizer.tools.evidence_layout import MissingPieceKind


def test_missing_piece_kind_values_pinned():
    expected = {
        "STDOUT_TRUNCATED",
        "STDOUT_FALLBACK_NOTEBOOK_OUTPUT",
        "JOB_RUN_FETCH_FAILED",
        "MLFLOW_AUDIT_FAILED",
        "PHASE_A_ARTIFACT_MISSING_ON_ANCHOR",
        "PHASE_B_ARTIFACT_MISSING_ON_ANCHOR",
        "REPLAY_FIXTURE_NOT_IN_STDOUT",
        "OPTIMIZATION_RUN_ID_UNRESOLVED",
        "BACKFILL_FAILED",
    }
    actual = {k.name for k in MissingPieceKind}
    assert actual == expected, (
        f"missing: {expected - actual}; unexpected: {actual - expected}"
    )
