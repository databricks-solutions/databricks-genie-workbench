"""Phase 0.1 — STALE_ANCHOR MissingPiece classification.

The evidence bundle must fail closed when no Phase H artifact's
``lever_loop_task_run_id`` matches the resolved lever_loop task,
rather than silently anchoring on a stale Phase H run (the airline
ff71c000 defect).
"""
from __future__ import annotations

from genie_space_optimizer.tools.evidence_layout import (
    MissingPiece,
    MissingPieceKind,
)
from genie_space_optimizer.tools.evidence_bundle import (
    detect_stale_phase_h_anchor,
)


def test_stale_anchor_enum_value_exists():
    """MissingPieceKind.STALE_ANCHOR must be a valid enum value."""
    assert MissingPieceKind.STALE_ANCHOR.value == "stale_anchor"


def test_detect_stale_phase_h_anchor_returns_none_when_matched():
    """When at least one Phase H sibling carries the same
    lever_loop_task_run_id as the chosen task, NO stale-anchor
    sentinel is emitted."""
    result = detect_stale_phase_h_anchor(
        chosen_task_run_id="9999",
        phase_h_sibling_task_run_ids=("9999", "8888"),
    )
    assert result is None


def test_detect_stale_phase_h_anchor_emits_when_no_match():
    """When NONE of the Phase H siblings match, emit STALE_ANCHOR
    with a diagnosis naming the chosen task and the candidates that
    did not match."""
    result = detect_stale_phase_h_anchor(
        chosen_task_run_id="9999",
        phase_h_sibling_task_run_ids=("1111", "2222"),
    )
    assert isinstance(result, MissingPiece)
    assert result.kind == MissingPieceKind.STALE_ANCHOR
    assert "9999" in result.diagnosis
    assert "1111" in result.diagnosis or "2222" in result.diagnosis


def test_detect_stale_phase_h_anchor_emits_when_no_siblings():
    """Empty sibling list also yields STALE_ANCHOR — the bundle
    cannot anchor on Phase H artifacts that don't exist."""
    result = detect_stale_phase_h_anchor(
        chosen_task_run_id="9999",
        phase_h_sibling_task_run_ids=(),
    )
    assert isinstance(result, MissingPiece)
    assert result.kind == MissingPieceKind.STALE_ANCHOR


def test_build_bundle_records_stale_anchor_when_no_phase_h_match(tmp_path):
    """When the resolved lever_loop task_run_id has NO matching
    Phase H sibling, the bundle manifest must include a
    STALE_ANCHOR MissingPiece AND must skip the Phase H artifact
    downloads."""
    from genie_space_optimizer.tools.evidence_bundle import build_bundle

    # Mlflow audit returns one sibling whose
    # ``genie.databricks.lever_loop_task_run_id`` tag points at a
    # *different* lever_loop task (1111) than the one selected from
    # the parent run's tasks list (9999). Phase H artifacts on this
    # sibling belong to a different run and must NOT be downloaded.
    download_calls: list[tuple[str, str]] = []

    class _Runner:
        def get_run(self, *, run_id, profile):
            return {
                "tasks": [
                    {
                        "task_key": "lever_loop",
                        "run_id": "9999",
                        "state": {"result_state": "SUCCESS"},
                        "end_time": 100,
                        "start_time": 50,
                    },
                ],
            }

        def get_run_output(self, *, run_id, profile):
            # Provide a valid GSO_RUN_MANIFEST_V1 marker so the audit
            # branch is actually exercised (otherwise the audit is
            # short-circuited by ``unresolved_<run_id>`` and the
            # stale-anchor check has no audit input).
            return {
                "logs": (
                    "GSO_RUN_MANIFEST_V1 "
                    '{"databricks_job_id":"j-1",'
                    '"databricks_parent_run_id":"r-1",'
                    '"event":"start",'
                    '"lever_loop_task_run_id":"9999",'
                    '"mlflow_experiment_id":"exp-1",'
                    '"optimization_run_id":"opt-stale",'
                    '"space_id":"sp-1"}\n'
                ),
                "error": "",
            }

    class _Mlflow:
        def audit(self, *, optimization_run_id, experiment_id):
            return {
                "anchor_run_id": "mlflow-anchor",
                "sibling_runs": [
                    {
                        "run_id": "mlflow-sibling-1",
                        "run_type": "lever_loop",
                        "tags": {
                            "genie.databricks.lever_loop_task_run_id": "1111",
                        },
                        "artifact_paths": [
                            "gso_postmortem_bundle/iterations/iter_1/x.json",
                        ],
                    },
                ],
                "missing_per_iteration": [],
            }

        def download_artifacts(self, *, run_id, artifact_path, dest):
            # Record any Phase H download attempt — the wiring must
            # skip these when STALE_ANCHOR is recorded.
            download_calls.append((run_id, artifact_path))
            return []

    result = build_bundle(
        job_id="job-1",
        run_id="parent-9999",
        profile="DEFAULT",
        output_root=tmp_path,
        databricks_runner=_Runner(),
        mlflow_runner=_Mlflow(),
    )
    kinds = [str(mp.kind.value) for mp in result.manifest.missing_pieces]
    assert "stale_anchor" in kinds, f"expected stale_anchor in {kinds}"
    # Fail-closed: the Phase H artifact download MUST be skipped when
    # STALE_ANCHOR is recorded — otherwise the postmortem skill would
    # consume mismatched artifacts.
    phase_h_calls = [
        c for c in download_calls
        if c[1].startswith("gso_postmortem_bundle/")
    ]
    assert phase_h_calls == [], (
        f"expected no Phase H downloads when stale anchor detected, "
        f"got {phase_h_calls}"
    )
