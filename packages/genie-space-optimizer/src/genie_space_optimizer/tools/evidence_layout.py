"""Typed paths + manifest schema for the evidence bundle.

Single source of truth for the on-disk layout under
``runid_analysis/<opt_run_id>/``. Importing this module gives you the
exact path of every artifact the bundle and its consumers read or
write.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

_RUN_ID_RE = re.compile(r"^[A-Za-z0-9._\-]+$")


class MissingPieceKind(Enum):
    STDOUT_TRUNCATED = "STDOUT_TRUNCATED"
    STDOUT_FALLBACK_NOTEBOOK_OUTPUT = "STDOUT_FALLBACK_NOTEBOOK_OUTPUT"
    JOB_RUN_FETCH_FAILED = "JOB_RUN_FETCH_FAILED"
    MLFLOW_AUDIT_FAILED = "MLFLOW_AUDIT_FAILED"
    PHASE_A_ARTIFACT_MISSING_ON_ANCHOR = "PHASE_A_ARTIFACT_MISSING_ON_ANCHOR"
    PHASE_B_ARTIFACT_MISSING_ON_ANCHOR = "PHASE_B_ARTIFACT_MISSING_ON_ANCHOR"
    REPLAY_FIXTURE_NOT_IN_STDOUT = "REPLAY_FIXTURE_NOT_IN_STDOUT"
    OPTIMIZATION_RUN_ID_UNRESOLVED = "OPTIMIZATION_RUN_ID_UNRESOLVED"
    BACKFILL_FAILED = "BACKFILL_FAILED"


class TraceFetchReason(Enum):
    INCOMPLETE_DECISION_TRACE = "INCOMPLETE_DECISION_TRACE"
    UNRESOLVED_REASON_CODE = "UNRESOLVED_REASON_CODE"
    EVAL_FAILURE_WITHOUT_RCA = "EVAL_FAILURE_WITHOUT_RCA"
    JOURNEY_VIOLATION_WITHOUT_TRACE = "JOURNEY_VIOLATION_WITHOUT_TRACE"


@dataclass(frozen=True)
class MissingPiece:
    kind: MissingPieceKind
    iteration: int | None
    diagnosis: str
    suggested_action: str


@dataclass(frozen=True)
class TraceFetchRecommendation:
    reason: TraceFetchReason
    iteration: int | None
    trace_ids: tuple[str, ...]
    detail: str


@dataclass(frozen=True)
class BundlePaths:
    root: Path
    evidence_dir: Path
    manifest: Path
    job_run: Path
    stdout_dir: Path
    stderr_dir: Path
    markers: Path
    replay_fixture: Path
    mlflow_audit_md: Path
    mlflow_audit_json: Path
    mlflow_dir: Path
    traces_dir: Path
    postmortem: Path
    intake: Path
    # ── Phase H parent bundle ────────────────────────────────────
    # Populated by Phase H so evidence_bundle can materialize the
    # gso_postmortem_bundle/* artifacts pulled from the parent
    # lever-loop MLflow run alongside the legacy phase artifacts.
    parent_bundle_dir: Path = Path()
    parent_bundle_manifest: Path = Path()
    parent_bundle_artifact_index: Path = Path()
    parent_bundle_operator_transcript: Path = Path()
    parent_bundle_iterations_dir: Path = Path()


@dataclass(frozen=True)
class Manifest:
    schema_version: int
    bundle_version: int
    captured_at_utc: str
    inputs: Mapping[str, str]
    resolved: Mapping[str, Any]
    artifacts_pulled: Mapping[str, Any]
    missing_pieces: tuple[MissingPiece, ...] = field(default_factory=tuple)
    trace_fetch_recommendations: tuple[TraceFetchRecommendation, ...] = field(
        default_factory=tuple
    )
    exit_status: str = "complete"


def bundle_paths_for(*, root: Path, optimization_run_id: str) -> BundlePaths:
    if not _RUN_ID_RE.match(optimization_run_id or ""):
        raise ValueError(
            "optimization_run_id must match [A-Za-z0-9._-]+; got "
            f"{optimization_run_id!r}"
        )
    run_root = root / optimization_run_id
    evidence = run_root / "evidence"
    parent_bundle_dir = evidence / "gso_postmortem_bundle"
    return BundlePaths(
        root=run_root,
        evidence_dir=evidence,
        manifest=evidence / "manifest.json",
        job_run=evidence / "job_run.json",
        stdout_dir=evidence,
        stderr_dir=evidence,
        markers=evidence / "markers.json",
        replay_fixture=evidence / "replay_fixture.json",
        mlflow_audit_md=evidence / "mlflow_audit.md",
        mlflow_audit_json=evidence / "mlflow_audit.json",
        mlflow_dir=evidence / "mlflow",
        traces_dir=evidence / "traces",
        postmortem=run_root / "postmortem.md",
        intake=run_root / "intake.md",
        parent_bundle_dir=parent_bundle_dir,
        parent_bundle_manifest=parent_bundle_dir / "manifest.json",
        parent_bundle_artifact_index=parent_bundle_dir / "artifact_index.json",
        parent_bundle_operator_transcript=parent_bundle_dir / "operator_transcript.md",
        parent_bundle_iterations_dir=parent_bundle_dir / "iterations",
    )


def manifest_to_dict(manifest: Manifest) -> dict[str, Any]:
    raw = asdict(manifest)
    raw["missing_pieces"] = [
        {
            "kind": p.kind.value,
            "iteration": p.iteration,
            "diagnosis": p.diagnosis,
            "suggested_action": p.suggested_action,
        }
        for p in manifest.missing_pieces
    ]
    raw["trace_fetch_recommendations"] = [
        {
            "reason": r.reason.value,
            "iteration": r.iteration,
            "trace_ids": list(r.trace_ids),
            "detail": r.detail,
        }
        for r in manifest.trace_fetch_recommendations
    ]
    raw["resolved"] = dict(manifest.resolved)
    raw["resolved"]["sibling_mlflow_run_ids"] = list(
        manifest.resolved.get("sibling_mlflow_run_ids", ())
    )
    raw["artifacts_pulled"] = dict(manifest.artifacts_pulled)
    for k in ("stdout", "stderr", "mlflow_artifacts", "traces"):
        if k in raw["artifacts_pulled"]:
            raw["artifacts_pulled"][k] = list(raw["artifacts_pulled"][k])
    return raw


def manifest_from_dict(data: Mapping[str, Any]) -> Manifest:
    missing = tuple(
        MissingPiece(
            kind=MissingPieceKind(p["kind"]),
            iteration=p.get("iteration"),
            diagnosis=p["diagnosis"],
            suggested_action=p["suggested_action"],
        )
        for p in data.get("missing_pieces", ())
    )
    recs = tuple(
        TraceFetchRecommendation(
            reason=TraceFetchReason(r["reason"]),
            iteration=r.get("iteration"),
            trace_ids=tuple(r["trace_ids"]),
            detail=r["detail"],
        )
        for r in data.get("trace_fetch_recommendations", ())
    )
    resolved = dict(data["resolved"])
    if "sibling_mlflow_run_ids" in resolved:
        resolved["sibling_mlflow_run_ids"] = tuple(resolved["sibling_mlflow_run_ids"])
    artifacts = dict(data["artifacts_pulled"])
    for k in ("stdout", "stderr", "mlflow_artifacts", "traces"):
        if k in artifacts:
            artifacts[k] = tuple(artifacts[k])
    return Manifest(
        schema_version=data["schema_version"],
        bundle_version=data["bundle_version"],
        captured_at_utc=data["captured_at_utc"],
        inputs=dict(data["inputs"]),
        resolved=resolved,
        artifacts_pulled=artifacts,
        missing_pieces=missing,
        trace_fetch_recommendations=recs,
        exit_status=data.get("exit_status", "complete"),
    )
