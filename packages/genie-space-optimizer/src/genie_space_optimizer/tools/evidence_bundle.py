"""Evidence bundle CLI: ``(job_id, run_id) → on-disk evidence/`` for a GSO run.

Read-only orchestrator. Pulls Databricks job state, task stdout/stderr,
parses stdout markers, runs the MLflow audit (Phase E.0), downloads
sibling-run decision-trail artifacts, and (optionally) auto-backfills
missing artifacts. Writes a typed ``manifest.json`` describing every
artifact pulled and every missing piece.

Idempotent: re-running fills gaps without re-pulling existing files.

Trace pulls are *not* part of this CLI. Use
``genie_space_optimizer.tools.trace_fetcher`` when the analysis skill
determines bundle artifacts are insufficient.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from genie_space_optimizer.tools.evidence_layout import (
    BundlePaths,
    Manifest,
    MissingPiece,
    MissingPieceKind,
    TraceFetchReason,
    TraceFetchRecommendation,
    bundle_paths_for,
    manifest_from_dict,
    manifest_to_dict,
)
from genie_space_optimizer.tools.marker_parser import (
    extract_replay_fixture,
    parse_markers,
)

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
BUNDLE_VERSION = 1


class DatabricksRunner(Protocol):
    def get_run(self, *, run_id: str, profile: str) -> Mapping[str, Any]: ...
    def get_run_output(self, *, run_id: str, profile: str) -> Mapping[str, Any]: ...


class MlflowRunner(Protocol):
    def audit(self, *, optimization_run_id: str, experiment_id: str) -> Mapping[str, Any]: ...
    def download_artifacts(
        self, *, run_id: str, artifact_path: str, dest: Path
    ) -> Sequence[Path]: ...


@dataclass
class BundleResult:
    paths: BundlePaths
    manifest: Manifest


def _extract_stdout_with_fallback(
    out: Mapping[str, Any] | dict[str, Any],
) -> tuple[str, str, MissingPiece | None]:
    """Resolve lever-loop stdout from a Databricks ``get-run-output`` payload.

    Resolution order:
        1. ``out["logs"]``  — populated for ``python_wheel_task`` /
           ``spark_python_task`` runs.
        2. ``out["notebook_output"]["result"]`` — populated for
           ``notebook_task`` runs (logs is empty by API contract).
        3. Empty string if neither is populated.

    Returns ``(stdout_text, source, missing_piece)``. ``source`` is one
    of ``"logs"``, ``"notebook_output.result"``, or ``"absent"``. A
    ``STDOUT_FALLBACK_NOTEBOOK_OUTPUT`` ``MissingPiece`` is returned
    when fallback is used so postmortems make the source explicit.
    """
    logs_text = str((out or {}).get("logs") or "")
    if logs_text:
        return logs_text, "logs", None

    notebook_output = (out or {}).get("notebook_output") or {}
    result_text = str(notebook_output.get("result") or "")
    truncated = bool(notebook_output.get("truncated"))
    if result_text:
        suffix = " (truncated by Databricks)" if truncated else ""
        diagnosis = (
            "logs field empty (notebook task — Databricks Jobs API does "
            "not populate `logs` for notebook_task; the real stdout is in "
            "notebook_output.result). Falling back to notebook_output."
            f"result{suffix}."
        )
        suggested = (
            "no operator action required. The marker parser and replay "
            "extractor consume the same string regardless of source."
        )
        return (
            result_text,
            "notebook_output.result",
            MissingPiece(
                kind=MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT,
                iteration=None,
                diagnosis=diagnosis,
                suggested_action=suggested,
            ),
        )

    return "", "absent", None


def _markers_to_json(markers: Any) -> str:
    return json.dumps(
        {
            "run_manifest": markers.run_manifest,
            "iteration_summaries": list(markers.iteration_summaries),
            "phase_b": list(markers.phase_b),
            "phase_b_no_records": list(markers.phase_b_no_records),
            "phase_a_artifact": list(markers.phase_a_artifact),
            "phase_b_artifact": list(markers.phase_b_artifact),
            "convergence": markers.convergence,
            "unknown": {k: list(v) for k, v in markers.unknown.items()},
            "parse_errors": list(markers.parse_errors),
        },
        indent=2,
        sort_keys=True,
    )


def _render_audit_markdown(audit: Mapping[str, Any]) -> str:
    lines = ["# MLflow Audit", ""]
    lines.append(f"Anchor run: `{audit.get('anchor_run_id', '')}`")
    lines.append("")
    lines.append("## Sibling runs")
    for sib in audit.get("sibling_runs", []):
        lines.append(f"- `{sib['run_id']}` (`{sib.get('run_type', '?')}`)")
        for path in sib.get("artifact_paths", []):
            lines.append(f"  - {path}")
    lines.append("")
    lines.append("## Missing per iteration")
    for entry in audit.get("missing_per_iteration", []):
        lines.append(
            f"- iter {entry.get('iteration', '?')}: "
            f"{entry.get('kind')} on `{entry.get('anchor_run_id', '')}`"
        )
    return "\n".join(lines) + "\n"


def _derive_trace_fetch_recommendations(
    *, mlflow_dir: Path
) -> tuple[TraceFetchRecommendation, ...]:
    recommendations: list[TraceFetchRecommendation] = []
    for trace_file in mlflow_dir.rglob("phase_b/decision_trace/iter_*.json"):
        try:
            data = json.loads(trace_file.read_text())
        except Exception:  # noqa: BLE001
            continue
        iteration = data.get("iteration")
        unresolved_trace_ids: list[str] = []
        unresolved_reasons = 0
        for decision in data.get("decisions", []):
            reason = decision.get("reason_code", "")
            if reason in {"UNKNOWN", "UNCLASSIFIED", ""} and decision.get(
                "outcome"
            ) in {"ABANDONED", "ROLLED_BACK", "FAILED"}:
                unresolved_reasons += 1
                for ref in decision.get("evidence_refs", []):
                    tid = ref.get("trace_id") if isinstance(ref, dict) else None
                    if tid:
                        unresolved_trace_ids.append(tid)
        if unresolved_reasons and unresolved_trace_ids:
            recommendations.append(
                TraceFetchRecommendation(
                    reason=TraceFetchReason.UNRESOLVED_REASON_CODE,
                    iteration=iteration,
                    trace_ids=tuple(sorted(set(unresolved_trace_ids))),
                    detail=(
                        f"reason_code in {{UNKNOWN, UNCLASSIFIED, ''}} on "
                        f"{unresolved_reasons} terminal decisions"
                    ),
                )
            )
    return tuple(recommendations)


_AUDIT_KIND_MAP = {
    "PHASE_A_JOURNEY_VALIDATION": MissingPieceKind.PHASE_A_ARTIFACT_MISSING_ON_ANCHOR,
    "PHASE_B_DECISION_TRACE": MissingPieceKind.PHASE_B_ARTIFACT_MISSING_ON_ANCHOR,
    "PHASE_B_OPERATOR_TRANSCRIPT": MissingPieceKind.PHASE_B_ARTIFACT_MISSING_ON_ANCHOR,
}


def _walk_audit_artifacts(
    *,
    audit: Mapping[str, Any],
    mlflow_runner: MlflowRunner,
    paths: BundlePaths,
    diagnosis_prefix: str = "audit reports",
) -> tuple[list[str], list[dict], list[MissingPiece], str]:
    """Download decision-trail artifacts referenced by the audit + collect gaps."""
    sibling_run_ids: list[str] = []
    pulled_artifacts: list[dict] = []
    missing: list[MissingPiece] = []
    anchor_run_id = audit.get("anchor_run_id", "")
    for sibling in audit.get("sibling_runs", []):
        sibling_run_ids.append(sibling["run_id"])
        for artifact_path in sibling.get("artifact_paths", []):
            if not (
                artifact_path.startswith("phase_a/")
                or artifact_path.startswith("phase_b/")
            ):
                continue
            dest = paths.mlflow_dir / sibling["run_id"]
            try:
                files = mlflow_runner.download_artifacts(
                    run_id=sibling["run_id"],
                    artifact_path=artifact_path,
                    dest=dest,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "download_artifacts failed for %s/%s: %s",
                    sibling["run_id"],
                    artifact_path,
                    exc,
                )
                continue
            for f in files:
                pulled_artifacts.append(
                    {
                        "run_id": sibling["run_id"],
                        "path": str(f.relative_to(paths.root)),
                        "size_bytes": f.stat().st_size,
                    }
                )
    for entry in audit.get("missing_per_iteration", []):
        kind = _AUDIT_KIND_MAP.get(entry.get("kind", ""))
        if kind is None:
            continue
        missing.append(
            MissingPiece(
                kind=kind,
                iteration=entry.get("iteration"),
                diagnosis=(
                    f"{diagnosis_prefix} {entry['kind']} missing on anchor run "
                    f"{entry.get('anchor_run_id', anchor_run_id)} for iteration "
                    f"{entry.get('iteration')}."
                ),
                suggested_action=(
                    "run mlflow_backfill with --fixture <evidence/replay_fixture.json>, "
                    "or rerun bundle with --auto-backfill."
                ),
            )
        )
    return sibling_run_ids, pulled_artifacts, missing, anchor_run_id


def build_bundle(
    *,
    job_id: str,
    run_id: str,
    profile: str,
    output_root: Path,
    databricks_runner: DatabricksRunner,
    mlflow_runner: MlflowRunner,
    auto_backfill: bool = False,
    opt_run_id_override: str = "",
    experiment_id_override: str = "",
) -> BundleResult:
    job_run = databricks_runner.get_run(run_id=run_id, profile=profile)

    lever_task = next(
        (t for t in job_run.get("tasks", []) if t.get("task_key") == "lever_loop"),
        None,
    )
    lever_task_run_id = lever_task["run_id"] if lever_task else ""
    stdout_text = ""
    stderr_text = ""
    stdout_source = "absent"
    stdout_fallback_missing: MissingPiece | None = None
    if lever_task_run_id:
        out = databricks_runner.get_run_output(
            run_id=lever_task_run_id, profile=profile
        )
        (
            stdout_text,
            stdout_source,
            stdout_fallback_missing,
        ) = _extract_stdout_with_fallback(out)
        stderr_text = str(out.get("error", "") or "")

    markers = parse_markers(stdout_text)
    # opt_run_id resolution order:
    #   1. operator override via --opt-run-id (used when the harness on
    #      the workspace pre-dates the GSO_RUN_MANIFEST_V1 emitter and
    #      stdout markers are absent)
    #   2. parsed GSO_RUN_MANIFEST_V1 marker
    #   3. placeholder "unresolved_<run_id>"
    if opt_run_id_override:
        optimization_run_id = opt_run_id_override
    else:
        optimization_run_id = markers.optimization_run_id() or f"unresolved_{run_id}"
    paths = bundle_paths_for(root=output_root, optimization_run_id=optimization_run_id)

    # Idempotence: short-circuit when an existing manifest matches inputs.
    if paths.manifest.exists():
        try:
            existing = json.loads(paths.manifest.read_text())
            if existing.get("inputs") == {
                "job_id": job_id,
                "run_id": run_id,
                "profile": profile,
            }:
                return BundleResult(
                    paths=paths, manifest=manifest_from_dict(existing)
                )
        except Exception:  # noqa: BLE001
            pass  # fall through to a full rebuild

    paths.evidence_dir.mkdir(parents=True, exist_ok=True)
    paths.mlflow_dir.mkdir(parents=True, exist_ok=True)

    paths.job_run.write_text(json.dumps(job_run, indent=2, sort_keys=True, default=str))
    if stdout_text:
        (paths.evidence_dir / "lever_loop_stdout.txt").write_text(stdout_text)
    if stderr_text:
        (paths.evidence_dir / "lever_loop_stderr.txt").write_text(stderr_text)

    paths.markers.write_text(_markers_to_json(markers))

    missing: list[MissingPiece] = []
    if stdout_fallback_missing is not None:
        missing.append(stdout_fallback_missing)
    if markers.optimization_run_id() is None and not opt_run_id_override:
        missing.append(
            MissingPiece(
                kind=MissingPieceKind.OPTIMIZATION_RUN_ID_UNRESOLVED,
                iteration=None,
                diagnosis=(
                    "no GSO_RUN_MANIFEST_V1 marker found in lever_loop stdout; "
                    "optimization_run_id pinned to placeholder "
                    f"'unresolved_{run_id}'."
                ),
                suggested_action=(
                    "verify the lever_loop task ran the harness with the run-manifest "
                    "marker emitter enabled, or pass --opt-run-id explicitly."
                ),
            )
        )

    fixture = extract_replay_fixture(stdout_text)
    if fixture is not None:
        paths.replay_fixture.write_text(json.dumps(fixture, indent=2, sort_keys=True))
    else:
        missing.append(
            MissingPiece(
                kind=MissingPieceKind.REPLAY_FIXTURE_NOT_IN_STDOUT,
                iteration=None,
                diagnosis=(
                    "PHASE_A replay fixture markers absent from lever_loop stdout; "
                    "intake skill cannot source via bundle:// for this run."
                ),
                suggested_action=(
                    "rerun the harness with the replay-fixture emitter enabled, "
                    "or pass an explicit fixture path to gso-replay-cycle-intake."
                ),
            )
        )

    audit: Mapping[str, Any] = {}
    sibling_run_ids: list[str] = []
    pulled_artifacts: list[dict] = []
    anchor_run_id = ""
    # experiment_id resolution: explicit override beats marker-derived.
    # The audit accepts experiment_id="" / None and searches every
    # experiment, so the audit can run even when the experiment is not
    # known up-front (slower but thorough).
    experiment_id = (
        experiment_id_override
        or (markers.run_manifest or {}).get("mlflow_experiment_id", "")
    )
    if optimization_run_id and not optimization_run_id.startswith("unresolved_"):
        try:
            audit = mlflow_runner.audit(
                optimization_run_id=optimization_run_id,
                experiment_id=experiment_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("mlflow audit failed: %s", exc)
            missing.append(
                MissingPiece(
                    kind=MissingPieceKind.MLFLOW_AUDIT_FAILED,
                    iteration=None,
                    diagnosis=f"{type(exc).__name__}: {exc}",
                    suggested_action=(
                        "rerun bundle with --profile pointing at the workspace "
                        "owning this experiment."
                    ),
                )
            )

    if audit:
        sibling_run_ids, pulled_artifacts, audit_missing, anchor_run_id = (
            _walk_audit_artifacts(
                audit=audit, mlflow_runner=mlflow_runner, paths=paths
            )
        )
        missing.extend(audit_missing)
        paths.mlflow_audit_json.write_text(json.dumps(audit, indent=2, sort_keys=True))
        paths.mlflow_audit_md.write_text(_render_audit_markdown(audit))

    # Auto-backfill branch: try to fill PHASE_*_ARTIFACT_MISSING_ON_ANCHOR
    # by invoking mlflow_backfill once and re-running the audit/download.
    decision_trail_gaps = [
        m
        for m in missing
        if m.kind
        in {
            MissingPieceKind.PHASE_A_ARTIFACT_MISSING_ON_ANCHOR,
            MissingPieceKind.PHASE_B_ARTIFACT_MISSING_ON_ANCHOR,
        }
    ]
    if (
        auto_backfill
        and decision_trail_gaps
        and paths.replay_fixture.exists()
        and hasattr(mlflow_runner, "backfill")
    ):
        try:
            mlflow_runner.backfill(
                optimization_run_id=optimization_run_id,
                fixture_path=paths.replay_fixture,
                anchor_run_id=anchor_run_id,
            )
            audit = mlflow_runner.audit(
                optimization_run_id=optimization_run_id,
                experiment_id=experiment_id,
            )
            # Drop stale decision-trail gaps; re-walk audit.
            missing = [m for m in missing if m not in decision_trail_gaps]
            sibling_run_ids, pulled_artifacts, audit_missing, anchor_run_id = (
                _walk_audit_artifacts(
                    audit=audit,
                    mlflow_runner=mlflow_runner,
                    paths=paths,
                    diagnosis_prefix="still missing after backfill;",
                )
            )
            # Override the diagnosis suggested_action for post-backfill gaps.
            audit_missing = [
                MissingPiece(
                    kind=m.kind,
                    iteration=m.iteration,
                    diagnosis="still missing after backfill",
                    suggested_action=(
                        "inspect mlflow_backfill stdout; investigate fixture content."
                    ),
                )
                for m in audit_missing
            ]
            missing.extend(audit_missing)
            paths.mlflow_audit_json.write_text(
                json.dumps(audit, indent=2, sort_keys=True)
            )
            paths.mlflow_audit_md.write_text(_render_audit_markdown(audit))
        except Exception as exc:  # noqa: BLE001
            missing.append(
                MissingPiece(
                    kind=MissingPieceKind.BACKFILL_FAILED,
                    iteration=None,
                    diagnosis=f"{type(exc).__name__}: {exc}",
                    suggested_action=(
                        "rerun bundle without --auto-backfill and inspect mlflow_audit.md."
                    ),
                )
            )

    recommendations = _derive_trace_fetch_recommendations(mlflow_dir=paths.mlflow_dir)

    manifest = Manifest(
        schema_version=SCHEMA_VERSION,
        bundle_version=BUNDLE_VERSION,
        captured_at_utc=dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        inputs={"job_id": job_id, "run_id": run_id, "profile": profile},
        resolved={
            "optimization_run_id": optimization_run_id,
            "lever_loop_task_run_id": lever_task_run_id,
            "mlflow_experiment_id": experiment_id,
            "anchor_mlflow_run_id": anchor_run_id,
            "sibling_mlflow_run_ids": tuple(sibling_run_ids),
        },
        artifacts_pulled={
            "job_run": "evidence/job_run.json",
            "stdout": ("evidence/lever_loop_stdout.txt",) if stdout_text else (),
            "stderr": ("evidence/lever_loop_stderr.txt",) if stderr_text else (),
            "markers": "evidence/markers.json",
            "replay_fixture": "evidence/replay_fixture.json" if fixture is not None else "",
            "mlflow_audit_md": "evidence/mlflow_audit.md" if audit else "",
            "mlflow_audit_json": "evidence/mlflow_audit.json" if audit else "",
            "mlflow_artifacts": tuple(pulled_artifacts),
            "traces": (),
            "stdout_source": stdout_source,
        },
        missing_pieces=tuple(missing),
        trace_fetch_recommendations=recommendations,
        exit_status="incomplete" if missing else "complete",
    )
    paths.manifest.write_text(json.dumps(manifest_to_dict(manifest), indent=2, sort_keys=True))
    return BundleResult(paths=paths, manifest=manifest)


def _default_databricks_runner() -> DatabricksRunner:
    from genie_space_optimizer.tools._databricks_cli import DatabricksCliRunner

    return DatabricksCliRunner()


def _default_mlflow_runner() -> MlflowRunner:
    from genie_space_optimizer.tools._mlflow_runner import DefaultMlflowRunner

    return DefaultMlflowRunner()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="evidence-bundle")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument(
        "--output-dir",
        default="packages/genie-space-optimizer/docs/runid_analysis",
        type=Path,
    )
    parser.add_argument("--auto-backfill", action="store_true")
    parser.add_argument(
        "--opt-run-id",
        default="",
        help=(
            "Optimization run ID override. Use when the lever_loop stdout "
            "has no GSO_RUN_MANIFEST_V1 marker (e.g., the workspace harness "
            "pre-dates the marker emitter). The opt_run_id is recoverable "
            "from job_run.job_parameters[].run_id."
        ),
    )
    parser.add_argument(
        "--experiment-id",
        default="",
        help=(
            "MLflow experiment ID override. Optional even with --opt-run-id; "
            "the audit will search every experiment when unset."
        ),
    )
    args = parser.parse_args(argv)

    result = build_bundle(
        job_id=args.job_id,
        run_id=args.run_id,
        profile=args.profile,
        output_root=args.output_dir,
        databricks_runner=_default_databricks_runner(),
        mlflow_runner=_default_mlflow_runner(),
        auto_backfill=args.auto_backfill,
        opt_run_id_override=args.opt_run_id,
        experiment_id_override=args.experiment_id,
    )
    print(json.dumps(manifest_to_dict(result.manifest), indent=2, sort_keys=True))
    return 0 if result.manifest.exit_status == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
