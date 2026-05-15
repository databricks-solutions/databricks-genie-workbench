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
import enum
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


def _task_result_state(task: Mapping[str, Any]) -> str:
    """Extract the terminal result state from a Databricks task dict.

    The Jobs API nests result state under ``state.result_state``;
    older fixtures place it directly on the task. Returns an upper-
    case string ("SUCCESS", "FAILED", "CANCELED", ...) or "" when
    the task is still running / no terminal state is recorded.
    """
    state = task.get("state")
    if isinstance(state, Mapping):
        rs = state.get("result_state") or ""
    else:
        rs = task.get("result_state") or ""
    return str(rs).upper()


def _select_lever_loop_task(
    tasks: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any] | None, list[Mapping[str, Any]]]:
    """Pick the lever_loop task to anchor evidence on, and return
    every failed attempt for separate per-attempt artifacts.

    Selection order:
      1. Among ``task_key=='lever_loop'`` tasks, prefer SUCCESS.
      2. Within the preferred-state set, prefer largest ``end_time``
         (most recent terminal time), then largest ``start_time`` as
         a tiebreaker.
      3. If no SUCCESS exists, fall back to the latest FAILED. The
         chosen task is also recorded in ``failed_attempts`` so the
         caller emits a per-attempt artifact for it too.
      4. If no ``lever_loop`` task exists, return ``(None, [])``.

    Reproducer: run 2423b960 had 4 FAILED + 1 SUCCESS attempts; the
    old ``next(t for ...)`` anchored to attempt 1 (FAILED). This
    selector picks attempt 5 (SUCCESS) and records 200..203 as
    failed_attempts.
    """
    lever_tasks = [
        t for t in (tasks or []) if t.get("task_key") == "lever_loop"
    ]
    if not lever_tasks:
        return None, []

    def _sort_key(t: Mapping[str, Any]) -> tuple[int, int, int]:
        """Strictly-ordered key: end_time, then start_time, then numeric
        task_run_id as a deterministic tiebreaker (Databricks task ids
        are monotonically allocated, so the larger id is the later
        attempt). Trial-3 exposed parents where two SUCCESS attempts
        tied on (end_time, start_time) at second-level resolution;
        without the third component the API's input order won and the
        bundler anchored on the older attempt."""
        try:
            tid = int(str(t.get("task_run_id") or "0") or "0")
        except (TypeError, ValueError):
            tid = 0
        return (
            int(t.get("end_time") or 0),
            int(t.get("start_time") or 0),
            tid,
        )

    successes = sorted(
        (t for t in lever_tasks if _task_result_state(t) == "SUCCESS"),
        key=_sort_key,
        reverse=True,
    )
    failures = sorted(
        (t for t in lever_tasks if _task_result_state(t) != "SUCCESS"),
        key=_sort_key,
        reverse=True,
    )
    if successes:
        return successes[0], list(failures)
    # All failed: pick the latest, but also include it in failed_attempts.
    return failures[0], list(failures)


def _failed_attempt_artifact_path(
    evidence_dir: Path,
    *,
    task_run_id: str,
) -> Path:
    """Return the on-disk path for one failed lever_loop attempt's
    ``get-run-output`` JSON. Lands under
    ``<evidence_dir>/failed_lever_loop_attempts/<task_run_id>.json``
    so the postmortem skill can scan failed-attempt error classes
    without colliding with the chosen attempt's stdout/stderr.
    """
    return evidence_dir / "failed_lever_loop_attempts" / f"{task_run_id}.json"


def download_parent_bundle(
    *,
    parent_run_id: str,
    target_dir: Path,
) -> tuple[bool, list[MissingPiece]]:
    """Download ``gso_postmortem_bundle/`` from the parent MLflow run (Phase H).

    Materializes the parent bundle under ``target_dir.parent`` so the
    files land at ``<target_dir.parent>/gso_postmortem_bundle/``.
    Returns ``(success, missing_pieces)``. On any failure, success is
    False and a ``MissingPiece(MLFLOW_AUDIT_FAILED)`` is recorded so
    the caller can fall back to the legacy phase artifacts.

    The parent bundle is the Phase H gso_postmortem_bundle artifact
    tree on the lever-loop MLflow run discovered via
    ``genie.run_role=lever_loop`` + ``genie.optimization_run_id``.
    """
    try:
        from mlflow.tracking import MlflowClient
        client = MlflowClient()
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        client.download_artifacts(
            run_id=parent_run_id,
            path="gso_postmortem_bundle",
            dst_path=str(target_dir.parent),
        )
        return True, []
    except Exception as exc:
        return False, [MissingPiece(
            kind=MissingPieceKind.MLFLOW_AUDIT_FAILED,
            iteration=None,
            diagnosis=f"parent bundle download failed: {exc}",
            suggested_action=(
                "Verify the parent run exists and gso_postmortem_bundle/* "
                "artifacts are present on the run discovered via "
                "genie.run_role=lever_loop + genie.optimization_run_id."
            ),
        )]


def _extract_stdout_with_fallback(
    out: Mapping[str, Any] | dict[str, Any],
) -> tuple[str, str, MissingPiece | None]:
    """Resolve lever-loop stdout from a Databricks ``get-run-output`` payload.

    Resolution order:
        1. ``out["logs"]``  — populated for ``python_wheel_task`` /
           ``spark_python_task`` runs.
        2. ``out["notebook_output"]["result"]`` — populated for
           ``notebook_task`` runs (logs is empty by API contract).
        3. **Phase 0.1**: ``out["notebook_output"]["error_trace"]`` —
           populated for ``python_wheel_task`` / ``spark_python_task``
           runs that wrote to stderr (the PHASE_A_REPLAY_FIXTURE_JSON
           markers live here for non-notebook tasks).
        4. **Phase 0.1**: ``result + "\\n" + error_trace`` concatenated
           when the fixture begin marker is in one and the end marker
           is in the other (Databricks truncation edge case).
        5. Empty string if none populated.

    Returns ``(stdout_text, source, missing_piece)``. ``source`` is one
    of ``"logs"``, ``"notebook_output.result"``,
    ``"notebook_output.error_trace"``,
    ``"notebook_output.result+error_trace"``, or ``"absent"``. A
    ``STDOUT_FALLBACK_NOTEBOOK_OUTPUT`` ``MissingPiece`` is returned
    when any fallback (tiers 2-4) is used.
    """
    logs_text = str((out or {}).get("logs") or "")
    if logs_text:
        return logs_text, "logs", None

    notebook_output = (out or {}).get("notebook_output") or {}
    result_text = str(notebook_output.get("result") or "")
    error_trace_text = str(notebook_output.get("error_trace") or "")
    truncated = bool(notebook_output.get("truncated"))

    _BEGIN = "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN==="
    _END = "===PHASE_A_REPLAY_FIXTURE_JSON_END==="

    def _has_full_fixture(s: str) -> bool:
        return _BEGIN in s and _END in s and s.index(_END) > s.index(_BEGIN)

    fallback_diagnosis_template = (
        "logs field empty (Databricks Jobs API). Falling back to {source}{truncated}."
    )
    fallback_suggested = (
        "no operator action required. The marker parser and replay "
        "extractor consume the same string regardless of source."
    )

    if _has_full_fixture(result_text):
        suffix = " (truncated by Databricks)" if truncated else ""
        return (
            result_text,
            "notebook_output.result",
            MissingPiece(
                kind=MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT,
                iteration=None,
                diagnosis=fallback_diagnosis_template.format(
                    source="notebook_output.result", truncated=suffix,
                ),
                suggested_action=fallback_suggested,
            ),
        )

    if _has_full_fixture(error_trace_text):
        return (
            error_trace_text,
            "notebook_output.error_trace",
            MissingPiece(
                kind=MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT,
                iteration=None,
                diagnosis=fallback_diagnosis_template.format(
                    source="notebook_output.error_trace (stderr)", truncated="",
                ),
                suggested_action=fallback_suggested,
            ),
        )

    if (_BEGIN in result_text and _END in error_trace_text) or (
        _BEGIN in error_trace_text and _END in result_text
    ):
        concatenated = result_text + "\n" + error_trace_text
        return (
            concatenated,
            "notebook_output.result+error_trace",
            MissingPiece(
                kind=MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT,
                iteration=None,
                diagnosis=(
                    "PHASE_A_REPLAY_FIXTURE markers split across "
                    "notebook_output.result and notebook_output.error_trace. "
                    "Concatenating and resplitting."
                ),
                suggested_action=fallback_suggested,
            ),
        )

    if result_text:
        suffix = " (truncated by Databricks)" if truncated else ""
        return (
            result_text,
            "notebook_output.result",
            MissingPiece(
                kind=MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT,
                iteration=None,
                diagnosis=fallback_diagnosis_template.format(
                    source="notebook_output.result (no PHASE_A markers)",
                    truncated=suffix,
                ),
                suggested_action=fallback_suggested,
            ),
        )

    if error_trace_text:
        return (
            error_trace_text,
            "notebook_output.error_trace",
            MissingPiece(
                kind=MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT,
                iteration=None,
                diagnosis=fallback_diagnosis_template.format(
                    source="notebook_output.error_trace (no PHASE_A markers)",
                    truncated="",
                ),
                suggested_action=fallback_suggested,
            ),
        )

    return "", "absent", None


def detect_stale_phase_h_anchor(
    *,
    chosen_task_run_id: str,
    phase_h_sibling_task_run_ids: tuple[str, ...] | list[str],
) -> MissingPiece | None:
    """Phase 0.1 — return STALE_ANCHOR when no Phase H sibling's
    ``lever_loop_task_run_id`` matches the chosen lever_loop task.

    Pure: no I/O. Caller decides whether to abort or degrade.
    """
    chosen = str(chosen_task_run_id or "").strip()
    candidates = [str(s or "").strip() for s in (phase_h_sibling_task_run_ids or ())]
    if chosen and chosen in candidates:
        return None
    diagnosis = (
        f"chosen lever_loop task_run_id={chosen or '<blank>'} does not match any "
        f"Phase H sibling task_run_id in {candidates or '<empty>'}. The Phase H "
        "artifacts (gso_postmortem_bundle, journey_validation_all, etc.) belong "
        "to a different run; consuming them would pollute the postmortem."
    )
    return MissingPiece(
        kind=MissingPieceKind.STALE_ANCHOR,
        iteration=None,
        diagnosis=diagnosis,
        suggested_action=(
            "Re-run the evidence bundle CLI with the lever_loop parent run "
            "whose MLflow tag genie.databricks.lever_loop_task_run_id matches "
            f"{chosen or 'the chosen task'}. If no such Phase H run exists, "
            "the lever_loop task ran without emitting Phase H artifacts — "
            "treat replay fixture as authoritative and skip Phase H consumers."
        ),
    )


class EvidenceBundleStatus(enum.Enum):
    """Phase 0.1 — typed status enum for Phase H anchor resolution.

    HEALTHY: the artifact's ``lever_loop_task_run_id`` matches the
             resolved task. Phase H artifacts may be consumed.
    STALE_ANCHOR: the artifact's ``lever_loop_task_run_id`` does NOT
                  match the resolved task. Phase H artifacts must NOT
                  be consumed; postmortem falls back to stdout.
    """

    HEALTHY = "healthy"
    STALE_ANCHOR = "stale_anchor"


class StalePhaseHAnchorError(RuntimeError):
    """Raised when ``resolve_phase_h_anchor`` detects a stale or
    unresolvable Phase H anchor.

    The error message always begins with the ``STALE_ANCHOR`` sentinel
    so callers and tests can match on it deterministically. When the
    mismatch involves two concrete task_run_ids, both are surfaced in
    the message to support post-hoc diagnosis without re-reading the
    artifact.
    """


def resolve_phase_h_anchor(
    *,
    artifact_path: str | Path,
    resolved_task_run_id: str,
) -> EvidenceBundleStatus:
    """Phase 0.1 / Phase 5 Task 13 — typed wrapper that reads a Phase H
    replay artifact and confirms its embedded
    ``lever_loop_task_run_id`` matches ``resolved_task_run_id``.

    Returns ``EvidenceBundleStatus.HEALTHY`` on match. On mismatch,
    missing file, or unparseable JSON, raises
    :class:`StalePhaseHAnchorError` whose message begins with
    ``STALE_ANCHOR`` and (for mismatches) includes both the expected
    and observed task_run_ids.

    Mirrors the pure-helper semantics of
    :func:`detect_stale_phase_h_anchor` but with a path-based interface
    so postmortem callers can do::

        status = resolve_phase_h_anchor(
            artifact_path=run_manifest_v2_path,
            resolved_task_run_id=task_run_id,
        )

    instead of constructing a :class:`MissingPiece` by hand.
    """
    p = Path(artifact_path) if not isinstance(artifact_path, Path) else artifact_path
    expected = str(resolved_task_run_id or "").strip()

    if not p.exists():
        raise StalePhaseHAnchorError(
            f"STALE_ANCHOR: Phase H artifact {p} does not exist; "
            f"cannot anchor on lever_loop_task_run_id={expected or '<blank>'}."
        )

    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise StalePhaseHAnchorError(
            f"STALE_ANCHOR: Phase H artifact {p} could not be parsed as "
            f"JSON ({exc}); cannot anchor on "
            f"lever_loop_task_run_id={expected or '<blank>'}."
        ) from exc

    # Look for lever_loop_task_run_id in common locations.
    raw_candidate = (
        payload.get("lever_loop_task_run_id")
        if isinstance(payload, dict)
        else None
    )
    if not raw_candidate and isinstance(payload, dict):
        manifest = payload.get("manifest")
        if isinstance(manifest, dict):
            raw_candidate = manifest.get("lever_loop_task_run_id")
    if not raw_candidate and isinstance(payload, dict):
        tags = payload.get("tags")
        if isinstance(tags, dict):
            raw_candidate = tags.get(
                "genie.databricks.lever_loop_task_run_id"
            )

    candidate = str(raw_candidate or "").strip()

    if candidate and candidate == expected:
        return EvidenceBundleStatus.HEALTHY

    raise StalePhaseHAnchorError(
        f"STALE_ANCHOR: Phase H artifact {p} has "
        f"lever_loop_task_run_id={candidate or '<blank>'} but caller "
        f"resolved task_run_id={expected or '<blank>'}. Phase H "
        "artifacts belong to a different run; consuming them would "
        "pollute the postmortem."
    )


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
        if isinstance(data, list):
            decisions = [entry for entry in data if isinstance(entry, dict)]
            iteration = next(
                (
                    decision.get("iteration")
                    for decision in decisions
                    if decision.get("iteration") is not None
                ),
                None,
            )
        elif isinstance(data, dict):
            decisions = data.get("decisions", [])
            iteration = data.get("iteration")
        else:
            continue
        unresolved_trace_ids: list[str] = []
        unresolved_reasons = 0
        for decision in decisions:
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
                or artifact_path.startswith(
                    "gso_postmortem_bundle/iterations/iter_"
                )
                # Phase 0.4 — Task 14: also pull the top-level candidate
                # ledger so build_bundle can parse it and set
                # ``candidate_ledger_entry_count`` on the manifest.
                or artifact_path == (
                    "gso_postmortem_bundle/iteration_candidate_ledger.jsonl"
                )
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

    lever_task, failed_lever_loop_attempts = _select_lever_loop_task(
        job_run.get("tasks", []) or []
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

    # Emit one ``get-run-output`` JSON per failed lever_loop attempt so
    # the postmortem skill can scan their error classes without
    # affecting the chosen-attempt stdout/stderr files. Skipped silently
    # when the per-attempt fetch fails (a truly broken attempt may have
    # no recoverable output).
    for failed in failed_lever_loop_attempts:
        failed_run_id = str(failed.get("run_id") or "")
        if not failed_run_id:
            continue
        try:
            failed_output = databricks_runner.get_run_output(
                run_id=failed_run_id, profile=profile,
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "Could not fetch get-run-output for failed lever_loop attempt %s",
                failed_run_id,
            )
            continue
        failed_path = _failed_attempt_artifact_path(
            paths.evidence_dir, task_run_id=failed_run_id
        )
        failed_path.parent.mkdir(parents=True, exist_ok=True)
        failed_path.write_text(json.dumps(failed_output, indent=2, default=str))

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

    _DATABRICKS_ID_FIELDS = (
        "databricks_job_id",
        "databricks_parent_run_id",
        "lever_loop_task_run_id",
    )
    _manifest_marker = markers.run_manifest_v2 or markers.run_manifest
    if _manifest_marker is not None:
        _sentinel_fields = [
            field
            for field in _DATABRICKS_ID_FIELDS
            if str(_manifest_marker.get(field, "")) == "unknown"
        ]
        if _sentinel_fields:
            missing.append(
                MissingPiece(
                    kind=MissingPieceKind.DATABRICKS_IDS_UNRESOLVED,
                    iteration=None,
                    diagnosis=(
                        "GSO_RUN_MANIFEST resolver returned the literal "
                        f"sentinel 'unknown' for: {', '.join(_sentinel_fields)}. "
                        "All three Tier-1/Tier-2/Tier-3 paths failed."
                    ),
                    suggested_action=(
                        "Inspect GSO_DATABRICKS_IDS_RESOLVED_V1 in markers.json: "
                        "verify dbutils_attempted/succeeded and "
                        "jobs_api_attempted/succeeded. If jobs_api_attempted=false, "
                        "the active MLflow run did not carry a "
                        "mlflow.databricks.runID tag — check the lever-loop notebook "
                        "is actually inside a Databricks Jobs task. If "
                        "jobs_api_succeeded=false, the WorkspaceClient call returned "
                        "an empty Run snapshot — check SP permissions on the job."
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

    # Phase 0.1: fail-closed stale-anchor detection. If at least one
    # sibling advertises Phase H artifacts (``gso_postmortem_bundle/*``)
    # and *none* of them carries the same ``lever_loop_task_run_id`` as
    # the chosen task, record STALE_ANCHOR and skip Phase H artifact
    # downloads so the postmortem does not consume mismatched artifacts
    # (the airline ff71c000 defect, where Phase H artifacts from a
    # *different* run polluted the postmortem).
    #
    # When no sibling advertises Phase H artifacts at all, the check is
    # a no-op — legacy Phase A/B/E siblings predate the lever-loop tag
    # contract and must continue to flow through ``_walk_audit_artifacts``
    # unchanged.
    _skip_phase_h_downloads = False
    if audit:
        _has_phase_h_sibling = any(
            any(
                str(p or "").startswith("gso_postmortem_bundle/")
                for p in (_sib.get("artifact_paths") or [])
            )
            for _sib in (audit.get("sibling_runs") or [])
        )
        if _has_phase_h_sibling:
            _chosen_task_run_id = str(
                (lever_task or {}).get("run_id") or ""
            ).strip()
            _phase_h_sibling_task_run_ids: list[str] = []
            for _sib in (audit.get("sibling_runs") or []):
                _has_phase_h_paths = any(
                    str(p or "").startswith("gso_postmortem_bundle/")
                    for p in (_sib.get("artifact_paths") or [])
                )
                if not _has_phase_h_paths:
                    continue
                _tags = _sib.get("tags") or {}
                _sib_task_run_id = str(
                    _tags.get("genie.databricks.lever_loop_task_run_id") or ""
                ).strip()
                if _sib_task_run_id:
                    _phase_h_sibling_task_run_ids.append(_sib_task_run_id)
            _stale_anchor = detect_stale_phase_h_anchor(
                chosen_task_run_id=_chosen_task_run_id,
                phase_h_sibling_task_run_ids=tuple(_phase_h_sibling_task_run_ids),
            )
            if _stale_anchor is not None:
                missing.append(_stale_anchor)
                _skip_phase_h_downloads = True

    if audit and not _skip_phase_h_downloads:
        sibling_run_ids, pulled_artifacts, audit_missing, anchor_run_id = (
            _walk_audit_artifacts(
                audit=audit, mlflow_runner=mlflow_runner, paths=paths
            )
        )
        missing.extend(audit_missing)
        paths.mlflow_audit_json.write_text(json.dumps(audit, indent=2, sort_keys=True))
        paths.mlflow_audit_md.write_text(_render_audit_markdown(audit))
    elif audit and _skip_phase_h_downloads:
        # Still persist the audit JSON/MD so operators can inspect the
        # mismatch — only the artifact downloads are skipped.
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

    # Phase 0.4 — Task 14: parse the Phase H candidate ledger when
    # present and record the entry count on the manifest. Search the
    # canonical bundle location first
    # (``evidence/gso_postmortem_bundle/iteration_candidate_ledger.jsonl``)
    # and fall back to the ``evidence/mlflow/<sibling>/...`` landing
    # path used by ``_walk_audit_artifacts``. On parse failure emit a
    # typed MissingPiece with a stdout-marker fallback suggestion.
    _ledger_entry_count = 0
    _ledger_candidate_paths: list[Path] = []
    _canonical_ledger_path = (
        paths.parent_bundle_dir / "iteration_candidate_ledger.jsonl"
    )
    if _canonical_ledger_path.exists():
        _ledger_candidate_paths.append(_canonical_ledger_path)
    else:
        # Fall back to whatever ``_walk_audit_artifacts`` placed under
        # ``paths.mlflow_dir/<sibling_run_id>/gso_postmortem_bundle/``.
        if paths.mlflow_dir.exists():
            _ledger_candidate_paths.extend(
                paths.mlflow_dir.glob(
                    "*/gso_postmortem_bundle/iteration_candidate_ledger.jsonl"
                )
            )
    if _ledger_candidate_paths:
        _ledger_artifact_path = _ledger_candidate_paths[0]
        try:
            from genie_space_optimizer.optimization.candidate_ledger import (
                read_ledger,
            )
            _ledger_entries = read_ledger(str(_ledger_artifact_path))
            _ledger_entry_count = len(_ledger_entries)
        except Exception as exc:  # noqa: BLE001
            missing.append(MissingPiece(
                kind=MissingPieceKind.PHASE_A_ARTIFACT_MISSING_ON_ANCHOR,
                iteration=None,
                diagnosis=(
                    f"candidate ledger parse failed at "
                    f"{_ledger_artifact_path}: "
                    f"{type(exc).__name__}: {exc}"
                ),
                suggested_action=(
                    "Inspect iteration_candidate_ledger.jsonl manually; "
                    "the marker_parser stdout fallback "
                    "(extract_candidate_ledger_from_stdout) may still "
                    "yield entries."
                ),
            ))

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
        candidate_ledger_entry_count=_ledger_entry_count,
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
