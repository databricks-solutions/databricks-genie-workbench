"""Read-only MLflow artifact audit for a Genie Space Optimizer run.

Usage:
    python -m genie_space_optimizer.tools.mlflow_audit \
        --opt-run-id <optimization_run_id> \
        [--experiment-id <id>] \
        [--profile <databricks_profile>]

Lists every MLflow run sharing the tag
`genie.optimization_run_id=<opt_run_id>`, dumps the artifact tree of
each run, and reports whether the decision-trail artifact paths
(`phase_a/journey_validation/`, `phase_b/decision_trace/`,
`phase_b/operator_transcript/`) are present on any sibling.

The script is read-only. It writes a markdown report to stdout.
Pipe to a file under `docs/runid_analysis/` for permanent capture.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


# Phase H: import alias so tests can monkeypatch a single symbol on this
# module instead of the third-party module path. The audit helper looks
# this name up at call time.
try:  # pragma: no cover — non-MLflow envs in tests stub this directly
    from mlflow.tracking import MlflowClient as MlflowClient
except Exception:  # pragma: no cover
    MlflowClient = None  # type: ignore[assignment,misc]


_DECISION_ARTIFACT_PREFIXES = (
    "phase_a/journey_validation/",
    "phase_b/decision_trace/",
    "phase_b/operator_transcript/",
)


def _list_artifacts_recursive(client: Any, run_id: str, path: str = "") -> list[str]:
    """Recursive flat listing of every file (not directory) under `path`."""
    out: list[str] = []
    for fi in client.list_artifacts(run_id, path):
        if fi.is_dir:
            out.extend(_list_artifacts_recursive(client, run_id, fi.path))
        else:
            out.append(fi.path)
    return out


_LEVER_LOOP_RUN_TYPE = "lever_loop"


def audit_optimization_run(
    *,
    optimization_run_id: str,
    experiment_id: str | None = None,
    client: Any = None,
) -> dict[str, Any]:
    """Programmatic audit returning a structured dict.

    Phase E.0 Task 6a (added for the evidence-bundle plan). Returns:

        {
            "anchor_run_id": str,                      # lever_loop sibling, or earliest
            "sibling_runs": [
                {"run_id": ..., "run_type": ..., "artifact_paths": [...]},
                ...
            ],
            "missing_per_iteration": [
                {"iteration": int, "kind": str, "anchor_run_id": str},
                ...
            ],
        }

    ``kind`` values: ``PHASE_A_JOURNEY_VALIDATION``,
    ``PHASE_B_DECISION_TRACE``, ``PHASE_B_OPERATOR_TRANSCRIPT``.

    The companion ``audit()`` function in this module still returns a
    markdown report for the CLI. They share the same listing logic.
    """
    import re as _re

    from mlflow.tracking import MlflowClient

    if client is None:
        client = MlflowClient()
    filter_string = f"tags.`genie.optimization_run_id` = '{optimization_run_id}'"
    experiment_ids = (
        [experiment_id]
        if experiment_id
        else [e.experiment_id for e in client.search_experiments()]
    )
    runs = client.search_runs(
        experiment_ids=experiment_ids,
        filter_string=filter_string,
        max_results=200,
    )

    sibling_runs: list[dict[str, Any]] = []
    anchor_run_id = ""
    earliest_start = None
    earliest_run_id = ""
    iters_seen_by_kind: dict[str, set[int]] = {
        "PHASE_A_JOURNEY_VALIDATION": set(),
        "PHASE_B_DECISION_TRACE": set(),
        "PHASE_B_OPERATOR_TRANSCRIPT": set(),
    }
    iter_re_by_kind = {
        "PHASE_A_JOURNEY_VALIDATION": _re.compile(
            r"^phase_a/journey_validation/iter_(\d+)\.json$"
        ),
        "PHASE_B_DECISION_TRACE": _re.compile(
            r"^phase_b/decision_trace/iter_(\d+)\.json$"
        ),
        "PHASE_B_OPERATOR_TRANSCRIPT": _re.compile(
            r"^phase_b/operator_transcript/iter_(\d+)\.txt$"
        ),
    }
    for run in runs:
        run_id = run.info.run_id
        tags = run.data.tags or {}
        run_type = tags.get("genie.run_type", "")
        artifacts = _list_artifacts_recursive(client, run_id)
        sibling_runs.append(
            {"run_id": run_id, "run_type": run_type, "artifact_paths": artifacts}
        )
        if run_type == _LEVER_LOOP_RUN_TYPE and not anchor_run_id:
            anchor_run_id = run_id
        start_time = getattr(run.info, "start_time", None) or 0
        if earliest_start is None or start_time < earliest_start:
            earliest_start = start_time
            earliest_run_id = run_id
        # Only count iter coverage on the lever_loop sibling — that's where
        # decision-trail artifacts are anchored per Phase E.0.
        if run_type == _LEVER_LOOP_RUN_TYPE:
            for art in artifacts:
                for kind, pattern in iter_re_by_kind.items():
                    match = pattern.match(art)
                    if match:
                        iters_seen_by_kind[kind].add(int(match.group(1)))
    if not anchor_run_id:
        anchor_run_id = earliest_run_id

    # Determine missing iters: any iter where one of the three kinds is
    # missing on the anchor while iter is referenced by at least one kind.
    all_iters: set[int] = set()
    for s in iters_seen_by_kind.values():
        all_iters.update(s)
    missing_per_iteration: list[dict[str, Any]] = []
    for iteration in sorted(all_iters):
        for kind, seen in iters_seen_by_kind.items():
            if iteration not in seen:
                missing_per_iteration.append(
                    {
                        "iteration": iteration,
                        "kind": kind,
                        "anchor_run_id": anchor_run_id,
                    }
                )

    return {
        "anchor_run_id": anchor_run_id,
        "sibling_runs": sibling_runs,
        "missing_per_iteration": missing_per_iteration,
    }


def audit(
    *,
    opt_run_id: str,
    experiment_id: str | None,
) -> str:
    """Run the audit and return a markdown report string."""
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    filter_string = f"tags.`genie.optimization_run_id` = '{opt_run_id}'"
    experiment_ids = [experiment_id] if experiment_id else None
    if experiment_ids is None:
        # Search across the default + named experiments.
        experiments = client.search_experiments()
        experiment_ids = [e.experiment_id for e in experiments]

    runs = client.search_runs(
        experiment_ids=experiment_ids,
        filter_string=filter_string,
        max_results=200,
    )

    lines: list[str] = []
    lines.append(f"# MLflow Artifact Audit — opt_run_id={opt_run_id}")
    lines.append("")
    lines.append(f"Sibling runs found: **{len(runs)}**")
    lines.append("")
    if not runs:
        lines.append("No runs match this optimization_run_id tag.")
        lines.append(
            "Possible causes: tag misspelled, runs in a different experiment, "
            "or the optimization didn't reach the tagging step."
        )
        return "\n".join(lines)

    decision_artifacts_by_prefix: dict[str, list[tuple[str, str]]] = defaultdict(list)
    by_run_table: list[str] = [
        "| run_id | run_name | artifact_count | decision_trail_present |",
        "|---|---|---|---|",
    ]
    for run in runs:
        run_id = run.info.run_id
        run_name = run.data.tags.get("mlflow.runName", "(no name)")
        artifacts = _list_artifacts_recursive(client, run_id)
        present_prefixes: set[str] = set()
        for art in artifacts:
            for prefix in _DECISION_ARTIFACT_PREFIXES:
                if art.startswith(prefix):
                    present_prefixes.add(prefix)
                    decision_artifacts_by_prefix[prefix].append((run_id, art))
        decision_summary = (
            ", ".join(sorted(present_prefixes)) if present_prefixes else "(none)"
        )
        by_run_table.append(
            f"| `{run_id}` | {run_name} | {len(artifacts)} | {decision_summary} |"
        )

    lines.extend(by_run_table)
    lines.append("")
    lines.append("## Decision-trail artifact distribution")
    for prefix in _DECISION_ARTIFACT_PREFIXES:
        entries = decision_artifacts_by_prefix.get(prefix, [])
        lines.append(f"### `{prefix}`")
        if not entries:
            lines.append("  **NOT FOUND on any sibling run.**")
        else:
            for run_id, art in entries:
                lines.append(f"  - `{run_id}` :: `{art}`")
        lines.append("")
    return "\n".join(lines)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit decision-trail MLflow artifacts for a GSO run.",
    )
    parser.add_argument("--opt-run-id", required=True)
    parser.add_argument("--experiment-id", default=None)
    return parser.parse_args(argv)


@dataclass(frozen=True)
class ParentBundleAuditReport:
    """Result of auditing the Phase H parent lever-loop bundle."""
    optimization_run_id: str
    parent_run_id: str | None
    has_manifest: bool
    missing_artifacts: tuple[str, ...] = ()
    notes: str = ""


def audit_parent_bundle(
    *,
    optimization_run_id: str,
    experiment_id: str | None = None,
) -> ParentBundleAuditReport:
    """Find the parent lever-loop run for ``optimization_run_id`` and
    assert that ``gso_postmortem_bundle/manifest.json`` exists (Phase H).

    Searches by ``genie.run_role=lever_loop`` + ``genie.optimization_run_id``;
    falls back to the legacy ``genie.run_id`` tag for back-compat.
    """
    client = MlflowClient()
    search_filter = (
        f"tags.genie.run_role = 'lever_loop' AND "
        f"tags.genie.optimization_run_id = '{optimization_run_id}'"
    )
    runs = client.search_runs(
        experiment_ids=[experiment_id] if experiment_id else [],
        filter_string=search_filter,
        max_results=10,
    )
    if not runs:
        legacy_filter = f"tags.genie.run_id = '{optimization_run_id}'"
        runs = client.search_runs(
            experiment_ids=[experiment_id] if experiment_id else [],
            filter_string=legacy_filter,
            max_results=10,
        )

    manifest_path = "gso_postmortem_bundle/manifest.json"
    if not runs:
        return ParentBundleAuditReport(
            optimization_run_id=optimization_run_id,
            parent_run_id=None,
            has_manifest=False,
            missing_artifacts=(manifest_path,),
            notes="parent run not found via genie.run_role or genie.run_id",
        )

    parent = runs[0]
    parent_run_id = parent.info.run_id
    artifacts = client.list_artifacts(parent_run_id, path="gso_postmortem_bundle")
    artifact_paths = {a.path for a in artifacts}
    has_manifest = manifest_path in artifact_paths
    missing: tuple[str, ...] = () if has_manifest else (manifest_path,)

    return ParentBundleAuditReport(
        optimization_run_id=optimization_run_id,
        parent_run_id=parent_run_id,
        has_manifest=has_manifest,
        missing_artifacts=missing,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(list(argv) if argv is not None else sys.argv[1:])
    print(audit(opt_run_id=args.opt_run_id, experiment_id=args.experiment_id))
    return 0


if __name__ == "__main__":
    sys.exit(main())
