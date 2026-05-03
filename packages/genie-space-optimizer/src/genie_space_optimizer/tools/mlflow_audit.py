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
from typing import Any


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


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(list(argv) if argv is not None else sys.argv[1:])
    print(audit(opt_run_id=args.opt_run_id, experiment_id=args.experiment_id))
    return 0


if __name__ == "__main__":
    sys.exit(main())
