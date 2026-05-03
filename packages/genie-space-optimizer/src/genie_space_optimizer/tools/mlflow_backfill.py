"""Backfill decision-trail MLflow artifacts for completed GSO runs.

Phase E.0 Task 6. Reads a persisted replay fixture (typically
docs/runid_analysis/<opt_run_id>/replay_fixture.json or the live
airline_real_v1.json), reconstructs phase_a/journey_validation/,
phase_b/decision_trace/, and phase_b/operator_transcript/ artifacts,
and uploads them to a resolved anchor MLflow run.

Usage:
    python -m genie_space_optimizer.tools.mlflow_backfill \
        --opt-run-id <opt_run_id> \
        --replay-fixture <path/to/fixture.json>

The replay fixture must conform to the schema in canonical-schema.md:
each iteration must carry `journey_validation` (Phase A) and may
carry `decision_records` (Phase B). Iterations missing
decision_records get phase_a/ only.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class BackfillSummary:
    uploaded: int
    skipped_iterations: list[int]
    target_run_id: str


def backfill_artifacts(
    *,
    client: Any,
    anchor_run_id: str,
    replay_fixture: dict,
) -> BackfillSummary:
    """Reconstruct + upload artifacts for every iteration in the fixture."""
    uploaded = 0
    skipped: list[int] = []
    for iter_dict in replay_fixture.get("iterations") or []:
        iteration = int(iter_dict.get("iteration", 0))
        validation = iter_dict.get("journey_validation")
        if validation:
            client.log_text(
                run_id=anchor_run_id,
                text=json.dumps(validation, sort_keys=True, separators=(",", ":")),
                artifact_file=f"phase_a/journey_validation/iter_{iteration}.json",
            )
            uploaded += 1
        decision_records = iter_dict.get("decision_records") or []
        if not decision_records:
            skipped.append(iteration)
            continue
        # Reconstruct canonical decision JSON from the persisted records.
        from genie_space_optimizer.optimization.rca_decision_trace import (
            DecisionRecord,
            OptimizationTrace,
            canonical_decision_json,
            render_operator_transcript,
        )
        records = [DecisionRecord.from_dict(r) for r in decision_records]
        client.log_text(
            run_id=anchor_run_id,
            text=canonical_decision_json(records),
            artifact_file=f"phase_b/decision_trace/iter_{iteration}.json",
        )
        trace = OptimizationTrace(decision_records=tuple(records))
        transcript = render_operator_transcript(trace=trace, iteration=iteration)
        client.log_text(
            run_id=anchor_run_id,
            text=transcript,
            artifact_file=f"phase_b/operator_transcript/iter_{iteration}.txt",
        )
        uploaded += 2
    return BackfillSummary(
        uploaded=uploaded,
        skipped_iterations=skipped,
        target_run_id=anchor_run_id,
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill decision-trail MLflow artifacts from a replay fixture.",
    )
    parser.add_argument("--opt-run-id", required=True)
    parser.add_argument("--replay-fixture", required=True, type=Path)
    parser.add_argument("--experiment-id", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from mlflow.tracking import MlflowClient
    from genie_space_optimizer.tools.mlflow_artifact_anchor import (
        resolve_anchor_run_id,
    )

    args = _parse_args(list(argv) if argv is not None else sys.argv[1:])
    fixture = json.loads(args.replay_fixture.read_text())

    client = MlflowClient()
    if args.experiment_id:
        experiment_ids = [args.experiment_id]
    else:
        experiment_ids = [e.experiment_id for e in client.search_experiments()]

    anchor = resolve_anchor_run_id(
        client=client,
        opt_run_id=args.opt_run_id,
        experiment_ids=experiment_ids,
    )
    if not anchor:
        print(f"No anchor run found for opt_run_id={args.opt_run_id}", file=sys.stderr)
        return 1
    summary = backfill_artifacts(
        client=client,
        anchor_run_id=anchor,
        replay_fixture=fixture,
    )
    print(
        f"Backfilled {summary.uploaded} artifacts onto run {anchor} "
        f"(skipped iterations with no decision_records: {summary.skipped_iterations})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
