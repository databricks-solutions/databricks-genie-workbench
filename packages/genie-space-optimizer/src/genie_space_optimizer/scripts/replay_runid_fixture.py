"""Replay a captured PHASE_A fixture and persist analysis outputs.

Operator workflow:

    uv run python -m genie_space_optimizer.scripts.replay_runid_fixture \
        --fixture packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle10_raw.json \
        --opt-run-id 407772af-9662-4803-be6b-f00a368c528a \
        --analysis-root packages/genie-space-optimizer/docs/runid_analysis

Writes four files into ``<analysis-root>/<opt-run-id>/analysis/``:

    journey_validation.json     pretty-printed JourneyValidationReport.to_dict()
    canonical_journey.json      canonical_journey_json(events) (one-line, sorted)
    canonical_decisions.json    canonical_decision_json(records) (one-line, sorted)
    operator_transcript.md      render_operator_transcript() + per-iteration summary

This script is deploy-free — it has no Databricks or Spark dependencies.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from genie_space_optimizer.optimization.lever_loop_replay import (
    ReplayResult,
    run_replay,
)


def replay_fixture_to_disk(
    *,
    fixture_path: Path,
    analysis_dir: Path,
) -> ReplayResult:
    """Load ``fixture_path``, run replay, write outputs into ``analysis_dir``."""
    if not fixture_path.exists():
        raise FileNotFoundError(f"fixture not found: {fixture_path}")
    fixture = json.loads(fixture_path.read_text())
    result = run_replay(fixture)

    analysis_dir.mkdir(parents=True, exist_ok=True)
    (analysis_dir / "journey_validation.json").write_text(
        json.dumps(result.validation.to_dict(), indent=2, sort_keys=True) + "\n"
    )
    (analysis_dir / "canonical_journey.json").write_text(
        result.canonical_json + "\n"
    )
    (analysis_dir / "canonical_decisions.json").write_text(
        result.canonical_decision_json + "\n"
    )
    transcript = result.operator_transcript or "(no decision_records in any iteration)"
    summary_lines = [
        f"# Replay analysis for {fixture.get('fixture_id', '?')}",
        "",
        f"- iterations: {len(fixture.get('iterations') or [])}",
        f"- decision_records: {len(result.decision_records)}",
        f"- journey events: {len(result.events)}",
        f"- violations: {len(result.validation.violations)}",
        f"- missing qids: {len(result.validation.missing_qids)}",
        f"- decision-vs-journey errors: {len(result.decision_validation)}",
        "",
        "## Operator transcript",
        "",
        transcript,
    ]
    if result.decision_validation:
        summary_lines += [
            "",
            "## Decision-vs-journey validation errors",
            "",
            *[f"- {e}" for e in result.decision_validation],
        ]
    (analysis_dir / "operator_transcript.md").write_text(
        "\n".join(summary_lines) + "\n"
    )
    return result


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--opt-run-id", required=True)
    parser.add_argument(
        "--analysis-root",
        required=True,
        type=Path,
        help="parent directory; outputs land in <root>/<opt-run-id>/analysis/",
    )
    args = parser.parse_args(argv[1:])
    analysis_dir = args.analysis_root / args.opt_run_id / "analysis"
    result = replay_fixture_to_disk(
        fixture_path=args.fixture,
        analysis_dir=analysis_dir,
    )
    print(
        f"replay complete: violations={len(result.validation.violations)} "
        f"missing_qids={len(result.validation.missing_qids)} "
        f"decisions={len(result.decision_records)} -> {analysis_dir}"
    )
    return 0 if result.validation.is_valid and not result.decision_validation else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
