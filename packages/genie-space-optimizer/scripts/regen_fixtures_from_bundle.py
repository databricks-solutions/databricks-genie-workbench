#!/usr/bin/env python
"""Plan 5 — postmortem-driven fixture regen orchestrator.

Reads a postmortem evidence bundle (produced by
``tools.evidence_bundle``) and, for each capture NDJSON present at
``evidence/gso_trial_captures/<plan>.ndjson``, invokes the matching
``scripts/export_<plan>_fixtures.py`` to populate
``tests/fixtures/<plan>/``.

Workflow:
  1. The operator triggers an optimization job from the UI.
  2. When the job finishes, the operator gives me ``(job_id, run_id)``.
  3. I run the ``gso-postmortem`` skill which produces a bundle at
     ``packages/genie-space-optimizer/docs/runid_analysis/<opt_run_id>/``.
  4. I run this script:
       python scripts/regen_fixtures_from_bundle.py <opt_run_id>
  5. I run the four fixture-regression test files. If green, I commit
     the regenerated fixtures + the postmortem report.

The script returns exit code 0 when every attempted exporter succeeded,
non-zero otherwise. Plans whose NDJSON file is absent are skipped
(reported under ``plans_skipped_no_capture``), not failed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# Per-plan dispatch table mapping plan_id (matches the NDJSON file's
# basename without ``.ndjson``) to the exporter script path, the CLI
# flag it accepts for the capture file, and whether it consumes the
# MLflow experiment id. Output dir is always
# ``tests/fixtures/<plan_id>/``. The flag names mirror what each
# ``scripts/export_*_fixtures.py`` actually declares with argparse —
# they are intentionally not uniform across plans (Plan 1 uses
# ``--narrowing-capture-path``; the others use ``--capture-path``;
# Plan 4 does not need MLflow because every record is self-contained).
@dataclass(frozen=True)
class _PlanDispatch:
    plan_id: str
    exporter_script: str
    capture_flag: str
    needs_mlflow_experiment_id: bool


_DISPATCH = (
    _PlanDispatch(
        plan_id="narrowing_v1",
        exporter_script="scripts/export_narrowing_fixtures.py",
        capture_flag="--narrowing-capture-path",
        needs_mlflow_experiment_id=True,
    ),
    _PlanDispatch(
        plan_id="lever5_split_v1",
        exporter_script="scripts/export_lever5_split_fixtures.py",
        capture_flag="--capture-path",
        needs_mlflow_experiment_id=True,
    ),
    _PlanDispatch(
        plan_id="three_stage_v1",
        exporter_script="scripts/export_three_stage_fixtures.py",
        capture_flag="--capture-path",
        needs_mlflow_experiment_id=True,
    ),
    _PlanDispatch(
        plan_id="raw_evidence_v1",
        exporter_script="scripts/export_raw_evidence_fixtures.py",
        capture_flag="--capture-path",
        needs_mlflow_experiment_id=False,
    ),
)


def _read_manifest(bundle_dir: Path) -> dict[str, Any] | None:
    manifest_path = bundle_dir / "evidence" / "manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _resolve_repo_root() -> Path:
    """Return the genie-space-optimizer package root so subprocess
    invocations of scripts/export_*.py find the scripts."""
    return Path(__file__).resolve().parents[1]


def regen_fixtures(
    *, bundle_dir: Path, output_root: Path | None = None,
) -> dict[str, Any]:
    """Public entry point used by both the CLI and the unit tests.

    Args:
      bundle_dir: Path to ``runid_analysis/<opt_run_id>``.
      output_root: Path to the ``tests/fixtures/`` root. Defaults to
        the in-repo ``packages/genie-space-optimizer/tests/fixtures``.
        Tests pass a tmp dir to avoid clobbering committed fixtures.

    Returns:
      dict summary with keys:
        plans_attempted: list[str]
        plans_skipped_no_capture: list[str]
        plans_failed: list[str]
        exit_code: int (0 when every attempted plan succeeded)
        error: str | None (set when the orchestrator itself failed
          before any exporter ran — e.g., missing manifest)
    """
    summary: dict[str, Any] = {
        "plans_attempted": [],
        "plans_skipped_no_capture": [],
        "plans_failed": [],
        "exit_code": 0,
        "error": None,
    }
    manifest = _read_manifest(bundle_dir)
    if manifest is None:
        summary["error"] = "manifest_missing"
        summary["exit_code"] = 2
        return summary
    resolved = manifest.get("resolved", {})
    experiment_id = resolved.get("mlflow_experiment_id")
    if not experiment_id:
        summary["error"] = "mlflow_experiment_id_missing"
        summary["exit_code"] = 2
        return summary
    captures_dir = bundle_dir / "evidence" / "gso_trial_captures"
    repo_root = _resolve_repo_root()
    if output_root is None:
        output_root = repo_root / "tests" / "fixtures"
    output_root.mkdir(parents=True, exist_ok=True)
    for plan in _DISPATCH:
        capture_path = captures_dir / f"{plan.plan_id}.ndjson"
        if not capture_path.is_file():
            summary["plans_skipped_no_capture"].append(plan.plan_id)
            continue
        out_dir = output_root / plan.plan_id
        out_dir.mkdir(parents=True, exist_ok=True)
        argv = [
            sys.executable,
            str(repo_root / plan.exporter_script),
            plan.capture_flag, str(capture_path),
            "--output-dir", str(out_dir),
        ]
        if plan.needs_mlflow_experiment_id:
            argv.extend(["--mlflow-experiment-id", str(experiment_id)])
        result = subprocess.run(argv, capture_output=True, text=True)
        if result.returncode == 0:
            summary["plans_attempted"].append(plan.plan_id)
        else:
            summary["plans_failed"].append(plan.plan_id)
            summary["exit_code"] = max(summary["exit_code"], result.returncode)
            print(
                f"[regen] plan {plan.plan_id} exporter exited "
                f"{result.returncode}\nstderr:\n{result.stderr}",
                file=sys.stderr,
            )
            print(f"[regen] stdout:\n{result.stdout}", file=sys.stderr)
            continue
        # Pass-through stdout for visibility — each exporter prints
        # a JSON summary.
        print(f"[regen] {plan.plan_id} exporter output:\n{result.stdout}")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "opt_run_id",
        help="optimization_run_id; bundle expected at "
             "packages/genie-space-optimizer/docs/runid_analysis/<opt_run_id>",
    )
    parser.add_argument(
        "--bundle-root",
        type=Path,
        default=_resolve_repo_root() / "docs" / "runid_analysis",
        help="Override bundle root (default: in-repo docs/runid_analysis).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Override fixture output root (default: in-repo tests/fixtures).",
    )
    args = parser.parse_args(argv)
    bundle_dir = args.bundle_root / args.opt_run_id
    if not bundle_dir.is_dir():
        print(
            f"bundle directory not found: {bundle_dir}\n"
            "Run the gso-postmortem skill first to produce the bundle.",
            file=sys.stderr,
        )
        return 2
    summary = regen_fixtures(bundle_dir=bundle_dir, output_root=args.output_root)
    print(json.dumps(summary, indent=2))
    return int(summary["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
