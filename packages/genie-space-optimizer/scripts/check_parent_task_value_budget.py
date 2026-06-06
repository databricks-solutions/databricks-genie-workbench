#!/usr/bin/env python3
"""Trial 25 W25.5 — pre-trigger task-value budget guardrail.

`gso-lever-loop-replay` MUST invoke this script BEFORE scheduling a
repair on an existing parent run. It counts the parent's accumulated
``taskValues`` and refuses to proceed when the count is at or above the
threshold (default 200; Databricks platform hard-cap is 250).

This converts a silent end-of-replay platform crash
(`PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250`) into an explicit
pre-replay rejection that the operator (and ``/goal``) can route around
by rotating the parent run via ``gso-lever-loop-trigger``.

Usage
-----
::

    python check_parent_task_value_budget.py \
        --job-id 488860692117207 \
        --parent-run-id 501649560474489 \
        --threshold 200 \
        --profile fevm-prashanth

Exit codes
----------
    0     PASS — count < threshold; safe to replay
    10    BLOCKED — count >= threshold; rotate parent via
          gso-lever-loop-trigger
    11    UNKNOWN — Databricks CLI returned non-zero exit; refuse to
          replay because we cannot prove the budget is safe
    2     bad usage (argparse rejects)

Stdout
------
A single line of the form ``GSO_TRIAL25_BUDGET_GATE_<VERDICT>_V1{json}``
where ``json`` is a structured payload the operator (and ``/goal``)
can parse for evidence.

Implementation note
-------------------
The Databricks CLI command used is
``databricks jobs get-run --include-resolved-values --run-id <id>
[--profile <p>]``. The ``--databricks-bin`` flag lets tests inject a
fake binary so the suite never touches a real workspace.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any, Optional


def _count_task_values_in_run_payload(payload: Optional[dict]) -> int:
    """Sum ``len(resolved_values)`` across every task in the run payload.

    The Databricks API returns a ``tasks`` list; each task may carry a
    ``resolved_values`` dict whose entries each cost one taskValue
    against the 250-entry parent budget. Tasks without resolved_values
    contribute zero. ``None`` and ``{}`` payloads return ``0`` so
    callers don't need to guard.
    """
    if not payload:
        return 0
    tasks = payload.get("tasks") or []
    total = 0
    for task in tasks:
        rv = task.get("resolved_values") or {}
        if isinstance(rv, dict):
            total += len(rv)
    return total


def _verdict(count: int, *, threshold: int) -> str:
    """``PASS`` when ``count < threshold``; ``NEAR_CEILING`` otherwise.

    The strict ``<`` is deliberate: AT the threshold we refuse, because
    even one further `taskValues.set` would push the parent over its
    250-entry platform cap and crash the run mid-publish."""
    return "PASS" if count < threshold else "NEAR_CEILING"


def _fetch_run_payload(
    *,
    job_id: int,
    parent_run_id: int,
    profile: Optional[str],
    databricks_bin: str,
) -> tuple[int, Optional[dict], str]:
    """Invoke the Databricks CLI and parse the JSON payload.

    Returns ``(cli_exit_code, parsed_payload_or_None, raw_stderr)``.
    Parsing errors are folded into ``parsed_payload_or_None = None``
    so the caller can treat them the same as a CLI failure (UNKNOWN
    verdict, refuse to replay).
    """
    # `_` references silence unused-arg complaints; job_id is included
    # in the verdict payload for operator context but the API call
    # itself only needs the run_id.
    _ = job_id
    cmd = [
        databricks_bin, "jobs", "get-run",
        "--include-resolved-values",
        "--run-id", str(parent_run_id),
    ]
    if profile:
        cmd.extend(["--profile", profile])

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return proc.returncode, None, proc.stderr or ""

    try:
        return 0, json.loads(proc.stdout), ""
    except (ValueError, TypeError) as exc:
        return 0, None, f"json.loads failed: {exc}: {proc.stdout[:200]!r}"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--job-id", type=int, required=True)
    parser.add_argument("--parent-run-id", type=int, required=True)
    parser.add_argument(
        "--threshold", type=int, default=200,
        help="block when accumulated task-value count >= this (default 200; "
             "Databricks platform hard-cap is 250)",
    )
    parser.add_argument(
        "--profile", default=None,
        help="Databricks CLI profile (passed via --profile to `databricks`)",
    )
    parser.add_argument(
        "--databricks-bin", default="databricks",
        help="path to the `databricks` CLI binary (override for tests)",
    )
    args = parser.parse_args(argv)

    cli_exit, payload, stderr_blob = _fetch_run_payload(
        job_id=args.job_id,
        parent_run_id=args.parent_run_id,
        profile=args.profile,
        databricks_bin=args.databricks_bin,
    )

    base_evidence: dict[str, Any] = {
        "job_id": args.job_id,
        "parent_run_id": args.parent_run_id,
        "threshold": args.threshold,
        "profile": args.profile,
    }

    if cli_exit != 0 or payload is None:
        evidence = {
            **base_evidence,
            "cli_exit_code": cli_exit,
            "stderr_excerpt": stderr_blob[:200],
            "reason": (
                "databricks CLI failed or returned non-JSON; cannot "
                "prove parent task-value budget is safe — refuse to "
                "replay. Re-authenticate the profile or rotate the "
                "parent via `gso-lever-loop-trigger`."
            ),
        }
        print(
            "GSO_TRIAL25_BUDGET_GATE_UNKNOWN_V1"
            + json.dumps(evidence, default=str)
        )
        return 11

    count = _count_task_values_in_run_payload(payload)
    verdict = _verdict(count, threshold=args.threshold)

    evidence = {
        **base_evidence,
        "task_value_count": count,
        "verdict": verdict,
    }

    if verdict == "PASS":
        print(
            "GSO_TRIAL25_BUDGET_GATE_PASSED_V1"
            + json.dumps(evidence, default=str)
        )
        return 0

    # NEAR_CEILING — name the remediation skill so the operator (and
    # /goal) can route around without round-tripping back to docs.
    evidence["remediation"] = (
        "rotate the anchor parent run via `gso-lever-loop-trigger` "
        "and update `canonical-anchors.md` Current parent job runs "
        "table; refuse this replay until rotation completes"
    )
    print(
        "GSO_TRIAL25_BUDGET_GATE_BLOCKED_V1"
        + json.dumps(evidence, default=str)
    )
    return 10


if __name__ == "__main__":  # pragma: no cover - thin CLI shim
    sys.exit(main())
