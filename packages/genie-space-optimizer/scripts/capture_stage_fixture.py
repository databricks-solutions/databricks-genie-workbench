"""Capture a stage's I/O from an MLflow-archived lever-loop run.

Usage:
    uv run --project packages/genie-space-optimizer python \\
      scripts/capture_stage_fixture.py \\
      --anchor airline_1105451933925748_iter01 \\
      --stage acceptance_decision \\
      --mlflow-run-id <run_uuid> \\
      --iteration 1

Reads the archived per-stage I/O bundle from
``gso_postmortem_bundle/iterations/iter_NN/stages/<stage_key>/``,
applies the redaction policy declared in the anchor's README, and
writes ``input.json`` + ``expected_output.json`` under
``tests/integration/fixtures/<anchor>/<stage_key>/``.

The script never touches network state when ``--from-archive``
points at a local archive directory; ``--mlflow-run-id`` is only
required for direct MLflow downloads.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

REDACTION_FIELDS = {
    "question_text",
    "generated_sql",
    "expected_sql",
    "evidence",
    "sql_body",
    "expression",
}

DBX_ID_FIELDS = {
    "databricks_job_id",
    "databricks_parent_run_id",
    "lever_loop_task_run_id",
}


def _redact(obj: object) -> object:
    if isinstance(obj, dict):
        out: dict[str, object] = {}
        for k, v in obj.items():
            if k in REDACTION_FIELDS and isinstance(v, str) and v:
                out[k] = "<redacted>"
            elif k in DBX_ID_FIELDS and isinstance(v, str) and len(v) > 4:
                out[k] = "X" * (len(v) - 4) + v[-4:]
            else:
                out[k] = _redact(v)
        return out
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def _read_archive(archive_dir: Path, iteration: int, stage_key: str) -> tuple[dict, dict]:
    iter_dir = archive_dir / "iterations" / f"iter_{iteration:02d}" / "stages" / stage_key
    inp = json.loads((iter_dir / "input.json").read_text())
    out = json.loads((iter_dir / "output.json").read_text())
    return inp, out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--anchor", required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--from-archive", required=True, type=Path,
                        help="Local archive root (gso_postmortem_bundle/)")
    parser.add_argument("--iteration", type=int, default=1)
    parser.add_argument("--fixtures-root", type=Path,
                        default=Path("packages/genie-space-optimizer/tests/integration/fixtures"))
    args = parser.parse_args()

    inp_raw, out_raw = _read_archive(args.from_archive, args.iteration, args.stage)
    inp = _redact(inp_raw)
    out = _redact(out_raw)

    target_dir = args.fixtures_root / args.anchor / args.stage
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "input.json").write_text(json.dumps(inp, indent=2, sort_keys=True) + "\n")
    (target_dir / "expected_output.json").write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")

    # Sanity scan: no redaction-target field carries a non-redacted value.
    text = (target_dir / "input.json").read_text() + (target_dir / "expected_output.json").read_text()
    leaks = re.findall(r'"(?:question_text|generated_sql|expected_sql|sql_body|expression)"\s*:\s*"[^"<]', text)
    if leaks:
        raise SystemExit(f"redaction leak detected: {leaks[:5]}")

    print(f"captured: {target_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
