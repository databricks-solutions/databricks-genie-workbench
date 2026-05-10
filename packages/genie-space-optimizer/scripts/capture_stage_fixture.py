"""Capture a stage's I/O from an MLflow-archived lever-loop run.

Usage:
    uv run --project packages/genie-space-optimizer python \\
      scripts/capture_stage_fixture.py \\
      --anchor airline_1105451933925748_iter01 \\
      --stage acceptance_decision \\
      --mlflow-run-id <run_uuid> \\
      --iteration 1

Reads the archived per-stage I/O bundle from
``gso_postmortem_bundle/iterations/iter_NN/stages/<NN>_<stage_key>/``,
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

from genie_space_optimizer.optimization.run_output_contract import (
    _STAGE_INDEX_BY_KEY,
)

REDACTION_FIELDS = {
    "question_text",
    "generated_sql",
    "expected_sql",
    "evidence",
    "sql_body",
    "expression",
    # Free-text / LLM-generated rationales
    "analysis_text",
    "rationale",
    "change_description",
    "counterfactual_fix",
    "counterfactual_fixes",
    # SQL / schema text that can carry customer data
    "sql",
    "definition",
    "description",
    "display_name",
    "alias",
    # LLM-proposed / measured values
    "proposed_value",
    "actual_value",
    "expected_value",
}

DBX_ID_FIELDS = {
    "databricks_job_id",
    "databricks_parent_run_id",
    "lever_loop_task_run_id",
    # Additional workspace / session IDs emitted by Phase H stage I/O
    "experiment_id",
    "client_request_id",
    "conversation_id",
}

# Keys whose string values may legitimately be long (structural / non-PII).
SAFE_LONG_TEXT_KEYS = {
    "stage_key",
    "run_id",
    "iteration_bundle_prefix",
    "process_stage_order",
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


def _run_pii_audit(redacted_payload: object) -> None:
    """Walk *redacted_payload* and fail loudly if any string field is
    longer than 200 chars and its key is not in SAFE_LONG_TEXT_KEYS.

    This is the second-pass fail-loud guard: better to abort the capture
    than to silently commit leaked customer data.
    """

    def _walk(obj: object) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if (
                    isinstance(v, str)
                    and len(v) > 200
                    and k not in SAFE_LONG_TEXT_KEYS
                ):
                    raise SystemExit(
                        f"unexpected long-text field {k!r}: consider adding to "
                        "REDACTION_FIELDS or SAFE_LONG_TEXT_KEYS"
                    )
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(redacted_payload)


def _read_archive(archive_dir: Path, iteration: int, stage_key: str) -> tuple[dict, dict]:
    # Resolve the numeric-prefixed stage directory (e.g. "09_acceptance_decision")
    # from the canonical PROCESS_STAGE_ORDER mapping so the capture script stays
    # in sync with production bundle layout even if order changes.
    if stage_key in _STAGE_INDEX_BY_KEY:
        idx = _STAGE_INDEX_BY_KEY[stage_key]
        stage_dir_name = f"{idx:02d}_{stage_key}"
        iter_dir = (
            archive_dir / "iterations" / f"iter_{iteration:02d}" / "stages" / stage_dir_name
        )
    else:
        # Fallback: glob for a single matching directory when the stage_key is
        # not (yet) in PROCESS_STAGE_ORDER (e.g. during development of a new
        # stage).  We assert exactly one match to prevent ambiguity.
        stages_dir = archive_dir / "iterations" / f"iter_{iteration:02d}" / "stages"
        matches = list(stages_dir.glob(f"*_{stage_key}"))
        if len(matches) == 0:
            raise FileNotFoundError(
                f"no stage directory matching '*_{stage_key}' under {stages_dir}"
            )
        if len(matches) > 1:
            raise RuntimeError(
                f"ambiguous stage directories for {stage_key!r}: {matches}"
            )
        iter_dir = matches[0]

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

    # Second-pass: fail loudly on unknown long-text fields in either payload.
    _run_pii_audit(inp)
    _run_pii_audit(out)

    target_dir = args.fixtures_root / args.anchor / args.stage
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "input.json").write_text(json.dumps(inp, indent=2, sort_keys=True) + "\n")
    (target_dir / "expected_output.json").write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")

    # Sanity scan: no redaction-target field carries a non-redacted value.
    text = (target_dir / "input.json").read_text() + (target_dir / "expected_output.json").read_text()
    leak_pattern = "|".join(re.escape(f) for f in sorted(REDACTION_FIELDS))
    leaks = re.findall(rf'"({leak_pattern})"\s*:\s*"[^"<]', text)
    if leaks:
        raise SystemExit(f"redaction leak detected: {leaks[:5]}")

    print(f"captured: {target_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
