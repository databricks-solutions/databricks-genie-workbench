#!/usr/bin/env python
"""Export narrowing-v1 trial-run captures into committable fixture files.

Reads:
  * NDJSON capture file written by _NarrowingCaptureSink during the trial
    run (path passed via --narrowing-capture-path).
  * MLflow traces for spans named one of:
      - lever_4_join_discovery
      - sql_expression_seeding_llm
      - generate_proactive_instructions  (or whatever span EXPAND_INSTRUCTION_PROMPT
        emits — discovered automatically below)
    via the MLflow tracking client (configured via env / MLFLOW_TRACKING_URI).

Writes:
  * One fixture file per (skill_id, captured render) into
    tests/fixtures/narrowing_v1/<skill_id>__<short_hash>.json
    Each file: {"skill_id", "prompt_bytes", "llm_response_bytes",
                "captured_at", "source_span_id"}.
  * A summary printed to stdout listing files emitted, files skipped,
    and any captures that could not be matched to an MLflow span.

Usage:
    python scripts/export_narrowing_fixtures.py \\
        --narrowing-capture-path /tmp/narrowing_v1.ndjson \\
        --mlflow-experiment-id <experiment_id> \\
        --output-dir tests/fixtures/narrowing_v1

The script is read-only with respect to MLflow and only writes to the
output directory. Existing fixture files with identical content are
left untouched (idempotent re-runs).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


# Map skill_id (NDJSON `skill_id` field) to the MLflow span_name that
# the corresponding _traced_llm_call uses. Matches the catalogue at
# docs/prompt_improvements/skill-catalogue.md.
SKILL_TO_SPAN_NAMES: dict[str, tuple[str, ...]] = {
    "lever-4-join-discovery": ("lever_4_join_discovery",),
    "preflight-sql-expression-seeding": ("sql_expression_seeding_llm",),
    # EXPAND_INSTRUCTION_PROMPT's call site at optimizer.py:4253 builds
    # span_name dynamically per attempt. Discover by prefix at runtime.
    "preflight-instruction-expand": (
        "generate_proactive_instructions",
        "expand_instructions",
    ),
}


def _short_hash(prompt_bytes: str) -> str:
    return hashlib.sha256(prompt_bytes.encode("utf-8")).hexdigest()[:12]


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    records = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records


def _query_mlflow_spans(experiment_id: str, span_names: set[str]) -> list[Any]:
    """Returns a list of MLflow span objects whose name is in span_names.
    Local import keeps mlflow optional for test environments."""
    import mlflow

    client = mlflow.tracking.MlflowClient()
    # Use experiment-scoped trace search; filter client-side because
    # MLflow's filter language does not support span-name filters.
    spans: list[Any] = []
    runs = client.search_runs([experiment_id], max_results=1000)
    for run in runs:
        try:
            traces = client.search_traces(experiment_ids=[experiment_id],
                                          run_id=run.info.run_id,
                                          max_results=200)
        except Exception:
            continue
        for trace in traces:
            for span in trace.data.spans:
                if span.name in span_names:
                    spans.append(span)
    return spans


def _match_span_for_record(
    record: dict[str, Any],
    spans_by_name: dict[str, list[Any]],
) -> Any | None:
    """Pick the MLflow span whose start time is closest to record's
    rendered_at_ts among spans with a matching span name."""
    candidate_span_names = SKILL_TO_SPAN_NAMES.get(record["skill_id"], ())
    candidates: list[Any] = []
    for name in candidate_span_names:
        candidates.extend(spans_by_name.get(name, []))
    if not candidates:
        return None
    target_ts = float(record["rendered_at_ts"])
    return min(
        candidates,
        key=lambda s: abs(getattr(s, "start_time", 0) / 1e9 - target_ts),
    )


def _extract_prompt_and_response(span: Any) -> tuple[str, str]:
    """Best-effort extraction of prompt + response bytes from an MLflow
    span. Span attribute layout follows _traced_llm_call's MLflow
    convention: input is the user prompt; output is the raw LLM text."""
    inputs = getattr(span, "inputs", None) or {}
    outputs = getattr(span, "outputs", None) or {}
    prompt = ""
    response = ""
    if isinstance(inputs, dict):
        # _traced_llm_call passes (system_msg, prompt) as positional args;
        # the 'prompt' key is added by the wrapper for searchability.
        prompt = inputs.get("prompt") or inputs.get("user_prompt") or ""
    if isinstance(outputs, dict):
        response = outputs.get("text") or outputs.get("response") or ""
    elif isinstance(outputs, str):
        response = outputs
    return str(prompt), str(response)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--narrowing-capture-path", required=True, type=Path)
    parser.add_argument("--mlflow-experiment-id", required=True)
    parser.add_argument(
        "--output-dir", required=True, type=Path,
        help="e.g. tests/fixtures/narrowing_v1",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be written without writing files.",
    )
    args = parser.parse_args(argv)

    if not args.narrowing_capture_path.is_file():
        print(f"capture file not found: {args.narrowing_capture_path}",
              file=sys.stderr)
        return 1

    records = _read_ndjson(args.narrowing_capture_path)
    if not records:
        print("capture file is empty — trial may have not exercised "
              "any non-causal site")
        return 1

    span_names_needed: set[str] = set()
    for skill_id in {r["skill_id"] for r in records}:
        span_names_needed.update(SKILL_TO_SPAN_NAMES.get(skill_id, ()))

    spans = _query_mlflow_spans(args.mlflow_experiment_id, span_names_needed)
    spans_by_name: dict[str, list[Any]] = {}
    for s in spans:
        spans_by_name.setdefault(s.name, []).append(s)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped_existing = 0
    unmatched: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()

    for record in records:
        span = _match_span_for_record(record, spans_by_name)
        if span is None:
            unmatched.append(record)
            continue
        prompt_bytes, response_bytes = _extract_prompt_and_response(span)
        if not prompt_bytes:
            unmatched.append(record)
            continue
        h = _short_hash(prompt_bytes)
        if h in seen_hashes:
            # Duplicate render of the same prompt — keep one fixture.
            continue
        seen_hashes.add(h)

        payload = {
            "skill_id": record["skill_id"],
            "prompt_bytes": prompt_bytes,
            "llm_response_bytes": response_bytes,
            "captured_at": record["rendered_at_ts"],
            "source_span_id": getattr(span, "span_id", "") or "",
        }
        out_path = args.output_dir / f"{record['skill_id']}__{h}.json"
        if out_path.exists():
            existing = json.loads(out_path.read_text(encoding="utf-8"))
            if existing.get("prompt_bytes") == prompt_bytes:
                skipped_existing += 1
                continue
        if args.dry_run:
            print(f"[dry-run] would write {out_path}")
        else:
            out_path.write_text(json.dumps(payload, indent=2),
                                encoding="utf-8")
        written += 1

    print(json.dumps({
        "captures_read": len(records),
        "fixtures_written": written,
        "fixtures_skipped_existing": skipped_existing,
        "captures_unmatched": len(unmatched),
        "unmatched_skills": sorted({r["skill_id"] for r in unmatched}),
    }, indent=2))
    return 0 if not unmatched else 2


if __name__ == "__main__":
    raise SystemExit(main())
