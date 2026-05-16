#!/usr/bin/env python
"""Export three-stage shadow-comparison records into committable
fixtures.

Reads:
  * NDJSON capture file from GSO_THREE_STAGE_CAPTURE_PATH
    (passed via --capture-path).
  * MLflow traces filtered to spans:
      - stage_1_discovery
      - lever_4_join_discovery
      - lever_5b_example_sql / lever_5b_example_sql_for_rca
      - lever_5a_instructions (Plan 2)
      - lever6_llm
      - adaptive_strategy (legacy, for shadow-mode comparison)

Writes one fixture file per shadow record into
tests/fixtures/three_stage_v1/<ag_id>__<short_hash>.json with the
comparison + the joined Stage-1/Stage-2 LLM bytes.

Idempotent: re-runs skip files whose ag_id + content hash exist.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


SPAN_NAMES_NEEDED = {
    "stage_1_discovery",
    "adaptive_strategy",
    "lever_4_join_discovery",
    "lever_5a_instructions",
    "lever_5b_example_sql",
    "lever_5b_example_sql_for_rca",
    "lever6_llm",
}


def _short_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


# Per-process metadata that captures may carry but that is irrelevant
# to the typed contract being pinned. Including these in the content
# hash would make every trial regenerate "new" fixture files for the
# same logical content, breaking the byte-stability gate.
_VOLATILE_HASH_KEYS: frozenset[str] = frozenset({
    "captured_at",
    "process_pid",
})


def _stable_hash_input(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``payload`` with ``_VOLATILE_HASH_KEYS`` removed,
    so the content hash depends only on the typed contract under test."""
    return {k: v for k, v in payload.items() if k not in _VOLATILE_HASH_KEYS}


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    out = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def _query_spans(experiment_id: str) -> list[Any]:
    import mlflow
    client = mlflow.tracking.MlflowClient()
    spans: list[Any] = []
    runs = client.search_runs([experiment_id], max_results=1000)
    for run in runs:
        try:
            traces = client.search_traces(
                experiment_ids=[experiment_id],
                run_id=run.info.run_id,
                max_results=200,
            )
        except Exception:
            continue
        for trace in traces:
            for span in trace.data.spans:
                if span.name in SPAN_NAMES_NEEDED:
                    spans.append(span)
    return spans


def _spans_in_window(spans: list[Any], target_ts: float, window_s: float = 600) -> list[Any]:
    return [s for s in spans
            if abs(getattr(s, "start_time", 0) / 1e9 - target_ts) <= window_s]


def _extract_io(span: Any) -> tuple[str, str]:
    inputs = getattr(span, "inputs", None) or {}
    outputs = getattr(span, "outputs", None) or {}
    prompt = ""
    response = ""
    if isinstance(inputs, dict):
        prompt = inputs.get("prompt") or inputs.get("user_prompt") or ""
    if isinstance(outputs, dict):
        response = outputs.get("text") or outputs.get("response") or ""
    elif isinstance(outputs, str):
        response = outputs
    return str(prompt), str(response)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--capture-path", required=True, type=Path)
    p.add_argument("--mlflow-experiment-id", required=True)
    p.add_argument("--output-dir", required=True, type=Path,
                   help="e.g. tests/fixtures/three_stage_v1")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    if not args.capture_path.is_file():
        print(f"capture file not found: {args.capture_path}", file=sys.stderr)
        return 1
    records = _read_ndjson(args.capture_path)
    if not records:
        print("capture file is empty — trial may not have triggered shadow/pipeline mode")
        return 1

    spans = _query_spans(args.mlflow_experiment_id)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped = 0

    for record in records:
        target_ts = float(record.get("captured_at") or 0)
        nearby = _spans_in_window(spans, target_ts) if target_ts > 0 else []
        by_name: dict[str, Any] = {}
        for s in nearby:
            by_name.setdefault(s.name, s)

        def _io_or_empty(name: str) -> dict:
            sp = by_name.get(name)
            if sp is None:
                return {"prompt_bytes": "", "response_bytes": ""}
            pp, rr = _extract_io(sp)
            return {"prompt_bytes": pp, "response_bytes": rr}

        # Reconstruct stage_2_results from MLflow spans the export
        # script saw inside the same time window. Mapping span_name →
        # canonical skill_id mirrors the catalogue in skill-catalogue.md.
        span_to_skill = {
            "lever_4_join_discovery": "lever-4-join-discovery",
            "lever_5a_instructions": "lever-5a-instructions",
            "lever_5b_example_sql": "lever-5b-example-sql",
            "lever_5b_example_sql_for_rca": "lever-5b-example-sql",
            "lever6_llm": "lever-6-sql-expression",
        }
        stage_2_results: list[dict] = []
        for span_name, sid in span_to_skill.items():
            sp = by_name.get(span_name)
            if sp is None:
                continue
            _, resp = _extract_io(sp)
            try:
                obj = json.loads(resp) if resp.strip().startswith("{") else {}
            except Exception:
                obj = {}
            stage_2_results.append({
                "skill_id": sid,
                "ag_id": record.get("ag_id", ""),
                "proposals": [obj] if obj else [],
            })

        payload: dict[str, Any] = {
            **record,
            "stage_1_discovery": _io_or_empty("stage_1_discovery"),
            "legacy_adaptive_strategy": _io_or_empty("adaptive_strategy"),
            "stage_2_results": stage_2_results,
        }

        content_hash = _short_hash(
            json.dumps(_stable_hash_input(payload), sort_keys=True, default=str)
        )
        out_path = args.output_dir / f"{(payload.get('ag_id') or 'unknownAG')}__{content_hash}.json"
        if out_path.exists():
            skipped += 1
            continue
        if args.dry_run:
            print(f"[dry-run] would write {out_path}")
        else:
            out_path.write_text(json.dumps(payload, indent=2, default=str),
                                 encoding="utf-8")
        written += 1

    print(json.dumps({
        "captures_read": len(records),
        "fixtures_written": written,
        "fixtures_skipped_existing": skipped,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
