#!/usr/bin/env python
"""Export hi-fidelity Plan-2 lever5-split fixtures from a real trial run.

Status
------
This is the **future hi-fidelity** Plan 2 fixture path. It captures the
holistic / 5a / 5b prompt+response bytes as actually rendered during a
live optimization trial that ran with ``GSO_LEVER5_SHADOW_V1=1`` (which
doubles LLM cost, so use sparingly).

For the **default** Plan 2 path that does not require a live shadow
trial, see ``scripts/export_lever5_split_fixtures_synthetic.py``. That
script builds ``(old, new)`` proposal pairs in memory, calls the real
``_emit_lever5_shadow_comparison`` to compute the comparison record,
and writes fixtures that satisfy the byte-stability test
(``tests/unit/optimization/test_lever5_split_fixtures.py``). Use the
synthetic path unless you specifically need real LLM-rendered prompts
in the fixture for a downstream consumer.

Inputs
------
* ``--capture-path`` — NDJSON written by ``_LeverFiveCaptureSink``
  during the shadow trial (one record per Stage-2 dispatch with
  ``GSO_LEVER5_SPLIT_CAPTURE_PATH`` set).
* ``--mlflow-experiment-id`` — experiment that owns the trial's
  traces; used to fetch the holistic / 5a / 5b spans corresponding to
  each NDJSON record.

Span model
----------
This exporter assumes traces with the following root-span names exist
in the target experiment, each carrying their LLM call's
prompt / response:

  - ``lever_5_holistic``
  - ``lever_5a_instructions``
  - ``lever_5b_example_sql``
  - ``lever_5b_example_sql_for_rca``

Note: The existing implementation reads ``inputs.prompt`` /
``outputs.text`` directly off the matched span. If a future trial is
run with the OpenAI autolog integration emitting ``Completions`` child
spans (the same shape as ``export_narrowing_fixtures.py``), this
helper will need a similar walk-to-child rewrite. Cross-reference
``scripts/export_narrowing_fixtures.py``'s
``_extract_prompt_and_response_from_completions`` for the pattern.

Outputs
-------
One fixture file per shadow record into
``tests/fixtures/lever5_split_v1/<ag_id>__<short_hash>.json`` with
structure:

    {
      "ag_id", "cluster_ids",
      "instruction_text_jaccard", "example_sqls_set_overlap",
      "old_example_sqls_count", "new_example_sqls_count",
      "old_example_sqls_hashes", "new_example_sqls_hashes",
      "lever_5_holistic":  {prompt_bytes, response_bytes},
      "lever_5a":           {instruction_text, rationale},
      "lever_5b_proposals": [ {example_sql, ...}, ... ]
    }

Idempotent: re-runs skip files whose ``ag_id`` + content hash already
exist.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


SPAN_NAMES_NEEDED = {
    "lever_5_holistic",
    "lever_5a_instructions",
    "lever_5b_example_sql",
    "lever_5b_example_sql_for_rca",
}


def _short_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


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


def _parse_5a_response(response: str) -> dict:
    try:
        # Allow fenced JSON for robustness.
        import re
        m = re.search(r"\{[\s\S]+\}", response)
        if not m:
            return {"instruction_text": "", "rationale": ""}
        return json.loads(m.group(0))
    except Exception:
        return {"instruction_text": "", "rationale": ""}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--capture-path", required=True, type=Path)
    p.add_argument("--mlflow-experiment-id", required=True)
    p.add_argument("--output-dir", required=True, type=Path,
                   help="e.g. tests/fixtures/lever5_split_v1")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    if not args.capture_path.is_file():
        print(f"capture file not found: {args.capture_path}", file=sys.stderr)
        return 1
    records = _read_ndjson(args.capture_path)
    if not records:
        print("capture file is empty — trial may not have triggered shadow mode")
        return 1

    spans = _query_spans(args.mlflow_experiment_id)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped_existing = 0
    seen_keys: set[str] = set()

    for record in records:
        target_ts = float(record.get("captured_at") or 0)
        nearby = _spans_in_window(spans, target_ts) if target_ts > 0 else []

        # Pull one span per name from the window.
        by_name: dict[str, Any] = {}
        for s in nearby:
            by_name.setdefault(s.name, s)

        holistic_span = by_name.get("lever_5_holistic")
        five_a_span = by_name.get("lever_5a_instructions")
        five_b_spans = [s for s in nearby
                         if s.name.startswith("lever_5b_example_sql")]

        holistic_io = _extract_io(holistic_span) if holistic_span else ("", "")
        five_a_io = _extract_io(five_a_span) if five_a_span else ("", "")
        five_a_parsed = _parse_5a_response(five_a_io[1]) if five_a_io[1] else {
            "instruction_text": "", "rationale": "",
        }
        five_b_proposals: list[dict] = []
        for sb in five_b_spans:
            try:
                resp = _extract_io(sb)[1]
                obj = json.loads(resp) if resp.strip().startswith("{") else {}
                if isinstance(obj, dict):
                    five_b_proposals.append({
                        "example_question": obj.get("example_question", ""),
                        "example_sql": obj.get("example_sql", ""),
                        "usage_guidance": obj.get("usage_guidance", ""),
                    })
            except Exception:
                continue

        payload: dict[str, Any] = {
            "ag_id": record.get("ag_id", ""),
            "cluster_ids": record.get("cluster_ids", []),
            "captured_at": target_ts,
            "instruction_text_jaccard": record.get("instruction_text_jaccard", 0.0),
            "example_sqls_set_overlap": record.get("example_sqls_set_overlap", 0.0),
            "old_example_sqls_count": record.get("old_example_sqls_count", 0),
            "new_example_sqls_count": record.get("new_example_sqls_count", 0),
            "old_example_sqls_hashes": record.get("old_example_sqls_hashes", []),
            "new_example_sqls_hashes": record.get("new_example_sqls_hashes", []),
            "lever_5_holistic": {
                "prompt_bytes": holistic_io[0],
                "response_bytes": holistic_io[1],
            },
            "lever_5a": {
                "instruction_text": five_a_parsed.get("instruction_text", ""),
                "rationale": five_a_parsed.get("rationale", ""),
            },
            "lever_5b_proposals": five_b_proposals,
        }
        # Stable filename: ag_id + content hash.
        content_hash = _short_hash(json.dumps(payload, sort_keys=True))
        key = f"{payload['ag_id'] or 'unknownAG'}__{content_hash}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        out_path = args.output_dir / f"{key}.json"
        if out_path.exists():
            skipped_existing += 1
            continue
        if args.dry_run:
            print(f"[dry-run] would write {out_path}")
        else:
            out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        written += 1

    print(json.dumps({
        "captures_read": len(records),
        "fixtures_written": written,
        "fixtures_skipped_existing": skipped_existing,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
