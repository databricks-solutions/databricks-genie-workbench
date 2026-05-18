#!/usr/bin/env python3
"""Phase 3.6 Task 1 — discovery script: dump the span tree of a real MLflow trace.

Used once per historic run to confirm the span schema produced by
``mlflow.openai.autolog()`` in this workspace. The Phase 3.6 extractor
(``optimization/mlflow_trace_extractor.py``) assumes a particular
CHAIN→CHAT_MODEL shape; this script verifies that shape against a
real trace before the extractor is wired up.

Usage:
    python scripts/discover_mlflow_trace_shape.py \\
        --experiment-id <id> \\
        --run-id        <mlflow_run_id> \\
        --out           docs/architecture/mlflow-trace-shape-airline.md
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _span_to_dict(span) -> dict:
    return {
        "name": getattr(span, "name", None),
        "span_id": getattr(span, "span_id", None),
        "parent_id": (
            getattr(span, "parent_id", None)
            or getattr(span, "parent_span_id", None)
        ),
        "span_type": (
            getattr(span, "span_type", None)
            or (getattr(span, "attributes", None) or {}).get("mlflow.spanType")
        ),
        "start_time_ns": getattr(span, "start_time_ns", None),
        "end_time_ns": getattr(span, "end_time_ns", None),
        "inputs": getattr(span, "inputs", None),
        "outputs": getattr(span, "outputs", None),
        "attributes": (
            dict(span.attributes)
            if getattr(span, "attributes", None) else None
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    import mlflow
    from mlflow.client import MlflowClient

    mlflow.set_tracking_uri("databricks")
    client = MlflowClient()

    traces = client.search_traces(
        experiment_ids=[args.experiment_id],
        run_id=args.run_id,
        max_results=10000,
    )
    print(
        f"Found {len(traces)} trace(s) for run {args.run_id}",
        file=sys.stderr,
    )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    span_name_counts: dict[str, int] = {}
    chat_model_parent_names: dict[str, int] = {}
    example_chain: dict | None = None
    example_chat_model: dict | None = None

    for trace in traces:
        spans = (
            trace.data.spans if hasattr(trace, "data") else trace.spans
        )
        for span in spans:
            name = getattr(span, "name", "<unknown>")
            span_name_counts[name] = span_name_counts.get(name, 0) + 1
            span_type = (
                getattr(span, "span_type", None)
                or (getattr(span, "attributes", None) or {}).get(
                    "mlflow.spanType",
                )
            )
            if span_type == "CHAT_MODEL":
                parent_id = (
                    getattr(span, "parent_id", None)
                    or getattr(span, "parent_span_id", None)
                )
                if parent_id:
                    parent = next(
                        (
                            s for s in spans
                            if getattr(s, "span_id", None) == parent_id
                        ),
                        None,
                    )
                    if parent is not None:
                        chat_model_parent_names[parent.name] = (
                            chat_model_parent_names.get(parent.name, 0) + 1
                        )
                        if example_chat_model is None:
                            example_chain = _span_to_dict(parent)
                            example_chat_model = _span_to_dict(span)

    report: list[str] = []
    report.append("# Phase 3.6 Task 1 — MLflow trace shape discovery\n\n")
    report.append(
        f"Run id: `{args.run_id}` / experiment: `{args.experiment_id}`\n\n"
    )
    report.append(f"Traces examined: {len(traces)}\n\n")
    report.append("## Span name counts (top 30)\n\n")
    for n, c in sorted(span_name_counts.items(), key=lambda x: -x[1])[:30]:
        report.append(f"  - `{n}` × {c}\n")
    report.append(
        "\n## CHAT_MODEL parents (the LLM-call CHAIN spans we extract from)\n\n"
    )
    for n, c in sorted(chat_model_parent_names.items(), key=lambda x: -x[1]):
        report.append(f"  - `{n}` × {c}\n")
    report.append("\n## Example parent CHAIN span\n\n```json\n")
    report.append(json.dumps(example_chain, indent=2, default=str))
    report.append("\n```\n\n## Example child CHAT_MODEL span\n\n```json\n")
    report.append(json.dumps(example_chat_model, indent=2, default=str))
    report.append("\n```\n")
    out.write_text("".join(report), encoding="utf-8")
    print(f"Wrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
