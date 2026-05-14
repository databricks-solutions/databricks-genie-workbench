#!/usr/bin/env python
"""Export hi-fidelity Plan-1 narrowing fixtures from a real MLflow trial.

Status
------
This is the **future hi-fidelity** Plan 1 fixture path. It captures the
prompt as actually rendered and submitted to the LLM (with all template
placeholders filled in by the call site), drawn from MLflow trace spans
that were emitted during a live optimization trial.

For the **default** Plan 1 path that does not require a live LLM call to
exercise any non-causal site, see
``scripts/export_narrowing_fixtures_from_templates.py``. That script
snapshots the prompt-template constants directly and is sufficient to
satisfy the byte-stability test
(``tests/unit/optimization/test_narrowing_v1_fixtures.py``).

Use this MLflow-based exporter only when you have empirical evidence (a
recent trial run) that one of the three non-causal sites fired at LLM
call time:

* ``preflight-instruction-expand``       (root span: ``generate_proactive_instructions`` or ``expand_instructions``)
* ``lever-4-join-discovery``             (root span: ``lever_4_join_discovery``)
* ``preflight-sql-expression-seeding``   (root span: ``sql_expression_seeding_llm``)

These are cold paths in most genie-space optimizations. Confirm at least
one root trace with the matching name exists in the target experiment
before running this script.

Inputs
------
* ``--narrowing-capture-path`` — NDJSON capture file written by
  ``_NarrowingCaptureSink`` during the trial run (one record per
  ``_rca_contract_for(prompt_name)`` invocation, includes
  ``rendered_at_ts`` for span correlation).
* ``--mlflow-experiment-id`` — experiment that owns the trial's traces.
* ``--output-dir`` — where to write fixture files (typically
  ``tests/fixtures/narrowing_v1``).

Span model
----------
The harness uses ``_traced_llm_call`` (see ``optimizer.py``) to wrap each
LLM invocation. That wrapper opens an outer span with a domain name
(e.g. ``lever_4_join_discovery``) whose ``inputs`` carry the call's
metadata. The actual prompt/response bytes live in a **child** span
emitted by the OpenAI SDK auto-instrumentation — typically a
``Completions`` span of ``span_type == LLM`` whose ``inputs`` are the
OpenAI chat-completions request (``{"messages": [...]}``) and whose
``outputs`` are the OpenAI chat-completions response
(``{"choices": [{"message": {"content": "..."}}]}``).

This exporter walks each matching outer span, locates the first
``Completions`` child, and pulls prompt/response from there.

Outputs
-------
Each fixture file in ``--output-dir`` follows the schema:

    {
      "skill_id":            "<one of the three non-causal skill_ids>",
      "prompt_bytes":        "<rendered prompt text actually submitted>",
      "llm_response_bytes":  "<raw response text returned by the LLM>",
      "captured_at":         <float epoch seconds, from NDJSON record>,
      "source_span_id":      "<MLflow span_id of the Completions child>",
      "source":              "mlflow_completions_span"
    }

The output is idempotent: existing files with identical
``prompt_bytes`` are left untouched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


# Map skill_id (NDJSON `skill_id` field) to the MLflow root-trace names
# that the corresponding _traced_llm_call produces. Matches the catalogue at
# docs/prompt_improvements/skill-catalogue.md.
SKILL_TO_ROOT_TRACE_NAMES: dict[str, tuple[str, ...]] = {
    "lever-4-join-discovery": ("lever_4_join_discovery",),
    "preflight-sql-expression-seeding": ("sql_expression_seeding_llm",),
    # EXPAND_INSTRUCTION_PROMPT's call site builds span_name dynamically
    # per attempt (see optimizer.py around line 4253). Both observed
    # variants are accepted.
    "preflight-instruction-expand": (
        "generate_proactive_instructions",
        "expand_instructions",
    ),
}

# Span-name fragments that identify the OpenAI-SDK child span carrying
# the actual prompt + response bytes. The MLflow OpenAI autolog
# integration uses one of these names; we accept any.
_COMPLETIONS_CHILD_NAMES = (
    "Completions",
    "ChatCompletion",
    "OpenAI.chat.completions.create",
)


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


def _query_mlflow_traces(
    experiment_id: str,
    root_trace_names: set[str],
) -> list[Any]:
    """Return Trace objects whose root span name matches one of the
    requested names. One filtered ``search_traces`` call per name keeps
    the query language requirements minimal (no IN clause needed).

    Local import keeps mlflow optional for test environments.
    """
    import mlflow  # noqa: F401  (verifies the package is installed)
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    out: list[Any] = []
    for name in sorted(root_trace_names):
        flt = f"tags.`mlflow.traceName` = '{name}'"
        try:
            batch = client.search_traces(
                experiment_ids=[experiment_id],
                filter_string=flt,
                max_results=200,
                order_by=["timestamp_ms ASC"],
            )
        except Exception as exc:  # network / auth / unsupported filter
            print(
                f"warning: search_traces failed for name={name!r}: {exc}",
                file=sys.stderr,
            )
            continue
        out.extend(batch)
    return out


def _find_completions_child(trace: Any, root_span_name: str) -> Any | None:
    """Locate the OpenAI-autolog ``Completions`` child span for the
    given root-span name within a single trace. Returns ``None`` if the
    trace has no recognizable child completion span."""
    spans = list(getattr(trace.data, "spans", []) or [])
    if not spans:
        return None

    # Identify the root span by name, then find its first descendant
    # whose name matches one of the known completions-span names.
    root = next((s for s in spans if s.name == root_span_name), None)
    if root is None:
        return None

    by_parent: dict[str, list[Any]] = {}
    for span in spans:
        parent = getattr(span, "parent_id", None) or ""
        by_parent.setdefault(parent, []).append(span)

    # BFS from root for any span whose name matches a known completions name.
    queue: list[Any] = list(by_parent.get(root.span_id, []))
    while queue:
        cur = queue.pop(0)
        if any(frag in cur.name for frag in _COMPLETIONS_CHILD_NAMES):
            return cur
        # Some integrations use span_type=LLM without "Completions" in
        # the name. Accept those as a fallback.
        span_type = str(getattr(cur, "span_type", "") or "").upper()
        if span_type in {"LLM", "CHAT_MODEL", "COMPLETIONS"}:
            return cur
        queue.extend(by_parent.get(cur.span_id, []))
    return None


def _extract_prompt_and_response_from_completions(span: Any) -> tuple[str, str]:
    """Extract prompt/response bytes from a Completions child span. The
    inputs are the OpenAI chat-completions request; the outputs are the
    response. Falls back to permissive lookups if the integration uses
    a slightly different shape."""
    inputs = getattr(span, "inputs", None) or {}
    outputs = getattr(span, "outputs", None) or {}

    prompt_text = ""
    if isinstance(inputs, dict):
        messages = inputs.get("messages")
        if isinstance(messages, list):
            # Render the prompt as the concatenation of "role: content"
            # lines so the byte-stability check sees the full payload
            # actually sent (including any system message prefix).
            parts = []
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                role = str(msg.get("role", ""))
                content = msg.get("content", "")
                if isinstance(content, list):
                    # OpenAI vision-style content blocks; flatten to text.
                    content = "".join(
                        block.get("text", "")
                        for block in content
                        if isinstance(block, dict)
                    )
                parts.append(f"{role}: {content}")
            prompt_text = "\n\n".join(parts)
        if not prompt_text:
            # Older convention: a single 'prompt' string.
            prompt_text = str(
                inputs.get("prompt") or inputs.get("user_prompt") or ""
            )

    response_text = ""
    if isinstance(outputs, dict):
        choices = outputs.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                msg = first.get("message")
                if isinstance(msg, dict):
                    response_text = str(msg.get("content") or "")
                if not response_text:
                    response_text = str(first.get("text") or "")
        if not response_text:
            response_text = str(outputs.get("text") or outputs.get("response") or "")
    elif isinstance(outputs, str):
        response_text = outputs

    return prompt_text, response_text


def _match_trace_for_record(
    record: dict[str, Any],
    traces_by_root_name: dict[str, list[Any]],
) -> tuple[Any, str] | None:
    """Pick the trace whose timestamp is closest to record's
    ``rendered_at_ts``, restricted to traces whose root-span name
    corresponds to the record's skill_id. Returns (trace, root_span_name)
    or ``None`` if no candidate."""
    candidate_root_names = SKILL_TO_ROOT_TRACE_NAMES.get(record["skill_id"], ())
    candidates: list[tuple[Any, str]] = []
    for name in candidate_root_names:
        for trace in traces_by_root_name.get(name, ()):
            candidates.append((trace, name))
    if not candidates:
        return None
    target_ts_ms = float(record["rendered_at_ts"]) * 1000.0
    return min(
        candidates,
        key=lambda item: abs(
            float(getattr(item[0].info, "timestamp_ms", 0.0)) - target_ts_ms
        ),
    )


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

    root_names_needed: set[str] = set()
    for skill_id in {r["skill_id"] for r in records}:
        root_names_needed.update(SKILL_TO_ROOT_TRACE_NAMES.get(skill_id, ()))

    traces = _query_mlflow_traces(args.mlflow_experiment_id, root_names_needed)
    traces_by_root_name: dict[str, list[Any]] = {}
    for t in traces:
        name = t.info.tags.get("mlflow.traceName", "")
        traces_by_root_name.setdefault(name, []).append(t)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped_existing = 0
    unmatched: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()

    for record in records:
        match = _match_trace_for_record(record, traces_by_root_name)
        if match is None:
            unmatched.append(record)
            continue
        trace, root_name = match
        child = _find_completions_child(trace, root_name)
        if child is None:
            unmatched.append(record)
            continue
        prompt_bytes, response_bytes = (
            _extract_prompt_and_response_from_completions(child)
        )
        if not prompt_bytes:
            unmatched.append(record)
            continue
        h = _short_hash(prompt_bytes)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)

        payload = {
            "skill_id": record["skill_id"],
            "prompt_bytes": prompt_bytes,
            "llm_response_bytes": response_bytes,
            "captured_at": record["rendered_at_ts"],
            "source_span_id": getattr(child, "span_id", "") or "",
            "source": "mlflow_completions_span",
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
            out_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
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
