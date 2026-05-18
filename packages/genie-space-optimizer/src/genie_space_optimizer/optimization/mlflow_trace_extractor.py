"""Phase 3.6 (2026-05-17) — MLflow trace → tape-entry extractor.

Given one or more MLflow traces (as returned by
``MlflowClient.search_traces``), walks the parent CHAIN spans named in
``tape._KNOWN_STAGES`` and emits dicts shaped like
``InMemoryLLMCallRecorder.calls[*]`` — the same shape the Phase 3.5
journey exporter writes into ``llm_call_log``. The Phase 3.6 CLI
script (``scripts/capture_tape_from_mlflow.py``) wraps this with auth
+ tape JSON assembly.

The extractor is pure: it takes already-fetched trace objects and
emits dicts. Tests use ``MagicMock`` to construct synthetic traces;
the real MLflow client only enters at the CLI layer.

The extractor handles two trace vintages:
  - Post-Task-2 traces: parent CHAIN span ``inputs`` carries
    ``iteration``, ``ag_id``, ``cluster_id``. Used directly.
  - Pre-Task-2 (historic) traces: no breadcrumbs. Each call's binding
    defaults to (-1, "", "") and the resulting tape ships with
    ``miss_policy="prompt_sha_only"`` so replay still matches.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Iterable, Iterator

from genie_space_optimizer.optimization.tape import _KNOWN_STAGES

logger = logging.getLogger(__name__)


def extract_llm_calls_from_trace(trace) -> Iterator[dict[str, Any]]:
    """Yield call-shape dicts for every CHAIN span named in _KNOWN_STAGES
    that has a CHAT_MODEL child.

    Order: spans appear in the order MLflow returns them (typically
    start_time_ns ascending). The caller is responsible for any
    re-ordering needed for replay determinism.
    """
    spans = _spans_of(trace)

    for parent in spans:
        name = _attr(parent, "name") or ""
        if name not in _KNOWN_STAGES:
            continue
        parent_id = _attr(parent, "span_id")
        chat_model_child = None
        for s in spans:
            if _attr(s, "parent_id") != parent_id:
                continue
            if _span_type(s) == "CHAT_MODEL":
                chat_model_child = s
                break
        if chat_model_child is None:
            continue

        prompt, system_msg = _read_prompt_and_system(chat_model_child)
        response_text = _read_response(chat_model_child)
        usage = _read_usage(chat_model_child)
        binding = _read_binding_breadcrumbs(parent)

        yield {
            "span_name": name,
            "iteration": int(binding.get("iteration", -1)),
            "ag_id": str(binding.get("ag_id", "")),
            "cluster_id": str(binding.get("cluster_id", "")),
            "prompt_sha256": _sha256(prompt),
            "system_msg": system_msg,
            "prompt": prompt,
            "response_text": response_text,
            "response_metadata": {
                "model": _read_model(chat_model_child),
                "temperature": _read_temperature(chat_model_child),
                **usage,
            },
        }


def extract_llm_calls_from_traces(
    traces: Iterable[object],
) -> Iterator[dict[str, Any]]:
    """Convenience wrapper: extract from a sequence of traces in order."""
    for trace in traces:
        yield from extract_llm_calls_from_trace(trace)


# ──────────────────────────────────────────────────────────────────────
# Internal helpers — kept tight so the Task 1 discovery report can map
# straight to each accessor. If MLflow ships a schema change, all the
# adjustments live in this section.
# ──────────────────────────────────────────────────────────────────────

def _spans_of(trace) -> list:
    if hasattr(trace, "data") and hasattr(trace.data, "spans"):
        return list(trace.data.spans)
    if hasattr(trace, "spans"):
        return list(trace.spans)
    return []


def _attr(obj, name):
    return getattr(obj, name, None)


def _span_type(span) -> str | None:
    t = _attr(span, "span_type")
    if t is not None:
        return str(t)
    attrs = _attr(span, "attributes") or {}
    return attrs.get("mlflow.spanType")


def _read_prompt_and_system(span) -> tuple[str, str]:
    """Pull the (user prompt, system message) pair from a CHAT_MODEL span.

    Schema (mlflow.openai.autolog, OpenAI v1 SDK):
        inputs.messages = [
            {role: "system", content: "..."} (optional),
            {role: "user",   content: "..."},
        ]
    We extract the LAST user message as the prompt and the FIRST
    system message as the system_msg. Multi-turn chat is rare in our
    code path; if it appears, we keep LAST user + FIRST system.
    """
    inputs = _attr(span, "inputs") or {}
    messages = inputs.get("messages") or []
    prompt = ""
    system_msg = ""
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role", "")).lower()
        content = str(m.get("content", "") or "")
        if role == "user":
            prompt = content
        elif role == "system" and not system_msg:
            system_msg = content
    return prompt, system_msg


def _read_response(span) -> str:
    outputs = _attr(span, "outputs") or {}
    choices = outputs.get("choices") or []
    if not choices:
        return ""
    first = choices[0]
    if isinstance(first, dict):
        message = first.get("message") or {}
        if isinstance(message, dict):
            return str(message.get("content") or "")
    return ""


def _read_usage(span) -> dict[str, int | None]:
    outputs = _attr(span, "outputs") or {}
    usage = outputs.get("usage") or {}
    if not isinstance(usage, dict):
        return {
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
        }
    return {
        "prompt_tokens": _int_or_none(usage.get("prompt_tokens")),
        "completion_tokens": _int_or_none(usage.get("completion_tokens")),
        "total_tokens": _int_or_none(usage.get("total_tokens")),
    }


def _read_model(span) -> str:
    inputs = _attr(span, "inputs") or {}
    return str(inputs.get("model") or "")


def _read_temperature(span) -> float | None:
    inputs = _attr(span, "inputs") or {}
    v = inputs.get("temperature")
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _read_binding_breadcrumbs(span) -> dict[str, Any]:
    inputs = _attr(span, "inputs") or {}
    return {
        "iteration": inputs.get("iteration", -1),
        "ag_id": inputs.get("ag_id", ""),
        "cluster_id": inputs.get("cluster_id", ""),
    }


def _int_or_none(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _sha256(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()
