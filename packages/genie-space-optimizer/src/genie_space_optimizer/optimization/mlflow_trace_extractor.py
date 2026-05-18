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

Phase 3.7 (2026-05-18) — for historic lever6_llm calls that lack
breadcrumbs, we can reconstruct the binding by parsing the cluster_id
out of the prompt JSON and correlating against an export payload's
``iter_source_clusters_by_id`` / ``action_groups``. This is needed so
the ``historic_inject`` replay mode can key by (stage, iteration,
ag_id, cluster_id) instead of prompt SHA — see
``docs/architecture/stage-prompt-fidelity-audit.md``.
"""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Iterable, Iterator

from genie_space_optimizer.optimization.tape import _KNOWN_STAGES

logger = logging.getLogger(__name__)

# Phase 3.7 (2026-05-18) — match the ``"cluster_id": "..."`` line
# emitted by ``json.dumps(format_afs(cluster), indent=2)`` in
# ``_generate_lever6_proposal``. The AFS projection puts cluster_id
# first; the regex matches the FIRST occurrence in the prompt so any
# later cluster_id references (e.g., inside strategist_hints) do not
# shadow the binding cluster.
_LEVER6_CLUSTER_ID_PATTERN = re.compile(r'"cluster_id"\s*:\s*"([^"]+)"')


def extract_llm_calls_from_trace(
    trace,
    *,
    export_payload: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield call-shape dicts for every CHAIN span named in _KNOWN_STAGES
    that has a CHAT_MODEL child.

    Order: spans appear in the order MLflow returns them (typically
    start_time_ns ascending). The caller is responsible for any
    re-ordering needed for replay determinism.

    Phase 3.7 (2026-05-18) — when ``export_payload`` is provided and a
    lever6_llm parent span lacks binding breadcrumbs (iteration=-1 /
    empty ag/cluster), the cluster_id is parsed from the prompt JSON
    and the (iteration, ag_id) pair is reconciled from the export's
    ``iter_source_clusters_by_id`` / ``action_groups``. Calls whose
    breadcrumbs were already set live (post-Phase-3.6-Task-2 captures)
    are emitted unchanged.
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

        iteration = int(binding.get("iteration", -1))
        ag_id = str(binding.get("ag_id", ""))
        cluster_id = str(binding.get("cluster_id", ""))

        # Phase 3.7 — backfill lever6_llm binding from prompt + export
        # when the trace lacks live breadcrumbs. Only fires for
        # historic captures (iteration == -1 AND both ids empty);
        # post-Task-2 captures retain their authoritative breadcrumbs.
        if (
            name == "lever6_llm"
            and export_payload is not None
            and iteration == -1
            and not ag_id
            and not cluster_id
        ):
            parsed_cluster = _extract_cluster_id_from_lever6_prompt(prompt)
            if parsed_cluster:
                rec_iter, rec_ag = reconcile_lever6_binding_from_export(
                    cluster_id=parsed_cluster,
                    export_payload=export_payload,
                )
                cluster_id = parsed_cluster
                iteration = rec_iter
                ag_id = rec_ag

        yield {
            "span_name": name,
            "iteration": iteration,
            "ag_id": ag_id,
            "cluster_id": cluster_id,
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
    *,
    export_payload: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    """Convenience wrapper: extract from a sequence of traces in order.

    Phase 3.7 — ``export_payload`` is forwarded to each
    ``extract_llm_calls_from_trace`` invocation so lever6_llm
    binding reconciliation works across multi-trace captures.
    """
    for trace in traces:
        yield from extract_llm_calls_from_trace(
            trace, export_payload=export_payload,
        )


def _extract_cluster_id_from_lever6_prompt(prompt: str) -> str | None:
    """Phase 3.7 — return the FIRST ``"cluster_id": "..."`` value in a
    lever6_llm prompt, or None if not found.

    The lever6 prompt-builder serializes ``format_afs(cluster)`` via
    ``json.dumps(..., indent=2)``; the AFS projection emits cluster_id
    as its first key, so the first regex hit is the binding cluster.
    """
    if not prompt:
        return None
    m = _LEVER6_CLUSTER_ID_PATTERN.search(prompt)
    if m is None:
        return None
    val = m.group(1).strip()
    return val or None


def reconcile_lever6_binding_from_export(
    *,
    cluster_id: str,
    export_payload: dict[str, Any],
) -> tuple[int, str]:
    """Phase 3.7 — given a cluster_id and an export payload, return
    ``(iteration, ag_id)``.

    Algorithm:
      1. Walk ``export_payload["iterations"]`` in order.
      2. For each iteration whose ``iter_source_clusters_by_id``
         contains ``cluster_id``, scan
         ``strategist_response.action_groups`` for the first AG whose
         ``source_cluster_ids`` contains ``cluster_id``.
      3. Return that pair. If the cluster appears in an iteration but
         no AG claims it, return ``(iteration, "")``.
      4. If the cluster is absent from every iteration, return
         ``(-1, "")``.
    """
    if not cluster_id or not isinstance(export_payload, dict):
        return (-1, "")
    iterations = export_payload.get("iterations") or []
    for it in iterations:
        if not isinstance(it, dict):
            continue
        src = it.get("iter_source_clusters_by_id") or {}
        if not isinstance(src, dict):
            continue
        if cluster_id not in src:
            continue
        iter_num = int(it.get("iteration") or 0)
        strategist = it.get("strategist_response") or {}
        action_groups = (
            strategist.get("action_groups") if isinstance(strategist, dict) else None
        ) or []
        for ag in action_groups:
            if not isinstance(ag, dict):
                continue
            scids = ag.get("source_cluster_ids") or []
            if cluster_id in [str(c) for c in scids]:
                return (iter_num, str(ag.get("id") or ""))
        return (iter_num, "")
    return (-1, "")


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
