"""Canonical question-id extraction shared across the lever-loop modules.

Eval rows reach the optimizer via several producer paths and arrive in
inconsistent shapes. Two independent extractors used to live in
``harness._baseline_row_qid`` (Track D, broad) and
``ground_truth_corrections._extract_question_id`` (narrower). They
diverged silently — Cycle 8 surfaced this when GT-correction
candidates whose qid lived in ``request.kwargs.question_id`` were
dropped with ``Skipping unidentifiable GT correction candidate``
even though the carrier saw the canonical qid fine.

This module owns the single source of truth for canonical-qid
extraction so the same divergence cannot recur. Both call sites are
thin wrappers over :func:`extract_question_id`.

The lookup order matches Track D: canonical sources first, trace-id
aliases (``client_request_id``, ``request_id``) only as a last-resort
fallback. The helper returns ``(qid, source)`` so callers that need
to distinguish (e.g. ``build_gt_correction_candidate`` logs a
structured warning when it falls back to a trace-id key) can branch
on the source without re-implementing the lookup.
"""

from __future__ import annotations

import json as _json
from typing import Any, Literal, Tuple

QidSource = Literal["canonical", "trace_fallback", ""]


def _coerce(value: Any) -> str:
    """Return ``str(value)`` with surrounding whitespace stripped, or
    ``""`` when the value is missing/blank. Mirrors the short-circuit
    semantics that ``_baseline_row_qid``'s ``or``-chain previously had."""
    if value is None:
        return ""
    s = str(value).strip()
    return s


def _from_canonical_keys(row: dict) -> str:
    """Check the canonical-source key chain in priority order.

    Order is significant: the cycle 5/6/7 lessons baked into Track D
    say top-level ``question_id`` wins over ``inputs.*``, which wins
    over nested ``request.kwargs.question_id``, etc.
    """
    # Top-level canonical aliases (flat dot/slash form included).
    for key in ("question_id", "id", "inputs/question_id", "inputs.question_id"):
        v = _coerce(row.get(key))
        if v:
            return v

    # Nested ``inputs`` dict.
    inputs = row.get("inputs")
    if isinstance(inputs, dict):
        for key in ("question_id", "id"):
            v = _coerce(inputs.get(key))
            if v:
                return v

    # Nested ``request`` (dict, or JSON-encoded string).
    request = row.get("request")
    request_dict: dict | None = None
    if isinstance(request, dict):
        request_dict = request
    elif isinstance(request, str):
        try:
            parsed = _json.loads(request)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, dict):
            request_dict = parsed
    if request_dict is not None:
        kwargs = request_dict.get("kwargs")
        if isinstance(kwargs, dict):
            v = _coerce(kwargs.get("question_id"))
            if v:
                return v
        v = _coerce(request_dict.get("question_id"))
        if v:
            return v

    # Synthetic harness rows put question_id under ``metadata``.
    meta = row.get("metadata")
    if isinstance(meta, dict):
        for key in ("question_id", "id"):
            v = _coerce(meta.get(key))
            if v:
                return v

    return ""


def _from_trace_id_keys(row: dict) -> str:
    """Last-resort fallback for rows that carry only a trace-id alias.

    Cycle 7 lesson: ``client_request_id`` is normally an MLflow trace
    ID like ``tr-...``, NOT a benchmark canonical QID. Returning it
    keeps the carrier from going silently empty, but callers should
    log/alert when they reach this path so producer-side bugs that
    misroute canonical qids into trace-id keys remain visible.
    """
    for key in ("client_request_id", "request_id"):
        v = _coerce(row.get(key))
        if v:
            return v
    return ""


def extract_question_id(row: dict) -> Tuple[str, QidSource]:
    """Return the row's question id and the source path that produced it.

    ``source`` is one of:

    * ``"canonical"`` — qid was found via a canonical-qid-bearing key
      (top-level ``question_id``/``id``, ``inputs.*``, ``request.*``,
      or ``metadata.*``). Safe to use without further sanity checks.
    * ``"trace_fallback"`` — only ``client_request_id`` / ``request_id``
      carried a value. The string is normally an MLflow trace ID;
      callers should treat it as identifying-but-non-canonical so they
      can emit a structured warning and surface producer-side bugs.
    * ``""`` — no extractable id at all. Callers must treat this as
      "unidentifiable" (the caller decides whether that is a soft
      warning or a hard error).
    """
    canonical = _from_canonical_keys(row)
    if canonical:
        return canonical, "canonical"
    fallback = _from_trace_id_keys(row)
    if fallback:
        return fallback, "trace_fallback"
    return "", ""
