"""Phase 0 P0.3 — rate-limit-aware retry helpers for FMAPI calls.

Centralizes the "is this a 429?" detection and the "how long should I
sleep?" calculation so both ``llm_client.call_llm`` and
``optimizer._traced_llm_call`` retry identically without duplicating
the parsing logic. The behaviour matches what Databricks' FMAPI
documents for ``REQUEST_LIMIT_EXCEEDED``:

  1. If the response carries a ``Retry-After`` header, honor it.
  2. Otherwise back off exponentially (``2**attempt`` seconds).
  3. Add 0..2 seconds of jitter so concurrent callers don't all
     unblock on the same tick.
  4. Cap at ``LLM_MAX_RETRIES`` attempts (3 in production). On
     exhaustion, callers convert the exception into a typed
     ``OPTIMIZER_CAPACITY_STARVED`` decline so the strategist does
     not interpret the failure as "this lever family does not work".
"""
from __future__ import annotations

import random
import re
from typing import Any


_RATE_LIMIT_EXCEPTION_NAMES: tuple[str, ...] = (
    # openai SDK
    "RateLimitError",
    # databricks/anthropic shapes that occasionally surface
    "TooManyRequestsError",
    "RateLimitException",
)

_RATE_LIMIT_MESSAGE_HINTS: tuple[str, ...] = (
    "REQUEST_LIMIT_EXCEEDED",
    "rate limit exceeded",
    "rate-limit exceeded",
    "Too Many Requests",
    "input token rate limit",
    "tokens per minute",
)

_RETRY_AFTER_HEADER_KEYS: tuple[str, ...] = (
    "Retry-After",
    "retry-after",
    "X-RateLimit-Reset",
    "x-ratelimit-reset",
)


def is_rate_limit_error(exc: BaseException) -> bool:
    """Return True iff ``exc`` looks like a Databricks/OpenAI 429.

    Detection is intentionally textual + class-name based so the
    helper does not import the ``openai`` package (importing it
    here would couple every consumer to a specific SDK version).
    """
    cls_name = type(exc).__name__
    if cls_name in _RATE_LIMIT_EXCEPTION_NAMES:
        return True
    msg = str(exc)
    if not msg:
        # OpenAI ``RateLimitError`` exposes the body via ``.body`` —
        # fall back to that surface if the message is empty.
        body = getattr(exc, "body", None)
        if body is not None:
            msg = str(body)
    for hint in _RATE_LIMIT_MESSAGE_HINTS:
        if hint.lower() in msg.lower():
            return True
    # Some SDKs surface only HTTP status; check ``status_code`` /
    # ``response.status_code`` as a last resort.
    status = getattr(exc, "status_code", None)
    if status is None:
        response = getattr(exc, "response", None)
        if response is not None:
            status = getattr(response, "status_code", None)
    if isinstance(status, int) and status == 429:
        return True
    return False


def parse_retry_after_seconds(exc: BaseException) -> float:
    """Extract ``Retry-After`` (or equivalent) from ``exc``'s headers
    or message; return 0.0 if no header is present.

    Honors integer seconds and HTTP-date are not parsed (the
    Databricks endpoint returns integer seconds in practice). Bounded
    to a defensive ceiling of 120s so a misconfigured server cannot
    pin the optimizer indefinitely.
    """
    headers = _extract_headers(exc)
    if headers:
        for key in _RETRY_AFTER_HEADER_KEYS:
            raw = headers.get(key)
            if raw is None:
                continue
            try:
                seconds = float(raw)
                if seconds > 0:
                    return min(seconds, 120.0)
            except (TypeError, ValueError):
                continue
    # Fall back to scanning the message body — Databricks sometimes
    # includes a ``retry after N seconds`` phrase in the JSON body.
    msg = str(exc) or str(getattr(exc, "body", "") or "")
    match = re.search(
        r"retry[- ]?after\D+(\d+(?:\.\d+)?)\s*(?:s|sec|seconds)?",
        msg, flags=re.IGNORECASE,
    )
    if match:
        try:
            return min(float(match.group(1)), 120.0)
        except ValueError:
            return 0.0
    return 0.0


def compute_rate_limit_backoff_seconds(
    exc: BaseException, *, attempt: int,
    jitter_seconds: float = 2.0,
    rng: random.Random | None = None,
) -> float:
    """Combine server-provided ``Retry-After`` with an exponential
    backoff floor and bounded jitter; returns total sleep seconds.

    Formula: ``max(retry_after, 2 ** attempt) + uniform(0, jitter)``.
    ``attempt`` is zero-indexed; the first retry sleeps at least 1s
    before re-trying.
    """
    retry_after = parse_retry_after_seconds(exc)
    floor = float(2 ** max(0, int(attempt)))
    base = max(retry_after, floor)
    if jitter_seconds <= 0:
        return base
    if rng is None:
        rng = random
    return base + rng.uniform(0.0, jitter_seconds)


def _extract_headers(exc: BaseException) -> dict[str, Any] | None:
    """Pull a header-mapping out of ``exc`` if one exists.

    The OpenAI SDK puts headers on ``exc.response.headers``; some
    Databricks shapes put them on ``exc.headers`` directly. Return
    ``None`` when neither surface is available.
    """
    headers = getattr(exc, "headers", None)
    if headers:
        return dict(headers)
    response = getattr(exc, "response", None)
    if response is not None:
        h = getattr(response, "headers", None)
        if h:
            return dict(h)
    return None
