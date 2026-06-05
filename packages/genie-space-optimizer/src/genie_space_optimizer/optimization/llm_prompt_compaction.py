"""Phase 0 P0.4 — LRU compaction helpers for stage ``_build_request``.

When the assembled stage prompt exceeds
:data:`llm_reasoning_call.MAX_PROMPT_INPUT_TOKENS` (40k), the call
short-circuits with a ``PROMPT_TOO_LARGE`` abstain. To recover, the
caller must shrink its payload BEFORE invoking again. This module
provides a single-pass LRU compaction routine that trims the most
expendable parts of a stage payload — typically history-like
collections (recent diagnoses, forbidden signatures, prior iteration
markers) — in order of "least valuable first" until the estimated
input fits under the cap.

The contract is intentionally narrow:

  * Callers pass a list of ``(key, items)`` history slots ordered
    from "least valuable" to "most valuable". The compactor drops
    OLDEST entries from each slot in order until the prompt fits
    or the slot is empty.
  * The compactor estimates tokens via the same ``len // 4`` rule
    :class:`LlmReasoningCall` uses pre-admission so the two surfaces
    agree on what "40k tokens" means.
  * After compaction the caller re-serializes its payload and re-
    invokes the LLM call. The compactor itself never touches the
    LLM — it is pure size accounting.

The cap is conservative: we target ``MAX_PROMPT_INPUT_TOKENS - 1024``
so a small amount of churn between the estimate and the actual
encoded length cannot re-trip the gate.
"""
from __future__ import annotations

from collections.abc import MutableSequence


def estimate_tokens_from_chars(char_count: int) -> int:
    """Same heuristic as ``LlmReasoningCall``: ~4 chars per token.

    Centralized so both surfaces stay in sync if the heuristic is
    ever swapped for a tokenizer-based estimate."""
    return max(1, int(char_count) // 4)


def compact_history_slots_to_fit(
    *,
    static_chars: int,
    history_slots: list[tuple[str, MutableSequence]],
    target_token_cap: int,
    safety_margin_tokens: int = 1024,
) -> dict[str, int]:
    """Drop oldest items from ``history_slots`` (low-value first) so the
    estimated input fits under ``target_token_cap``.

    Parameters
    ----------
    static_chars
        Total characters in the non-history part of the prompt
        (system_msg + the stable user payload). The compactor never
        touches these — if even the static part exceeds the cap the
        caller has a deeper problem the LRU cannot solve.
    history_slots
        A list of ``(label, mutable_list)`` pairs ordered from least
        valuable to most valuable. The compactor mutates the lists
        in place, popping from the FRONT (oldest) until either the
        slot is empty or the prompt fits.
    target_token_cap
        The absolute ceiling. Production uses
        :data:`llm_reasoning_call.MAX_PROMPT_INPUT_TOKENS`.
    safety_margin_tokens
        Subtracted from ``target_token_cap`` so the post-compaction
        prompt has a small buffer against encoding-vs-estimate drift.

    Returns
    -------
    dict[str, int]
        Map from slot label to count of items dropped. Useful for
        marker payloads ("dropped 7 recent_diagnoses entries to fit").
    """
    effective_cap = max(1, int(target_token_cap) - int(safety_margin_tokens))
    drops: dict[str, int] = {label: 0 for label, _ in history_slots}

    def _current_tokens() -> int:
        total_chars = static_chars
        for _, items in history_slots:
            for it in items:
                # ``str(it)`` is a coarse but stable surrogate for
                # the JSON-encoded length of dict/list entries; the
                # ratio between ``str(dict)`` and ``json.dumps(dict)``
                # is close to 1.0 and the safety margin absorbs the
                # difference.
                total_chars += len(str(it))
        return estimate_tokens_from_chars(total_chars)

    # Fast-path: nothing to do if we already fit.
    if _current_tokens() <= effective_cap:
        return drops

    for label, items in history_slots:
        while items and _current_tokens() > effective_cap:
            items.pop(0)
            drops[label] += 1
        if _current_tokens() <= effective_cap:
            break

    return drops
