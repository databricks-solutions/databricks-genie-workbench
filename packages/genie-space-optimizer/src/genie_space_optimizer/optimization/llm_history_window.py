"""Phase 1 P1.3 — fixed-window history capping for stage payloads.

The Stage 1 (``diagnose``) and Stage 3 (``synthesize``) prompts both
accumulate history slots that grow linearly with the iteration count:

  * Stage 1: ``recent_diagnoses_for_same_qids``
  * Stage 3: ``history``, ``forbidden_signatures``,
    ``insufficient_repair_signatures``

By iteration 8 these slots dominate the prompt token budget — the
e94376a3 postmortem traced 22-of-32 Stage 1 rate-limit-starved
diagnoses to exactly this growth. Phase 0 P0.4 added LRU compaction as
a *fail-safe* (it drops oldest entries when the assembled prompt
exceeds :data:`MAX_PROMPT_INPUT_TOKENS`). Phase 1 P1.3 adds a *fixed
window* at the structural level so the prompt size is bounded
*before* the compactor sees it.

The contract is:

  * Slots whose entries carry an ``iteration`` field
    (history, recent_diagnoses) are capped to the last
    :data:`LAST_N_ITERATIONS` iterations. Older entries are collapsed
    into a single digest dict tagged ``"__digest__": True`` so the LLM
    still observes "something happened earlier" without paying for
    every per-entry token.

  * Flat signature lists (``forbidden_signatures``,
    ``insufficient_repair_signatures``) lack iteration metadata. They
    are capped to the last :data:`LAST_N_SIGNATURES` entries. Older
    signatures are collapsed into a single digest token at the head
    of the list.

Both helpers preserve the original list type (returning a fresh
``list``) so callers can drop the result into the same place the
uncapped list was being used. The helpers never raise on malformed
input — missing ``iteration`` defaults to ``0`` so the entry is
treated as "from iteration 0 and thus older than current_iteration -
last_n" for any positive current iteration.
"""
from __future__ import annotations

from typing import Any

# Fixed-window size for iteration-bucketed history slots. 3 is the
# minimum needed to support the C2 pivot logic which compares the
# current and prior two iterations' lever choices to detect cycles.
LAST_N_ITERATIONS: int = 3

# Fixed-window size for flat signature lists. Chosen to be roughly
# equivalent to LAST_N_ITERATIONS * 10 sigs/iter, with a small safety
# buffer so a burst-iteration that emits 12+ unique signatures does
# not lose the immediate prior iteration's signal.
LAST_N_SIGNATURES: int = 30


def _safe_iteration(entry: Any) -> int:
    """Best-effort extraction of an ``iteration`` field from an entry
    dict. Missing / non-int → 0 so the entry is treated as old."""
    if not isinstance(entry, dict):
        return 0
    raw = entry.get("iteration", 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _family_label(entry: dict) -> str:
    """Pick the most-likely family label from a history-style dict.

    Stage 1 ``recent_diagnoses_for_same_qids`` entries carry
    ``rca_kind_label``; Stage 3 ``history`` entries carry
    ``patch_family`` / ``family`` / ``rca_kind``. We try them in that
    order and fall back to ``unknown`` so the digest still has a
    bucket even for malformed entries.
    """
    for key in ("rca_kind_label", "patch_family", "family", "rca_kind"):
        v = entry.get(key)
        if isinstance(v, str) and v:
            return v
    return "unknown"


def _outcome_label(entry: dict) -> str:
    """Pick the most-likely outcome label from a Stage 3 history
    entry. Used in the histogram so the LLM sees not just *which*
    families were tried older but also how they resolved."""
    for key in ("outcome", "verdict", "acceptance_outcome", "status"):
        v = entry.get(key)
        if isinstance(v, str) and v:
            return v
    return "unknown"


def cap_iteration_bucketed_history(
    items: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
    *,
    current_iteration: int,
    last_n: int = LAST_N_ITERATIONS,
) -> list[dict[str, Any]]:
    """Cap a list of history dicts to the last ``last_n`` iterations.

    Entries with ``iteration >= current_iteration - last_n`` are
    retained verbatim. Older entries are collapsed into a single
    digest dict prepended at index 0:

        {
            "__digest__": True,
            "older_iterations_count": <int>,
            "older_iterations_range": [<lo>, <hi>],
            "family_histogram": {"<family>": <count>, ...},
            "outcome_histogram": {"<outcome>": <count>, ...},
        }

    When no entries are older the digest is omitted entirely.
    """
    if not items:
        return []
    seq = list(items)
    # "last N iterations" includes the current iteration itself: for
    # current=10, last_n=3, we keep iterations {8, 9, 10}. So the
    # cutoff (inclusive lower bound) is current_iteration - last_n + 1.
    cutoff = int(current_iteration) - int(last_n) + 1
    recent: list[dict[str, Any]] = []
    older: list[dict[str, Any]] = []
    for entry in seq:
        if not isinstance(entry, dict):
            continue
        if _safe_iteration(entry) >= cutoff:
            recent.append(entry)
        else:
            older.append(entry)
    if not older:
        return recent
    families: dict[str, int] = {}
    outcomes: dict[str, int] = {}
    for entry in older:
        families[_family_label(entry)] = (
            families.get(_family_label(entry), 0) + 1
        )
        outcomes[_outcome_label(entry)] = (
            outcomes.get(_outcome_label(entry), 0) + 1
        )
    iter_lo = min(_safe_iteration(e) for e in older)
    iter_hi = max(_safe_iteration(e) for e in older)
    digest: dict[str, Any] = {
        "__digest__": True,
        "older_iterations_count": len(older),
        "older_iterations_range": [iter_lo, iter_hi],
        "family_histogram": dict(sorted(families.items())),
        "outcome_histogram": dict(sorted(outcomes.items())),
    }
    return [digest, *recent]


def _signature_family(signature: str) -> str:
    """Parse the family token out of a signature string.

    Signatures use the harness convention
    ``"<lever>:<patch_type>:<rca_kind>:<behavior>"`` (see
    ``harvest_sm_insufficient_repair_signatures``). The rca_kind is
    the third colon-separated token; we treat it as the family label
    for histogramming. Malformed signatures fall back to ``unknown``
    so the digest still buckets them somewhere.
    """
    if not isinstance(signature, str):
        return "unknown"
    parts = signature.split(":")
    if len(parts) >= 3 and parts[2]:
        return parts[2]
    if parts:
        return parts[0]
    return "unknown"


def cap_signature_list(
    signatures: tuple[str, ...] | list[str] | None,
    *,
    last_n: int = LAST_N_SIGNATURES,
) -> list[str]:
    """Cap a flat signature list to the last ``last_n`` entries.

    When the input has more than ``last_n`` entries, the first
    ``len-last_n`` entries are collapsed into a single digest token
    prepended at index 0:

        ``"__digest__:older_count=<N>;families=<fam1>=<c1>,..."``

    The digest token is intentionally a string (not a dict) so it
    remains type-compatible with the surrounding signature list and
    fits whatever JSON shape the prompt template expects.
    """
    if not signatures:
        return []
    seq = [str(s) for s in signatures if s is not None]
    if len(seq) <= int(last_n):
        return seq
    older = seq[: len(seq) - int(last_n)]
    recent = seq[len(seq) - int(last_n) :]
    families: dict[str, int] = {}
    for s in older:
        fam = _signature_family(s)
        families[fam] = families.get(fam, 0) + 1
    fam_part = ",".join(
        f"{k}={v}" for k, v in sorted(families.items())
    )
    digest = (
        f"__digest__:older_count={len(older)};families={fam_part}"
    )
    return [digest, *recent]
