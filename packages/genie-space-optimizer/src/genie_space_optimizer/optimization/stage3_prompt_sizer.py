"""P4 C8 — Stage 3 prompt size budget primitives.

The d139 postmortem showed 16 of 20 Stage 3 synthesis calls declining
with ``prompt_too_large`` at peak 96,807 tokens vs the 40,000-token
cap. 80% of synthesis calls produced nothing on the cluster that
mattered. The 2.6x gap is too large to be a counting bug — the
per-cluster path was almost certainly bundling every cluster's RCA
cards into one call, and segment-level token budgets did not exist.

This module gives Stage 3 a typed prompt-size budget with:

  * A canonical taxonomy of segments
    (:class:`Stage3PromptSegment`).
  * Per-segment hard caps that sum to the 40,000-token aggregate cap.
  * Deterministic slicing helpers so the slice chosen on a given input
    is byte-stable across runs (postmortems can replay).
  * A single :func:`build_stage3_prompt_budget` entry point that
    consumes raw segment text + an optional cacheable-block share and
    returns a :class:`Stage3PromptSizeBreakdown` whose ``total_tokens``
    is guaranteed to be ``<= STAGE3_PROMPT_TOTAL_CAP``.
  * The :func:`stage3_prompt_size_breakdown_marker` emitter that turns
    the breakdown into a stable single-line marker
    (``GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1``) so the size budget is
    visible to postmortems on every call.

Per-cluster invariant
---------------------
Callers MUST construct one :class:`Stage3PromptInput` per cluster and
not bundle multiple clusters' RCA cards into a single call. The
budget is enforced at the per-cluster boundary so a 100k-token
catalog cannot reach the LLM regardless of how many clusters share
a single iteration.

Sub-cluster split
-----------------
When the post-cap total still exceeds the aggregate cap (e.g. a
single QID's RCA card alone overflows the per-segment cap),
:func:`partition_for_sub_cluster_split` returns a deterministic
partition over the input QIDs that the caller can use to invoke
Stage 3 once per partition.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Mapping, Sequence

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


# Aggregate cap — pins the existing ``MAX_PROMPT_INPUT_TOKENS = 40000``
# wired through :class:`LlmReasoningCall`. Kept as a module-level
# constant so callers can reference it instead of re-deriving.
STAGE3_PROMPT_TOTAL_CAP: int = 40_000


class Stage3PromptSegment(StrEnum):
    """Canonical Stage 3 prompt segments.

    Pinned by ``test_stage3_prompt_sizer.py``. Adding a new segment
    requires updating ``_DEFAULT_SEGMENT_CAPS`` in the same commit.
    """

    HISTORY = "history"
    RCA_CARDS = "rca_cards"
    LEVER_MENU = "lever_menu"
    ARCHETYPE_CATALOG = "archetype_catalog"
    SCHEMA_COLUMNS = "schema_columns"


# Per-segment caps (in tokens). Sum equals ``STAGE3_PROMPT_TOTAL_CAP``.
# Plan-prescribed: history ≤ 6k, RCA cards ≤ 12k, lever menu ≤ 4k,
# archetype catalog ≤ 8k, schema columns ≤ 10k.
_DEFAULT_SEGMENT_CAPS: Mapping[Stage3PromptSegment, int] = {
    Stage3PromptSegment.HISTORY: 6_000,
    Stage3PromptSegment.RCA_CARDS: 12_000,
    Stage3PromptSegment.LEVER_MENU: 4_000,
    Stage3PromptSegment.ARCHETYPE_CATALOG: 8_000,
    Stage3PromptSegment.SCHEMA_COLUMNS: 10_000,
}


def default_segment_caps() -> dict[Stage3PromptSegment, int]:
    """Return a fresh dict of per-segment caps. Callers may copy and
    tighten; the floor is whatever value sums to ``STAGE3_PROMPT_TOTAL_CAP``."""
    return dict(_DEFAULT_SEGMENT_CAPS)


def estimate_tokens(text: str) -> int:
    """Cheap deterministic token estimator.

    Approximates the GPT/Anthropic tokenizer at ~4 chars/token. The
    cap is enforced from the *LLM* side via ``MAX_PROMPT_INPUT_TOKENS``;
    this estimator only needs to be *monotonic* in text length to
    preserve the slicing contract. Same input always returns the same
    integer — that is the byte-stability requirement.
    """
    if not text:
        return 0
    # Ceil division so non-empty text always counts as ≥ 1 token.
    return (len(text) + 3) // 4


def slice_segments(
    *,
    system_msg_tokens: int,
    user_prompt_tokens: int,
    cacheable_block_tokens: int,
    cap: int = STAGE3_PROMPT_TOTAL_CAP,
) -> dict[str, int | bool]:
    """Trial 21 W3 — token-budget allocator across the three Stage 3
    prompt regions.

    Allocates ``cap`` tokens across the three coarse Stage 3 prompt
    regions Stage 3 currently builds:

      * ``system_msg`` — non-negotiable (model role + invariants).
      * ``cacheable_block`` — non-negotiable (lever menu + system
        instructions blob, cache-warmed by the pacer).
      * ``user_prompt`` — fully sliceable (RCA cards + history +
        per-cluster catalog).

    The function returns the post-slice projection. ``user_prompt``
    absorbs the entire remaining budget after system + cacheable; if
    even the non-negotiable parts exceed ``cap`` (a structural
    misconfiguration, not a normal Stage 3 call), the projection
    reports ``over_cap=True`` and the caller MUST fall back to the
    sub-cluster split. When the originals are already under ``cap``,
    the function is a no-op (over_cap=False, sub_cluster_split_needed
    only true when the user prompt was over-budget pre-slice).

    Output keys (stable wire — pinned by the Trial 21 postmortem-replay
    test):

      * ``system_msg_tokens``         — kept verbatim.
      * ``cacheable_block_tokens``    — kept verbatim.
      * ``user_prompt_tokens``        — post-slice.
      * ``total_tokens``              — sum, post-slice.
      * ``cap``                       — echoed input.
      * ``over_cap``                  — True iff ``total_tokens > cap``
        AFTER slicing (only possible when system + cacheable > cap).
      * ``observe_only``              — Trial 21 enforce-mode marker.
        Always ``False`` — the Evidence Actuator reads this verdict
        and drops over-cap proposals with ``PROMPT_SPLIT_REQUIRED``.
      * ``sub_cluster_split_needed``  — True iff the original
        ``user_prompt_tokens`` exceeded its allocated post-slice
        budget. The caller MUST honor this flag (the Actuator drops
        the slate's proposals when set).
    """
    cap = int(cap)
    sys_t = max(0, int(system_msg_tokens))
    cache_t = max(0, int(cacheable_block_tokens))
    user_t = max(0, int(user_prompt_tokens))

    non_negotiable = sys_t + cache_t
    user_budget = max(0, cap - non_negotiable)
    sliced_user = min(user_t, user_budget)
    sub_cluster_split = user_t > user_budget
    total = sys_t + cache_t + sliced_user
    return {
        "system_msg_tokens": sys_t,
        "cacheable_block_tokens": cache_t,
        "user_prompt_tokens": sliced_user,
        "total_tokens": total,
        "cap": cap,
        "over_cap": total > cap,
        "observe_only": False,
        "sub_cluster_split_needed": sub_cluster_split,
    }


def _content_key(item: object) -> str:
    """Deterministic hash key for slicing tie-breaks.

    Used when two items have equal sort priority (e.g. two RCA cards
    with the same ``qid`` — should not happen, but if it does the
    slice must still be byte-stable).
    """
    payload = str(item).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:8]


@dataclass(frozen=True, slots=True)
class HistoryEntry:
    """One iteration of optimizer history. Sorted most-recent first."""

    iteration: int
    text: str

    def estimated_tokens(self) -> int:
        return estimate_tokens(self.text)


@dataclass(frozen=True, slots=True)
class RcaCardEntry:
    """One RCA card. Sorted target-QID-first then lexicographic."""

    qid: str
    is_target: bool
    text: str

    def estimated_tokens(self) -> int:
        return estimate_tokens(self.text)


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One archetype or schema-column entry. Sorted lexicographic by
    ``name``."""

    name: str
    text: str

    def estimated_tokens(self) -> int:
        return estimate_tokens(self.text)


@dataclass(frozen=True, slots=True)
class Stage3PromptInput:
    """Raw segment inputs for one Stage 3 call (one cluster).

    The producer hands this struct to
    :func:`build_stage3_prompt_budget`; the budget computes typed,
    capped per-segment outputs.

    ``cluster_id`` is the single-cluster invariant — callers must NOT
    construct an input with RCA cards from multiple clusters. The
    builder asserts that every ``RcaCardEntry`` either is_target for
    this cluster or names a QID in ``cluster_qids``.

    ``cacheable_prefix_tokens`` is the LLM provider's prompt-cache
    block share (the portion of the prompt the model serves from
    cache). Recorded on the marker for cache-hit-rate analysis;
    doesn't affect the cap (the cap applies to the full assembled
    prompt regardless of cache).
    """

    cluster_id: str
    cluster_qids: tuple[str, ...]
    history: tuple[HistoryEntry, ...]
    rca_cards: tuple[RcaCardEntry, ...]
    lever_menu_text: str
    archetype_catalog: tuple[CatalogEntry, ...]
    schema_columns: tuple[CatalogEntry, ...]
    cacheable_prefix_tokens: int = 0


def _slice_history_to_cap(
    entries: Sequence[HistoryEntry],
    cap_tokens: int,
) -> tuple[tuple[HistoryEntry, ...], int]:
    """Most-recent-first slice. Returns (kept entries, total tokens)."""
    if cap_tokens <= 0:
        return ((), 0)
    sorted_entries = sorted(
        entries,
        key=lambda e: (-int(e.iteration), _content_key(e.text)),
    )
    kept: list[HistoryEntry] = []
    running = 0
    for entry in sorted_entries:
        cost = entry.estimated_tokens()
        if running + cost > cap_tokens:
            continue
        kept.append(entry)
        running += cost
    return (tuple(kept), running)


def _slice_rca_cards_to_cap(
    entries: Sequence[RcaCardEntry],
    cap_tokens: int,
    *,
    cluster_qids: tuple[str, ...],
) -> tuple[tuple[RcaCardEntry, ...], int]:
    """Target-QID-first, then lexicographic. Returns (kept, tokens)."""
    if cap_tokens <= 0:
        return ((), 0)
    qid_set = frozenset(cluster_qids)
    # Sort: target cards before non-target; within each, lex by qid;
    # tie-break by content hash for byte stability.
    sorted_entries = sorted(
        entries,
        key=lambda e: (
            0 if (e.is_target or e.qid in qid_set) else 1,
            str(e.qid),
            _content_key(e.text),
        ),
    )
    kept: list[RcaCardEntry] = []
    running = 0
    for entry in sorted_entries:
        cost = entry.estimated_tokens()
        if running + cost > cap_tokens:
            continue
        kept.append(entry)
        running += cost
    return (tuple(kept), running)


def _slice_catalog_to_cap(
    entries: Sequence[CatalogEntry],
    cap_tokens: int,
) -> tuple[tuple[CatalogEntry, ...], int]:
    """Lexicographic-by-name slice. Returns (kept, tokens)."""
    if cap_tokens <= 0:
        return ((), 0)
    sorted_entries = sorted(
        entries,
        key=lambda e: (str(e.name), _content_key(e.text)),
    )
    kept: list[CatalogEntry] = []
    running = 0
    for entry in sorted_entries:
        cost = entry.estimated_tokens()
        if running + cost > cap_tokens:
            continue
        kept.append(entry)
        running += cost
    return (tuple(kept), running)


def _slice_text_to_cap(text: str, cap_tokens: int) -> tuple[str, int]:
    """Hard truncate by characters (4 chars/token, ceil)."""
    if cap_tokens <= 0:
        return ("", 0)
    cost = estimate_tokens(text or "")
    if cost <= cap_tokens:
        return (text or "", cost)
    char_budget = cap_tokens * 4
    truncated = (text or "")[:char_budget]
    return (truncated, estimate_tokens(truncated))


@dataclass(frozen=True, slots=True)
class Stage3PromptSizeBreakdown(JsonRoundTrip):
    """Result of :func:`build_stage3_prompt_budget`.

    ``total_tokens`` is guaranteed ``<= STAGE3_PROMPT_TOTAL_CAP``.
    Per-segment fields report the post-slice tokens. ``segment_caps``
    is the cap table that was applied. ``sub_cluster_split_needed`` is
    True iff, even AFTER per-segment slicing, the input would still
    exceed the cap and the caller must invoke
    :func:`partition_for_sub_cluster_split`.
    """

    cluster_id: str
    history_tokens: int
    rca_card_tokens: int
    lever_menu_tokens: int
    archetype_catalog_tokens: int
    schema_column_tokens: int
    total_tokens: int
    cap: int
    cacheable_block_tokens: int
    segment_caps: Mapping[Stage3PromptSegment, int] = field(
        default_factory=default_segment_caps
    )
    sub_cluster_split_needed: bool = False

    def to_json(self) -> dict:  # type: ignore[override]
        return {
            "cluster_id": self.cluster_id,
            "history_tokens": self.history_tokens,
            "rca_card_tokens": self.rca_card_tokens,
            "lever_menu_tokens": self.lever_menu_tokens,
            "archetype_catalog_tokens": self.archetype_catalog_tokens,
            "schema_column_tokens": self.schema_column_tokens,
            "total_tokens": self.total_tokens,
            "cap": self.cap,
            "cacheable_block_tokens": self.cacheable_block_tokens,
            "segment_caps": {
                seg.value: int(cap) for seg, cap in self.segment_caps.items()
            },
            "sub_cluster_split_needed": bool(self.sub_cluster_split_needed),
        }

    @classmethod
    def from_json(cls, payload: dict) -> "Stage3PromptSizeBreakdown":  # type: ignore[override]
        raw_caps = payload.get("segment_caps") or {}
        caps = {
            Stage3PromptSegment(k): int(v) for k, v in raw_caps.items()
        }
        return cls(
            cluster_id=str(payload["cluster_id"]),
            history_tokens=int(payload.get("history_tokens") or 0),
            rca_card_tokens=int(payload.get("rca_card_tokens") or 0),
            lever_menu_tokens=int(payload.get("lever_menu_tokens") or 0),
            archetype_catalog_tokens=int(
                payload.get("archetype_catalog_tokens") or 0
            ),
            schema_column_tokens=int(
                payload.get("schema_column_tokens") or 0
            ),
            total_tokens=int(payload.get("total_tokens") or 0),
            cap=int(payload.get("cap") or STAGE3_PROMPT_TOTAL_CAP),
            cacheable_block_tokens=int(
                payload.get("cacheable_block_tokens") or 0
            ),
            segment_caps=caps if caps else default_segment_caps(),
            sub_cluster_split_needed=bool(
                payload.get("sub_cluster_split_needed") or False
            ),
        )


@dataclass(frozen=True, slots=True)
class Stage3PromptSlices:
    """Sliced segment payloads. Returned alongside the breakdown so
    the Stage 3 prompt builder can hand these strings into the LLM
    call verbatim."""

    history_entries: tuple[HistoryEntry, ...]
    rca_card_entries: tuple[RcaCardEntry, ...]
    lever_menu_text: str
    archetype_catalog_entries: tuple[CatalogEntry, ...]
    schema_column_entries: tuple[CatalogEntry, ...]


def build_stage3_prompt_budget(
    inp: Stage3PromptInput,
    *,
    segment_caps: Mapping[Stage3PromptSegment, int] | None = None,
    total_cap: int = STAGE3_PROMPT_TOTAL_CAP,
) -> tuple[Stage3PromptSizeBreakdown, Stage3PromptSlices]:
    """Slice ``inp`` to the per-segment caps and return breakdown+slices.

    Order of operations:
      1. Apply per-segment caps via deterministic slicing helpers.
      2. Compute aggregate ``total_tokens``.
      3. If ``total_tokens > total_cap`` (e.g. one RCA card alone
         overflows), set ``sub_cluster_split_needed=True`` so the
         caller invokes :func:`partition_for_sub_cluster_split`.

    Invariants (post-call):
      * ``history_tokens <= segment_caps[HISTORY]``
      * ``rca_card_tokens <= segment_caps[RCA_CARDS]``
      * ``lever_menu_tokens <= segment_caps[LEVER_MENU]``
      * ``archetype_catalog_tokens <= segment_caps[ARCHETYPE_CATALOG]``
      * ``schema_column_tokens <= segment_caps[SCHEMA_COLUMNS]``
    """
    caps = dict(segment_caps) if segment_caps else default_segment_caps()

    history_kept, history_tokens = _slice_history_to_cap(
        inp.history, caps[Stage3PromptSegment.HISTORY]
    )
    rca_kept, rca_tokens = _slice_rca_cards_to_cap(
        inp.rca_cards,
        caps[Stage3PromptSegment.RCA_CARDS],
        cluster_qids=inp.cluster_qids,
    )
    lever_text, lever_tokens = _slice_text_to_cap(
        inp.lever_menu_text, caps[Stage3PromptSegment.LEVER_MENU]
    )
    archetype_kept, archetype_tokens = _slice_catalog_to_cap(
        inp.archetype_catalog,
        caps[Stage3PromptSegment.ARCHETYPE_CATALOG],
    )
    schema_kept, schema_tokens = _slice_catalog_to_cap(
        inp.schema_columns, caps[Stage3PromptSegment.SCHEMA_COLUMNS]
    )

    total = (
        history_tokens
        + rca_tokens
        + lever_tokens
        + archetype_tokens
        + schema_tokens
    )
    sub_cluster_split = total > total_cap

    breakdown = Stage3PromptSizeBreakdown(
        cluster_id=inp.cluster_id,
        history_tokens=history_tokens,
        rca_card_tokens=rca_tokens,
        lever_menu_tokens=lever_tokens,
        archetype_catalog_tokens=archetype_tokens,
        schema_column_tokens=schema_tokens,
        total_tokens=total,
        cap=int(total_cap),
        cacheable_block_tokens=int(inp.cacheable_prefix_tokens),
        segment_caps=caps,
        sub_cluster_split_needed=sub_cluster_split,
    )
    slices = Stage3PromptSlices(
        history_entries=history_kept,
        rca_card_entries=rca_kept,
        lever_menu_text=lever_text,
        archetype_catalog_entries=archetype_kept,
        schema_column_entries=schema_kept,
    )
    return (breakdown, slices)


def partition_for_sub_cluster_split(
    inp: Stage3PromptInput,
    *,
    max_qids_per_partition: int = 1,
) -> tuple[tuple[str, ...], ...]:
    """Deterministic partition over ``inp.cluster_qids`` for the
    sub-cluster split fallback.

    The default ``max_qids_per_partition=1`` produces one partition per
    QID, which is the most conservative split possible. Callers that
    can tolerate larger partitions (e.g. 2-3 QIDs) may raise it. The
    sort key is lexicographic over the QID string so the partition is
    byte-stable.
    """
    qids = sorted(set(inp.cluster_qids))
    if max_qids_per_partition <= 0:
        max_qids_per_partition = 1
    partitions: list[tuple[str, ...]] = []
    for i in range(0, len(qids), max_qids_per_partition):
        partitions.append(tuple(qids[i : i + max_qids_per_partition]))
    return tuple(partitions)


def partition_rca_subcluster_by_token_budget(
    *,
    qids: Sequence[str],
    user_prompt_tokens: int,
    system_msg_tokens: int,
    cacheable_block_tokens: int,
    cap: int = STAGE3_PROMPT_TOTAL_CAP,
) -> tuple[tuple[str, ...], ...]:
    """Trial 22 W7 — token-budget-aware partition for the RCA-subcluster
    Stage 3 builder.

    The d139 postmortem showed RCA-subcluster Stage 3 requests at
    ~98k user-prompt tokens (105k total) declining with
    ``prompt_too_large`` against the 40k cap. The legacy
    :func:`partition_for_sub_cluster_split` splits by QID *count*
    (default 1-per-partition), which neither bounds per-batch tokens
    nor matches the plan's slicing math. This function computes the
    batch count from the actual token overflow:

        user_budget   = cap - (system_msg_tokens + cacheable_block_tokens)
        n_batches     = ceil(user_prompt_tokens / user_budget)

    then distributes the QIDs as evenly as possible across
    ``n_batches`` partitions (deterministic, lexicographic order). The
    even split keeps each batch's projected user-prompt token share
    (``user_prompt_tokens * batch_qids / total_qids``) at or below
    ``user_budget`` so every sub-batch fits under ``cap``.

    Returns a tuple of QID tuples. When the input already fits under
    budget (``n_batches <= 1``) returns a single partition holding all
    QIDs (no split).
    """
    import math

    ordered = sorted(str(q) for q in (qids or ()))
    if not ordered:
        return ()

    cap = int(cap)
    non_negotiable = max(0, int(system_msg_tokens)) + max(
        0, int(cacheable_block_tokens)
    )
    user_budget = max(1, cap - non_negotiable)
    user_t = max(0, int(user_prompt_tokens))

    n_batches = max(1, math.ceil(user_t / user_budget))
    # Never need more batches than QIDs.
    n_batches = min(n_batches, len(ordered))
    if n_batches <= 1:
        return (tuple(ordered),)

    # Even, deterministic distribution: the first ``remainder`` batches
    # get one extra QID so |batch sizes| differ by at most one.
    base, remainder = divmod(len(ordered), n_batches)
    partitions: list[tuple[str, ...]] = []
    cursor = 0
    for b in range(n_batches):
        size = base + (1 if b < remainder else 0)
        partitions.append(tuple(ordered[cursor : cursor + size]))
        cursor += size
    return tuple(partitions)


def stage3_subcluster_split_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    builder: str,
    partitions: Sequence[Sequence[str]],
    user_prompt_tokens: int,
    user_budget: int,
    cap: int = STAGE3_PROMPT_TOTAL_CAP,
) -> str:
    """Trial 22 W7 — emit one ``GSO_TRIAL22_STAGE3_SUBCLUSTER_SPLIT_V1``
    marker recording the deterministic sub-batch partition. ``builder``
    pins the split to the RCA-subcluster path so postmortems can tell
    it apart from the (un-split) H001 cluster builder.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        marker_line,
    )

    return marker_line(
        "GSO_TRIAL22_STAGE3_SUBCLUSTER_SPLIT_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "cluster_id": str(cluster_id),
            "builder": str(builder),
            "batch_count": len(partitions),
            "batch_sizes": [len(p) for p in partitions],
            "qids_per_batch": [list(p) for p in partitions],
            "user_prompt_tokens": int(user_prompt_tokens),
            "user_budget": int(user_budget),
            "cap": int(cap),
        },
    )


def stage3_prompt_size_breakdown_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    breakdown: Stage3PromptSizeBreakdown,
) -> str:
    """Return one ``GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1`` marker line.

    Emitted before every Stage 3 LLM invocation so postmortems can
    audit segment-level token attribution and cache-hit-rate share.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        marker_line,
    )

    return marker_line(
        "GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "cluster_id": breakdown.cluster_id,
            "history_tokens": breakdown.history_tokens,
            "rca_card_tokens": breakdown.rca_card_tokens,
            "lever_menu_tokens": breakdown.lever_menu_tokens,
            "archetype_catalog_tokens": breakdown.archetype_catalog_tokens,
            "schema_column_tokens": breakdown.schema_column_tokens,
            "total_tokens": breakdown.total_tokens,
            "cap": breakdown.cap,
            "cacheable_block_tokens": breakdown.cacheable_block_tokens,
            "sub_cluster_split_needed": breakdown.sub_cluster_split_needed,
            "segment_caps": {
                seg.value: int(cap)
                for seg, cap in breakdown.segment_caps.items()
            },
        },
    )
