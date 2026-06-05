"""Phase 0 P0.5 — Anthropic-style prompt-caching helpers.

The Databricks Foundation Model API for Claude Opus 4.6 supports
Anthropic-style prompt caching via ``cache_control`` markers on
individual content blocks. The first call sending a block with
``cache_control={"type": "ephemeral"}`` warms the cache (paid at
1.25x input); subsequent calls within the cache TTL that send the
same prefix pay 0.1x of the input cost for those tokens.

The optimizer's reasoning calls have heavily repeated static content
that is perfect for caching:

  * **Stage 1 / Stage 3 system_msg** — the SKILL.md body, identical
    every call.
  * **Stage 3 lever_menu** — the closed-vocabulary lever-1..lever-6
    catalog, ~5-10k tokens, identical every iteration.
  * **Stage 3 archetype_catalog_menu** — ~3-5k tokens, identical
    every iteration.
  * **Stage 3 lever_contract_instructions** — ~2-3k tokens, identical
    every iteration when Trial 20 multi-lever defaults are on.
  * **Stage 1 / Stage 3 schema_columns** — stable per Genie Space
    for the lifetime of one optimization run.

This module provides a single helper, :func:`build_cached_messages`,
that translates a ``(system_text, cacheable_blocks, dynamic_text)``
tuple into the OpenAI-compatible messages payload with cache_control
markers on the static blocks.

Anthropic's API supports up to 4 ``cache_control`` breakpoints per
call. We use:

  1. system (one marker on the final block, marks ``system_msg``)
  2. user block 1 — first cacheable user block (e.g. ``lever_menu``)
  3. user block 2 — second cacheable user block
  4. user block 3 — third cacheable user block

The dynamic part is sent as the FINAL user block WITHOUT a
cache_control marker (so the cache lookup stops before the dynamic
text influences the key). Reading order:
``[cached_block_1, cached_block_2, cached_block_3, dynamic_block]``.

If a caller passes more than 3 cacheable blocks, this helper
concatenates them in order until it has 3 entries, then merges any
overflow into the dynamic block (the cache breakpoint stops before
the first un-cached block).
"""
from __future__ import annotations

import os
from typing import Any


_MAX_CACHEABLE_USER_BLOCKS = 3  # Anthropic API budget; one extra goes on system.


def cache_control_enabled() -> bool:
    """Read the rollback flag. Default-ON; set
    ``GSO_PROMPT_CACHE=0`` to disable cache_control emission (the
    payload then degrades cleanly to plain string content arrays)."""
    raw = os.getenv("GSO_PROMPT_CACHE", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def build_cached_messages(
    *,
    system_text: str,
    cacheable_user_blocks: tuple[str, ...] = (),
    dynamic_user_text: str,
    cache_control: bool | None = None,
) -> list[dict[str, Any]]:
    """Construct an OpenAI-compatible messages list with optional
    Anthropic cache_control markers on the static prefix.

    Returns
    -------
    list of dict
        Suitable for ``client.chat.completions.create(messages=...)``
        against a Databricks Claude Opus 4.6 endpoint.

    Notes
    -----
    * When ``cache_control`` is ``False`` (or the env flag rolls
      caching back), the function still emits multi-block content
      arrays but WITHOUT the marker — this keeps the message shape
      stable so postmortem markers can read ``len(messages)`` /
      ``len(content)`` without conditional code.
    * Block ordering is part of the cache key; do NOT reorder
      ``cacheable_user_blocks`` between calls expected to hit the
      same cache.
    * Extra blocks beyond ``_MAX_CACHEABLE_USER_BLOCKS`` are merged
      into the dynamic block — the cache prefix stops before them
      so they don't break the cache lookup.
    """
    use_cache = (
        cache_control_enabled() if cache_control is None else bool(cache_control)
    )

    messages: list[dict[str, Any]] = []

    # System message — single content block, optionally with
    # cache_control. Claude's API rejects cache_control on a plain
    # string content, so we always emit a list-of-objects shape
    # when caching is on.
    if system_text:
        if use_cache:
            messages.append({
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": system_text,
                        "cache_control": {"type": "ephemeral"},
                    },
                ],
            })
        else:
            messages.append({
                "role": "system",
                "content": system_text,
            })

    # User message — multi-block content with cache_control on each
    # static block up to the Anthropic budget. Overflow blocks are
    # merged into the dynamic block.
    cacheable_list = list(cacheable_user_blocks or ())
    overflow_blocks: list[str] = []
    if len(cacheable_list) > _MAX_CACHEABLE_USER_BLOCKS:
        overflow_blocks = cacheable_list[_MAX_CACHEABLE_USER_BLOCKS:]
        cacheable_list = cacheable_list[:_MAX_CACHEABLE_USER_BLOCKS]

    user_blocks: list[dict[str, Any]] = []
    for block in cacheable_list:
        if not block:
            continue
        entry: dict[str, Any] = {"type": "text", "text": block}
        if use_cache:
            entry["cache_control"] = {"type": "ephemeral"}
        user_blocks.append(entry)

    # Dynamic block — overflow + caller's dynamic text. Always last,
    # never marked with cache_control.
    dynamic_combined = "\n\n".join(
        s for s in (*overflow_blocks, dynamic_user_text) if s
    )
    if dynamic_combined:
        user_blocks.append({"type": "text", "text": dynamic_combined})

    if user_blocks:
        # If caching is OFF and there is exactly one block, fall back
        # to the legacy plain-string content shape so the request is
        # byte-stable with pre-P0.5 callers.
        if not use_cache and len(user_blocks) == 1:
            messages.append({
                "role": "user",
                "content": user_blocks[0]["text"],
            })
        else:
            messages.append({"role": "user", "content": user_blocks})

    return messages
