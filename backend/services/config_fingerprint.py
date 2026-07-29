"""Canonical fingerprints for Genie Agent ``serialized_space`` configs.

Powers the Auto-Optimize "current version" check: given the live space config
and every config captured by past optimization runs (run baselines in
``genie_opt_runs.config_snapshot``, champion API observations in
``genie_opt_iterations.observed_config_json``), a fingerprint match answers
*"which known optimization version is the live agent on right now?"*. A
non-match proves external drift only when all expected versions have an
authoritative observation; submitted-only legacy history is inconclusive.

Fingerprint contract (all three steps matter equally):

1. **Unwrap** — stored/live configs arrive in three historical shapes (full
   Genie GET-response wrapper, ``_parsed_space`` wrapper, bare parsed
   serialized_space). All reduce to the parsed serialized_space object. The
   pristine ``serialized_space`` payload is preferred over ``_parsed_space``:
   GSO's preflight mutates the latter in place (e.g. injecting
   ``_data_profile``) before persisting the snapshot, so the parsed copy can
   diverge from what the Genie API round-trips. Internal ``_``-prefixed
   top-level keys are stripped regardless of source.
2. **Canonicalize** — the top-level ``benchmarks`` block is dropped (revert
   deliberately preserves the *live* benchmark block, so including it would
   make every revert-to-baseline look like drift); object keys are sorted;
   arrays whose elements all carry a string ``id`` are sorted by id (Genie's
   own validation rules require id-sorted arrays, so this only normalizes
   ordering the API itself does not treat as meaningful).
3. **Hash** — SHA-256 over the compact JSON serialization.

Pure functions only — no I/O, fully unit-testable offline.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

# Recognized as a bare serialized_space object when any of these keys is present.
_SERIALIZED_SPACE_KEYS = ("data_sources", "instructions", "config", "benchmarks")


def unwrap_serialized_space(config: Any) -> dict | None:
    """Reduce any stored/live config shape to the parsed serialized_space object.

    Shapes seen in the wild:

    * full Genie GET response — ``{"serialized_space": "<json string>" | {...}}``;
    * ``{"_parsed_space": {...}}`` — GSO's fetch stashes a parsed copy under
      this key (``common/genie_client.py``);
    * bare serialized_space dict — champion submitted/observed JSON rows and
      already-parsed live configs.

    The pristine ``serialized_space`` payload is preferred over
    ``_parsed_space`` when both exist: preflight mutates the parsed copy in
    place (injecting ``_data_profile``) before persisting the snapshot, so the
    parsed copy can diverge from what the Genie API round-trips. Internal
    ``_``-prefixed top-level keys are stripped from every source, so snapshots
    that only retain the mutated copy still match.

    Legacy projected rows that dropped the required top-level ``version`` get
    the current documented default (mirrors GSO's revert path so a legacy
    snapshot can still match the live config it produced). Returns ``None``
    when no recognizable serialized_space object is present — callers treat
    that as "not matchable", never as drift.
    """
    if not isinstance(config, dict) or not config:
        return None

    serialized = config.get("serialized_space")
    if isinstance(serialized, str) and serialized.strip():
        try:
            serialized = json.loads(serialized)
        except (json.JSONDecodeError, TypeError):
            # Legacy SQL-corrupted snapshots (unescaped nested JSON) — fall
            # through to _parsed_space if present; otherwise not matchable.
            # Fail-open either way: no false badge, no false drift.
            serialized = None
    if isinstance(serialized, dict) and serialized:
        return _strip_internal_keys(_with_default_version(serialized))

    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict) and parsed:
        return _strip_internal_keys(_with_default_version(parsed))

    if any(key in config for key in _SERIALIZED_SPACE_KEYS):
        return _strip_internal_keys(_with_default_version(config))
    return None


def _strip_internal_keys(space: dict) -> dict:
    """Drop internal ``_``-prefixed top-level keys injected by the optimizer.

    Preflight persists snapshots after in-place injections such as
    ``_data_profile`` (and similar bookkeeping fields). Those keys never
    appear in the live Genie config, so hashing them would report drift on an
    unchanged agent. The Genie ``serialized_space`` schema has no
    underscore-prefixed top-level fields, so this is semantics-preserving.
    """
    return {key: value for key, value in space.items() if not key.startswith("_")}


def _with_default_version(config: dict) -> dict:
    """Backfill ``version`` for legacy projected rows (same rule as GSO revert)."""
    if "version" in config:
        return config
    return {"version": 2, **config}


def canonicalize(node: Any, *, _depth: int = 0) -> Any:
    """Recursively canonicalize a parsed serialized_space object.

    * top-level ``benchmarks`` dropped (evaluation state, not configuration —
      see module docstring);
    * dict keys sorted (``json.dumps(sort_keys=True)`` would do this too, but
      explicit sorting keeps the structure inspectable for debugging);
    * arrays whose elements are all dicts with a string ``id`` are sorted by
      that id — Genie's validation rules require id-sorted arrays, so element
      order there carries no meaning;
    * everything else (scalars, string content, non-id array order) is
      preserved exactly — those are meaningful configuration.
    """
    if isinstance(node, dict):
        items = {
            key: canonicalize(value, _depth=_depth + 1)
            for key, value in node.items()
            if not (_depth == 0 and key == "benchmarks")
        }
        return dict(sorted(items.items()))
    if isinstance(node, list):
        items = [canonicalize(value, _depth=_depth + 1) for value in node]
        if items and all(
            isinstance(value, dict) and isinstance(value.get("id"), str)
            for value in items
        ):
            items.sort(key=lambda value: value["id"])
        return items
    return node


def config_fingerprint(config: Any) -> str | None:
    """SHA-256 fingerprint of a stored/live Genie Agent config.

    Accepts any of the shapes handled by :func:`unwrap_serialized_space`
    (including raw JSON strings returned by Delta reads). Returns ``None`` when the value cannot be
    reduced to a serialized_space object.
    """
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError):
            return None
    space = unwrap_serialized_space(config)
    if space is None:
        return None
    canonical = canonicalize(space)
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
