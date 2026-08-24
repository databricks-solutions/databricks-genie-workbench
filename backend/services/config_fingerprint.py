"""Canonical fingerprints for Genie Agent config and benchmark state.

Powers the Auto-Optimize "current version" check. Configuration and benchmarks
are fingerprinted independently because a scoped revert can intentionally mix
the config from one historical version with the benchmarks from another. A
component non-match proves external drift only when all expected versions have
an authoritative observation; submitted-only legacy history is inconclusive.

Fingerprint contract (all three steps matter equally):

1. **Unwrap** — stored/live configs arrive in three historical shapes (full
   Genie GET-response wrapper, ``_parsed_space`` wrapper, bare parsed
   serialized_space). All reduce to the parsed serialized_space object. The
   pristine ``serialized_space`` payload is preferred over ``_parsed_space``:
   GSO's preflight mutates the latter in place (e.g. injecting
   ``_data_profile``) before persisting the snapshot, so the parsed copy can
   diverge from what the Genie API round-trips. Internal ``_``-prefixed
   top-level keys are stripped regardless of source.
2. **Canonicalize** — configuration hashing drops the top-level ``benchmarks``
   block, while benchmark hashing isolates that block and treats an omitted
   block as an empty question set. ``content`` and ``sql`` fragments are
   concatenated; boundary quotes added by Genie are ignored for configuration
   hashing but preserved for benchmark ground-truth SQL; object keys are
   sorted; arrays whose elements all carry a string ``id`` are sorted by id
   (Genie's validation rules require id-sorted arrays, so this only normalizes
   ordering the API itself does not treat as meaningful).
3. **Hash** — SHA-256 over the compact JSON serialization.

Pure functions only — no I/O, fully unit-testable offline.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

# Recognized as a bare serialized_space object when any of these keys is present.
_SERIALIZED_SPACE_KEYS = ("data_sources", "instructions", "config", "benchmarks")

# Genie represents these logical strings as ``list[str]`` fragments and may
# split the same value at different boundaries on subsequent GETs. Their
# contract is concatenation, unlike fields such as ``synonyms`` and
# ``default_value.values`` where each array element is independently
# meaningful.
_FRAGMENT_ARRAY_KEYS = frozenset({"content", "sql"})

# Genie also canonicalizes unquoted literals/phrases by adding boundary single
# quotes (for example ``Segment = Enterprise`` -> ``Segment = 'Enterprise'``
# and ``Highest churn risk`` -> ``'Highest churn risk'``). Ignore only quotes
# that are not between two alphanumeric characters. Apostrophes inside words
# such as ``customer's`` and ``O'Reilly`` remain significant.
_BOUNDARY_SINGLE_QUOTE_RE = re.compile(r"(?<![A-Za-z0-9])'|'(?![A-Za-z0-9])")


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


def _normalize_representation_string(value: str) -> str:
    """Remove quote punctuation Genie may add during API normalization."""
    return _BOUNDARY_SINGLE_QUOTE_RE.sub("", value)


def canonicalize(
    node: Any,
    *,
    normalize_boundary_quotes: bool = True,
    _depth: int = 0,
    _parent_key: str | None = None,
) -> Any:
    """Recursively canonicalize a parsed serialized_space object.

    * top-level ``benchmarks`` dropped (evaluation state, not configuration —
      see module docstring);
    * dict keys sorted (``json.dumps(sort_keys=True)`` would do this too, but
      explicit sorting keeps the structure inspectable for debugging);
    * ``content`` / ``sql`` fragment arrays are concatenated;
    * Genie-added boundary single quotes are optionally ignored while
      apostrophes inside words remain significant; benchmark ground-truth SQL
      disables this because SQL quote punctuation changes semantics;
    * arrays whose elements are all dicts with a string ``id`` are sorted by
      that id — Genie's validation rules require id-sorted arrays, so element
      order there carries no meaning;
    * everything else (scalars and non-id array order) is preserved — those
      are meaningful configuration.
    """
    if isinstance(node, dict):
        items = {
            key: canonicalize(
                value,
                normalize_boundary_quotes=normalize_boundary_quotes,
                _depth=_depth + 1,
                _parent_key=key,
            )
            for key, value in node.items()
            if not (_depth == 0 and key == "benchmarks")
        }
        return dict(sorted(items.items()))
    if isinstance(node, list):
        if (
            node
            and _parent_key in _FRAGMENT_ARRAY_KEYS
            and all(isinstance(value, str) for value in node)
        ):
            joined = "".join(node)
            return (
                _normalize_representation_string(joined)
                if normalize_boundary_quotes
                else joined
            )
        items = [
            canonicalize(
                value,
                normalize_boundary_quotes=normalize_boundary_quotes,
                _depth=_depth + 1,
                _parent_key=_parent_key,
            )
            for value in node
        ]
        if items and all(
            isinstance(value, dict) and isinstance(value.get("id"), str)
            for value in items
        ):
            items.sort(key=lambda value: value["id"])
        return items
    if isinstance(node, str):
        return (
            _normalize_representation_string(node)
            if normalize_boundary_quotes
            else node
        )
    return node


def config_fingerprint(config: Any) -> str | None:
    """SHA-256 fingerprint of a stored/live Genie Agent config.

    Accepts any of the shapes handled by :func:`unwrap_serialized_space`
    (including raw JSON strings returned by Delta reads). Returns ``None``
    when the value cannot be reduced to a serialized_space object.
    """
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError):
            return None
    space = unwrap_serialized_space(config)
    if space is None:
        return None
    return _hash_canonical(canonicalize(space))


def benchmark_fingerprint(config: Any) -> str | None:
    """SHA-256 fingerprint of a stored/live Genie benchmark block.

    The official ``serialized_space`` schema makes ``benchmarks`` optional, so
    an omitted block, an empty block, and ``questions=[]`` all represent the
    same empty benchmark state. Malformed benchmark shapes are not matchable and
    return ``None`` so callers fail open instead of reporting false drift.
    """
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (json.JSONDecodeError, TypeError):
            return None
    space = unwrap_serialized_space(config)
    if space is None:
        return None

    benchmarks = space.get("benchmarks")
    if benchmarks is None:
        benchmarks = {}
    if not isinstance(benchmarks, dict):
        return None
    questions = benchmarks.get("questions")
    if questions is None:
        questions = []
    if not isinstance(questions, list):
        return None

    canonical = canonicalize(
        {**benchmarks, "questions": questions},
        normalize_boundary_quotes=False,
    )
    return _hash_canonical(canonical)


def _hash_canonical(canonical: Any) -> str:
    """Hash one already-canonicalized config component."""
    payload = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
