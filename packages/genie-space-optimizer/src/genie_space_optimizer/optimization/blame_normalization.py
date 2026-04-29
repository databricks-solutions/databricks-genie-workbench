"""Canonical blame-set normalization for RCA and lever-loop control."""

from __future__ import annotations

import json
import re
from typing import Any


_BRACKET_LIST_RE = re.compile(r"^\[\s*([^,\[\]]+?)\s*\]$")


def _flatten_blame(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            out.extend(_flatten_blame(item))
        return out
    text = str(value).strip()
    if not text or text == "[]":
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            return _flatten_blame(parsed)
        match = _BRACKET_LIST_RE.match(text)
        if match:
            inner = match.group(1).strip().strip('"').strip("'").strip()
            return [inner] if inner else []
    return [text.strip('"').strip("'").strip()]


def normalize_blame_set(value: Any) -> tuple[str, ...]:
    """Return a flat, deduplicated tuple of non-empty blame tokens."""
    tokens: list[str] = []
    for token in _flatten_blame(value):
        cleaned = str(token).strip()
        if cleaned and cleaned != "[]":
            tokens.append(cleaned)
    return tuple(dict.fromkeys(tokens))


def normalize_blame_key(value: Any) -> tuple[str, ...] | str:
    """Return the reflection/tried-cluster key shape for a blame set.

    The reflection identity contract sorts tokens so identity keys stay
    stable across runs even when blame_set ordering differs.
    """
    normalized = normalize_blame_set(value)
    if not normalized:
        return ""
    return tuple(sorted(normalized))
