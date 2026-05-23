"""Shared test helpers for JSON-Schema response_format assertions.

The pre-PR-C envelope tests asserted "no forbidden keyword leaked" by
doing a substring check on ``repr(fmt)``. That check is wrong: it also
matches the literal word ``pattern`` (or ``anyOf``, ``$ref``, ...) when
it appears inside a property's ``description`` text.

Pre-PR-C the bug was masked because ``_strip_unsupported`` was so
aggressive it stripped descriptions along with everything else, leaving
nothing for the substring to match against. After PR-C correctly
preserves the typed branches (and therefore their descriptions), the
substring check fires false positives on natural-language phrases like
"failure pattern".

Use ``schema_dict_keys`` instead — it walks the actual schema dict and
collects only dict keys, so prose containing forbidden words doesn't
trip the check.
"""
from __future__ import annotations

from typing import Any, Iterable


def schema_dict_keys(node: Any) -> set[str]:
    """Return the set of dict keys that appear anywhere in ``node``.

    Walks dicts and lists recursively. Used to check that a built
    response_format does not contain Databricks-unsupported JSON Schema
    keywords (``anyOf``, ``oneOf``, ``$ref``, ``pattern``, ...) — but
    only as STRUCTURAL keys, never as content inside a string value.
    """
    found: set[str] = set()
    if isinstance(node, dict):
        for k, v in node.items():
            found.add(k)
            found.update(schema_dict_keys(v))
    elif isinstance(node, list):
        for item in node:
            found.update(schema_dict_keys(item))
    return found


def assert_no_forbidden_schema_keys(
    response_format: dict,
    forbidden: Iterable[str] = (
        "anyOf", "oneOf", "allOf", "$ref", "pattern", "$defs",
        "prefixItems",
    ),
) -> None:
    """Fail if the built response_format contains any Databricks-
    unsupported JSON Schema keyword as a dict key.

    This is the post-PR-C replacement for the fragile
    ``forbidden not in repr(fmt)`` substring check.
    """
    keys = schema_dict_keys(response_format)
    leaked = keys & set(forbidden)
    assert not leaked, (
        f"Databricks-unsupported JSON Schema keywords leaked into "
        f"response_format: {sorted(leaked)}"
    )
