"""Deterministic semantic diffs of comparable canonical observations."""

from typing import Any

from backend.services.config_fingerprint import _VC_COLLECTION_KEYS
from backend.services.version_control.contracts import (
    DiffCategory,
    DiffChange,
    DiffItem,
    Snapshot,
    canonical_json_hash,
    to_wire,
)

_MISSING = object()
_CATEGORIES = {
    "data_sources": DiffCategory.SOURCES,
    "sources": DiffCategory.SOURCES,
    "tables": DiffCategory.SOURCES,
    "metric_views": DiffCategory.SOURCES,
    "column_configs": DiffCategory.COLUMNS,
    "columns": DiffCategory.COLUMNS,
    "text_instructions": DiffCategory.INSTRUCTIONS,
    "join_specs": DiffCategory.JOINS,
    "joins": DiffCategory.JOINS,
    "filters": DiffCategory.FILTERS,
    "parameters": DiffCategory.PARAMETERS,
    "sql_snippets": DiffCategory.SQL,
    "example_question_sqls": DiffCategory.SQL,
    "sql_functions": DiffCategory.SQL,
    "expressions": DiffCategory.SQL,
    "measures": DiffCategory.SQL,
    "sample_questions": DiffCategory.QUESTIONS,
    "questions": DiffCategory.QUESTIONS,
    "benchmark": DiffCategory.BENCHMARKS,
    "metadata": DiffCategory.METADATA,
    "bindings": DiffCategory.BINDINGS,
    "warehouse_id": DiffCategory.BINDINGS,
    "space_id": DiffCategory.BINDINGS,
    "workspace_id": DiffCategory.BINDINGS,
}


def semantic_diff(left: Snapshot, right: Snapshot) -> list[DiffItem]:
    """Return JSON-pointer paths, using stable IDs for addressed collections.

    Incompatible versions raise rather than returning a misleading empty diff.
    Use the Canonicalizer adapter to opt into verified dual computation.
    Unknown fields stay visible and require review instead of being discarded.
    """
    if (
        left.fingerprints.canonicalizer_version != "vc-c14n/1"
        or right.fingerprints.canonicalizer_version != "vc-c14n/1"
    ):
        raise ValueError("Cannot diff incompatible canonicalizer versions")
    items: list[DiffItem] = []
    _walk(
        to_wire(left.canonical_state),
        to_wire(right.canonical_state),
        (),
        None,
        None,
        items,
    )
    return sorted(
        items, key=lambda item: (item.category.value, item.path, item.change.value)
    )


def _category(key: str, current: DiffCategory | None) -> DiffCategory | None:
    if current in (DiffCategory.BENCHMARKS, DiffCategory.METADATA):
        return current
    return _CATEGORIES.get(key, current)


def _index(value: Any, identity: str) -> dict | None:
    if value is _MISSING:
        return {}
    if not isinstance(value, list) or not all(
        isinstance(entry, dict) and isinstance(entry.get(identity), str)
        for entry in value
    ):
        return None
    indexed = {entry[identity]: entry for entry in value}
    return indexed if len(indexed) == len(value) else None


def _walk(
    before: Any,
    after: Any,
    path: tuple[str, ...],
    field: str | None,
    category: DiffCategory | None,
    items: list[DiffItem],
) -> None:
    if (
        before is not _MISSING
        and after is not _MISSING
        and canonical_json_hash("vc-diff-value/1", {"value": before})
        == canonical_json_hash("vc-diff-value/1", {"value": after})
    ):
        return
    if isinstance(before, dict) and isinstance(after, dict):
        for key in sorted(before.keys() | after.keys()):
            _walk(
                before.get(key, _MISSING),
                after.get(key, _MISSING),
                (*path, key),
                key,
                _category(key, category),
                items,
            )
        return
    identity = _VC_COLLECTION_KEYS.get(field)
    if identity:
        left_index, right_index = _index(before, identity), _index(after, identity)
        if left_index is not None and right_index is not None:
            if not left_index and not right_index:
                _emit(before, after, path, category, items)
                return
            for key in sorted(left_index.keys() | right_index.keys()):
                left_value, right_value = (
                    left_index.get(key, _MISSING),
                    right_index.get(key, _MISSING),
                )
                if left_value is _MISSING or right_value is _MISSING:
                    _emit(left_value, right_value, (*path, key), category, items)
                else:
                    _walk(
                        left_value,
                        right_value,
                        (*path, key),
                        None,
                        category,
                        items,
                    )
            return
    _emit(before, after, path, category, items)


def _contains_sql(value: Any) -> bool:
    if isinstance(value, dict):
        return str(value.get("format", "")).upper() == "SQL" or any(
            "sql" in key.lower() or _contains_sql(child)
            for key, child in value.items()
        )
    return isinstance(value, list) and any(_contains_sql(child) for child in value)


def _emit(
    before: Any,
    after: Any,
    path: tuple[str, ...],
    category: DiffCategory | None,
    items: list[DiffItem],
) -> None:
    change = (
        DiffChange.ADDED
        if before is _MISSING
        else DiffChange.REMOVED
        if after is _MISSING
        else DiffChange.MODIFIED
    )
    review = (
        category in (DiffCategory.SQL, DiffCategory.JOINS, DiffCategory.FILTERS)
        or any("sql" in key.lower() for key in path)
        or _contains_sql(before)
        or _contains_sql(after)
        or category is None
    )
    items.append(
        DiffItem(
            category=category or DiffCategory.INSTRUCTIONS,
            path="/"
            + "/".join(key.replace("~", "~0").replace("/", "~1") for key in path),
            change=change,
            before=None if before is _MISSING else before,
            after=None if after is _MISSING else after,
            review_required=review,
        )
    )
