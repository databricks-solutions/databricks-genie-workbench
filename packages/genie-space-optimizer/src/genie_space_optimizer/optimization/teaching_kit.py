"""Teaching-kit normalization for RCA-driven Example SQL synthesis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SUPPORTED_SUPPORT_PATCH_TYPES = frozenset({
    "add_instruction",
    "add_column_synonym",
    "add_sql_snippet_measure",
    "add_sql_snippet_filter",
    "add_sql_snippet_expression",
})

SQL_SNIPPET_PATCH_TYPES = frozenset({
    "add_sql_snippet_measure",
    "add_sql_snippet_filter",
    "add_sql_snippet_expression",
})


@dataclass(frozen=True)
class TeachingKit:
    primary: dict[str, Any]
    supporting: list[dict[str, Any]]


def _clean_str(value: Any, *, limit: int = 4000) -> str:
    text = str(value or "").strip()
    return text[:limit]


def _list_str(value: Any, *, limit: int = 12) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                out.append(text[:200])
        return list(dict.fromkeys(out))[:limit]
    text = str(value).strip()
    return [text] if text else []


def _primary_from_raw(raw: dict) -> dict[str, Any]:
    nested = raw.get("example_sql")
    if isinstance(nested, dict):
        src = nested
    else:
        src = raw

    question = _clean_str(
        src.get("example_question") or src.get("question"),
        limit=500,
    )
    sql = _clean_str(
        src.get("example_sql") or src.get("sql"),
        limit=8000,
    )
    if not question or not sql:
        return {}
    parameters = src.get("parameters", [])
    return {
        "patch_type": "add_example_sql",
        "example_question": question,
        "example_sql": sql,
        "parameters": parameters if isinstance(parameters, list) else [],
        "usage_guidance": _clean_str(src.get("usage_guidance") or src.get("rationale"), limit=1000),
        "rationale": _clean_str(raw.get("kit_summary") or src.get("rationale"), limit=1000),
    }


def _normalize_supporting_change(change: dict[str, Any]) -> dict[str, Any] | None:
    patch_type = _clean_str(change.get("patch_type"), limit=80)
    if patch_type not in SUPPORTED_SUPPORT_PATCH_TYPES:
        return None

    if patch_type == "add_instruction":
        text = _clean_str(
            change.get("new_text") or change.get("text") or change.get("proposed_value"),
            limit=2000,
        )
        if not text:
            return None
        return {
            "patch_type": "add_instruction",
            "section_name": _clean_str(change.get("section_name") or "QUERY CONSTRUCTION", limit=120),
            "new_text": text,
            "proposed_value": text,
            "rationale": _clean_str(change.get("rationale"), limit=1000),
        }

    if patch_type == "add_column_synonym":
        table = _clean_str(
            change.get("table") or change.get("table_id") or change.get("target_table"),
            limit=500,
        )
        column = _clean_str(change.get("column") or change.get("column_name"), limit=200)
        synonyms = _list_str(change.get("synonyms"), limit=8)
        if not table or not column or not synonyms:
            return None
        return {
            "patch_type": "add_column_synonym",
            "table": table,
            "table_id": table,
            "column": column,
            "column_name": column,
            "synonyms": synonyms,
            "rationale": _clean_str(change.get("rationale"), limit=1000),
        }

    if patch_type in SQL_SNIPPET_PATCH_TYPES:
        snippet_type = patch_type.replace("add_sql_snippet_", "")
        sql = _clean_str(change.get("sql"), limit=4000)
        display_name = _clean_str(change.get("display_name") or change.get("name"), limit=200)
        if not sql or not display_name:
            return None
        target_table = _clean_str(change.get("target_table") or change.get("table"), limit=500)
        return {
            "patch_type": patch_type,
            "lever": 6,
            "snippet_type": snippet_type,
            "display_name": display_name,
            "alias": _clean_str(change.get("alias"), limit=120),
            "sql": sql,
            "synonyms": _list_str(change.get("synonyms"), limit=8),
            "instruction": _clean_str(change.get("instruction"), limit=1000),
            "target_table": target_table,
            "rationale": _clean_str(change.get("rationale"), limit=1000),
        }

    return None


def normalize_teaching_kit(
    raw: dict[str, Any],
    *,
    kit_id: str,
    target_qids: list[str],
    rca_id: str = "",
) -> TeachingKit:
    """Normalize raw LLM teaching-kit JSON into a primary + support proposal pair.

    The returned ``TeachingKit.primary`` is always either an empty dict or a
    ``add_example_sql`` proposal carrying ``kit_id``, ``target_qids``, and
    ``rca_id``. ``TeachingKit.supporting`` is a (possibly empty) list of
    additive instruction, synonym, or SQL snippet proposals — every other
    patch type is rejected so unsupported shapes cannot leak through.
    """
    if not isinstance(raw, dict):
        return TeachingKit(primary={}, supporting=[])

    target_qids = [str(q) for q in target_qids if str(q)]
    primary = _primary_from_raw(raw)
    if primary:
        primary.update({
            "kit_id": kit_id,
            "target_qids": target_qids,
            "rca_id": rca_id,
            "source": "rca_teaching_kit",
        })

    supporting: list[dict[str, Any]] = []
    changes = raw.get("supporting_changes") or []
    if isinstance(changes, dict):
        changes = [changes]
    if not isinstance(changes, list):
        changes = []
    for change in changes:
        if not isinstance(change, dict):
            continue
        normalized = _normalize_supporting_change(change)
        if normalized is None:
            continue
        normalized.update({
            "kit_id": kit_id,
            "target_qids": target_qids,
            "rca_id": rca_id,
            "source": "rca_teaching_kit",
        })
        supporting.append(normalized)

    return TeachingKit(primary=primary, supporting=supporting)
