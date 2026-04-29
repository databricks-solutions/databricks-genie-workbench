"""Canonical accessors for MLflow/benchmark eval row dictionaries.

The optimizer sees rows in several shapes:

* MLflow slash keys: ``inputs/question_id``, ``outputs/response``
* dotted keys: ``inputs.question_id``, ``outputs.predictions.sql``
* nested dicts: ``{"inputs": {"question_id": ...}}``
* request/response payloads, sometimes as JSON strings

All control-plane stages must use this module instead of local row parsers.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable, Iterator
from typing import Any

logger = logging.getLogger(__name__)

IDENT_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")
DOTTED_IDENT_RE = re.compile(
    r"[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+"
)


def normalize_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _json_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip().startswith("{"):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def nested_get(row: dict, *paths: str, default: Any = "") -> Any:
    for path in paths:
        if path in row and row.get(path) not in (None, ""):
            return row.get(path)
        cur: Any = row
        ok = True
        for part in path.replace("/", ".").split("."):
            if not isinstance(cur, dict) or part not in cur:
                ok = False
                break
            cur = cur.get(part)
        if ok and cur not in (None, ""):
            return cur
    return default


def request_kwargs(row: dict) -> dict:
    request = _json_dict(row.get("request"))
    kwargs = request.get("kwargs")
    if isinstance(kwargs, dict):
        return kwargs
    return request if isinstance(request, dict) else {}


def response_payload(row: dict) -> dict:
    return _json_dict(row.get("response"))


def row_qid(row: dict) -> str:
    kwargs = request_kwargs(row)
    return str(
        nested_get(
            row,
            "inputs/question_id",
            "inputs.question_id",
            "inputs/id",
            "inputs.id",
            "question_id",
            "qid",
            "id",
            default="",
        )
        or kwargs.get("question_id")
        or kwargs.get("id")
        or ""
    ).strip()


def row_question(row: dict) -> str:
    kwargs = request_kwargs(row)
    return str(
        nested_get(
            row,
            "inputs/question",
            "inputs.question",
            "question",
            default="",
        )
        or kwargs.get("question")
        or ""
    ).strip()


def row_expected_sql(row: dict) -> str:
    kwargs = request_kwargs(row)
    return str(
        kwargs.get("expected_sql")
        or nested_get(
            row,
            "inputs/expected_sql",
            "inputs.expected_sql",
            "inputs/expected_response",
            "inputs.expected_response",
            "expectations/expected_response",
            "expected_sql",
            "expected_response/value",
            "expected_response",
            default="",
        )
        or ""
    ).strip()


def row_generated_sql(row: dict) -> str:
    response = response_payload(row)
    return str(
        nested_get(
            row,
            "outputs/response",
            "outputs.response",
            "outputs/predictions/sql",
            "outputs.predictions.sql",
            "outputs/predictions/query",
            "outputs.predictions.query",
            "generated_sql",
            "genie_sql",
            default="",
        )
        or response.get("response")
        or response.get("sql")
        or response.get("query")
        or ""
    ).strip()


def row_response_text(row: dict) -> str:
    response = response_payload(row)
    return str(
        nested_get(
            row,
            "outputs/predictions/response_text",
            "outputs.predictions.response_text",
            "nl_response",
            default="",
        )
        or response.get("response_text")
        or response.get("text")
        or ""
    ).strip()


def iter_text_values(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        if value.strip():
            yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from iter_text_values(child)
    elif isinstance(value, Iterable):
        for child in value:
            yield from iter_text_values(child)


def token_terms(value: Any) -> set[str]:
    terms: set[str] = set()
    for text in iter_text_values(value):
        normalized = normalize_token(text)
        if normalized:
            terms.add(normalized)
        for token in IDENT_RE.findall(text):
            terms.add(normalize_token(token))
        for dotted in DOTTED_IDENT_RE.findall(text):
            dotted_norm = normalize_token(dotted)
            terms.add(dotted_norm)
            terms.update(part for part in dotted_norm.split(".") if part)
    return {term for term in terms if term}


def iter_asi_metadata(row: dict) -> Iterator[tuple[str, dict]]:
    for key, value in (row or {}).items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        if key.endswith("/metadata"):
            yield key.rsplit("/", 1)[0].replace("feedback/", ""), value
        elif key.endswith(".metadata"):
            yield key.rsplit(".", 1)[0].replace("feedback.", ""), value


def _sql_surface(sql: str) -> set[str]:
    surface: set[str] = set()
    if not sql:
        return surface
    try:
        import sqlglot
        from sqlglot import exp as sql_exp
    except Exception:
        sqlglot = None  # type: ignore[assignment]
        sql_exp = None  # type: ignore[assignment]

    if sqlglot is not None:
        try:
            parsed = sqlglot.parse_one(sql, read="databricks")
            if parsed is not None:
                for col in parsed.find_all(sql_exp.Column):
                    if getattr(col, "name", None):
                        surface.add(normalize_token(col.name))
                for table in parsed.find_all(sql_exp.Table):
                    if getattr(table, "name", None):
                        surface.add(normalize_token(table.name))
                for fn in parsed.find_all(sql_exp.Func):
                    try:
                        name = fn.sql_name()
                    except Exception:
                        name = ""
                    if name:
                        surface.add(normalize_token(name))
        except Exception:
            logger.debug("sqlglot parse failed; using regex fallback", exc_info=True)

    surface |= token_terms(sql)
    return surface


ASI_SURFACE_KEYS: tuple[str, ...] = (
    "failure_type",
    "wrong_clause",
    "blame_set",
    "counterfactual_fix",
    "expected_objects",
    "actual_objects",
    "rca_kind",
    "patch_family",
)


def asi_metadata_surface(row: dict, *, ignored_judges: set[str] | frozenset[str] = frozenset()) -> set[str]:
    surface: set[str] = set()
    for judge, metadata in iter_asi_metadata(row):
        if judge in ignored_judges:
            continue
        for key in ASI_SURFACE_KEYS:
            surface |= token_terms(metadata.get(key))
    return surface


def extract_failure_surface(row: dict, *, ignored_judges: set[str] | frozenset[str] = frozenset()) -> set[str]:
    surface: set[str] = set()
    surface |= _sql_surface(row_expected_sql(row))
    surface |= _sql_surface(row_generated_sql(row))
    surface |= token_terms(row_question(row))
    surface |= token_terms(row_response_text(row))
    surface |= asi_metadata_surface(row, ignored_judges=ignored_judges)
    return surface


def rows_by_qid(rows: Iterable[dict]) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        qid = row_qid(row)
        if qid:
            indexed[qid] = row
    return indexed


def rows_for_qids(rows: Iterable[dict], qids: Iterable[str]) -> list[dict]:
    indexed = rows_by_qid(rows)
    out: list[dict] = []
    for qid in qids or []:
        row = indexed.get(str(qid))
        if row is not None:
            out.append(row)
    return out
