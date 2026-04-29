"""Safe RCA failure context for Example SQL teaching-kit synthesis.

This module exposes Genie's generated SQL and judge feedback to synthesis.
It intentionally excludes benchmark question text and benchmark expected SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from genie_space_optimizer.common.config import IGNORED_OPTIMIZATION_JUDGES
from genie_space_optimizer.optimization.eval_row_access import (
    iter_asi_metadata,
    row_generated_sql,
    row_qid,
)


_QUESTION_KEYS = frozenset({
    "question",
    "inputs.question",
    "inputs/question",
    "request.question",
    "request/question",
})

_EXPECTED_SQL_KEYS = frozenset({
    "expected_sql",
    "inputs.expected_sql",
    "inputs/expected_sql",
    "expected_response",
    "inputs.expected_response",
    "inputs/expected_response",
})

_QID_KEYS = (
    "question_id",
    "inputs.question_id",
    "inputs/question_id",
    "request.question_id",
    "request/question_id",
    "benchmark_id",
    "id",
)

_GENERATED_SQL_KEYS = (
    "outputs.predictions.sql",
    "outputs/predictions/sql",
    "outputs.response",
    "outputs/response",
    "generated_sql",
    "genie_sql",
    "prediction_sql",
)

_JUDGE_METADATA_KEYS = (
    "schema_accuracy/metadata",
    "answer_correctness/metadata",
    "asset_routing/metadata",
    "response_quality/metadata",
    "feedback/schema_accuracy/metadata",
    "feedback/answer_correctness/metadata",
    "feedback/asset_routing/metadata",
    "feedback/response_quality/metadata",
)


@dataclass(frozen=True)
class RcaFailureContext:
    question_id: str
    generated_sql: str = ""
    root_cause: str = "unknown"
    failed_judges: tuple[str, ...] = ()
    blame_set: tuple[str, ...] = ()
    counterfactual_fixes: tuple[str, ...] = ()
    rationales: tuple[str, ...] = ()
    arbiter_verdict: str = ""

    def as_prompt_dict(self) -> dict[str, Any]:
        return {
            "question_id": self.question_id,
            "generated_sql": self.generated_sql,
            "root_cause": self.root_cause,
            "failed_judges": list(self.failed_judges),
            "blame_set": list(self.blame_set),
            "counterfactual_fixes": list(self.counterfactual_fixes),
            "rationales": list(self.rationales),
            "arbiter_verdict": self.arbiter_verdict,
        }


def _row_value(row: dict, *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is None and "/" in key:
            value = row.get(key.replace("/", "."))
        if value is None and "." in key:
            value = row.get(key.replace(".", "/"))
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _as_tuple(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw.strip(),) if raw.strip() else ()
    if isinstance(raw, dict):
        out: list[str] = []
        for key in ("identifier", "name", "column", "table", "fqn", "value"):
            value = raw.get(key)
            if value:
                out.append(str(value).strip())
        return tuple(dict.fromkeys(x for x in out if x))
    if isinstance(raw, Iterable):
        out = []
        for item in raw:
            out.extend(_as_tuple(item))
        return tuple(dict.fromkeys(x for x in out if x))
    text = str(raw).strip()
    return (text,) if text else ()


def _metadata_blocks(row: dict) -> list[tuple[str, dict]]:
    """Return ``[(judge_name, metadata_dict), ...]`` excluding ignored judges.

    ``response_quality`` and any other judge in
    :data:`IGNORED_OPTIMIZATION_JUDGES` are filtered out so they cannot drive
    teaching-kit failure context (and therefore optimizer mutations).
    """
    ignored = {str(j).lower() for j in IGNORED_OPTIMIZATION_JUDGES}
    return [
        (judge, metadata)
        for judge, metadata in iter_asi_metadata(row)
        if str(judge).lower() not in ignored
    ]


def _failed_judges(blocks: list[tuple[str, dict]]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(judge for judge, _metadata in blocks))


def _arbiter_verdict(row: dict) -> str:
    return _row_value(
        row,
        "feedback/arbiter/value",
        "feedback.arbiter.value",
        "arbiter/value",
        "arbiter.value",
        "arbiter",
    ).lower()


def _metadata_texts(
    blocks: list[tuple[str, dict]], *keys: str,
) -> tuple[str, ...]:
    out: list[str] = []
    for _judge, block in blocks:
        for key in keys:
            value = block.get(key)
            if isinstance(value, str) and value.strip():
                out.append(value.strip()[:500])
            elif isinstance(value, list):
                out.extend(
                    str(v).strip()[:500] for v in value if str(v).strip()
                )
    return tuple(dict.fromkeys(out))


def _root_cause(blocks: list[tuple[str, dict]]) -> str:
    for _judge, block in blocks:
        value = block.get("failure_type") or block.get("root_cause")
        if value:
            return str(value).strip()
    return "unknown"


def _blame_set(blocks: list[tuple[str, dict]]) -> tuple[str, ...]:
    from genie_space_optimizer.optimization.blame_normalization import (
        normalize_blame_set,
    )

    out: list[str] = []
    for _judge, block in blocks:
        out.extend(normalize_blame_set(block.get("blame_set")))
    return tuple(dict.fromkeys(x for x in out if x))


def failure_context_from_row(row: dict) -> RcaFailureContext | None:
    if not isinstance(row, dict):
        return None
    qid = row_qid(row)
    generated_sql = row_generated_sql(row)
    blocks = _metadata_blocks(row)
    if not qid or not generated_sql:
        return None

    ctx = RcaFailureContext(
        question_id=qid,
        generated_sql=generated_sql[:4000],
        root_cause=_root_cause(blocks),
        failed_judges=_failed_judges(blocks),
        blame_set=_blame_set(blocks),
        counterfactual_fixes=_metadata_texts(blocks, "counterfactual_fix", "counterfactual_fixes"),
        rationales=_metadata_texts(blocks, "rationale", "rationale_snippet", "reason", "explanation"),
        arbiter_verdict=_arbiter_verdict(row),
    )

    rendered = str(ctx.as_prompt_dict())
    for forbidden_key in _QUESTION_KEYS | _EXPECTED_SQL_KEYS:
        forbidden_value = str(row.get(forbidden_key) or "").strip()
        if forbidden_value and forbidden_value in rendered:
            raise ValueError(
                f"RCA failure context leaked forbidden row field {forbidden_key}"
            )
    return ctx


def failure_contexts_by_qid(rows: Iterable[dict]) -> dict[str, list[dict]]:
    indexed: dict[str, list[dict]] = {}
    for row in rows or []:
        ctx = failure_context_from_row(row)
        if ctx is None:
            continue
        indexed.setdefault(ctx.question_id, []).append(ctx.as_prompt_dict())
    return indexed


def contexts_for_target_qids(
    contexts_by_qid: dict[str, list[dict]],
    target_qids: Iterable[str],
) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for qid_raw in target_qids or []:
        qid = str(qid_raw).strip()
        if not qid or qid in seen:
            continue
        seen.add(qid)
        out.extend(contexts_by_qid.get(qid, [])[:1])
    return out
