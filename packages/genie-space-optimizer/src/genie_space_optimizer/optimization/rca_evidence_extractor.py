"""Plan 3 — per-qid RCA evidence extractor.

Public surface:
  * ``extract_evidence_for_qid(w, qid, judge, asi, sql, iteration)``
    — dispatches one Plan-2 reasoning call for one qid; returns
    ``PerQidRcaEvidence`` on success, ``None`` on abstain/error
    (caller falls back to ``_asi_finding_from_metadata``).
  * ``extract_evidence_for_all_qids(w, qids, ...)`` — sequential
    driver (added in Task 11).

Internal helpers:
  * ``_build_request_for_qid`` — pure: builds the LlmReasoningRequest
    without dispatching. Exposed for testability.
  * ``_render_user_prompt`` — pure: renders the per-qid user prompt.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from genie_space_optimizer.optimization.llm_reasoning_call import (
    LlmReasoningCall,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.skills._loader import _SKILL_LOADER

logger = logging.getLogger(__name__)

_SKILL_ID = "rca_evidence_extraction"
_PROMPT_CONST = "RCA_EVIDENCE_EXTRACTION_PROMPT"
_HYPHEN_SKILL_ID = "rca-evidence-extraction"


def _render_user_prompt(
    *,
    qid: str,
    judge: dict[str, Any],
    asi: dict[str, Any],
    sql: str,
) -> str:
    """Render the per-qid user prompt as a single JSON-shaped block.

    Per the Anthropic context-engineering guide: smallest possible
    set of high-signal tokens. We pass only the fields the SKILL.md
    promises in <context_inputs>; no extra blob.
    """
    payload = {
        "qid": str(qid),
        "judge_verdict": str(
            judge.get("verdict")
            or judge.get("failure_type")
            or ""
        ),
        "generated_sql": str(sql or ""),
        "sql_diff": str(asi.get("sql_diff") or ""),
        "counterfactual_fix": str(asi.get("counterfactual_fix") or ""),
        "asi_features": {
            k: v for k, v in (asi or {}).items()
            if k in (
                "failure_type", "wrong_clause", "expected_objects",
                "actual_objects",
            )
        },
        "blame_set_hint": list(
            asi.get("expected_objects") or asi.get("blame_set") or []
        ),
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def _build_request_for_qid(
    *,
    qid: str,
    judge: dict[str, Any],
    asi: dict[str, Any],
    sql: str,
    iteration: int,
) -> LlmReasoningRequest:
    """Build a Plan-2 LlmReasoningRequest for one qid. Pure function —
    no LLM dispatch, no I/O. Exposed for testability."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    if rsm is None:
        raise RuntimeError(
            f"{_SKILL_ID!r} is not a reasoning skill — check SKILL.md "
            "frontmatter"
        )
    output_cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=_PROMPT_CONST,
    )
    user_prompt = _render_user_prompt(
        qid=qid, judge=judge, asi=asi, sql=sql,
    )
    call_id = f"rca_evidence.iter_{int(iteration)}.{qid}"
    return LlmReasoningRequest(
        call_id=call_id,
        skill_id=_HYPHEN_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
    )


def extract_evidence_for_qid(
    *,
    w: Any,
    qid: str,
    judge: dict[str, Any],
    asi: dict[str, Any],
    sql: str,
    iteration: int,
) -> PerQidRcaEvidence | None:
    """Dispatch one reasoning call for one qid; return typed evidence
    or None on abstain/error.

    The caller is responsible for the deterministic fallback when
    this returns None — the extractor's only job is to translate a
    Plan-2 LlmReasoningResponse into a typed PerQidRcaEvidence.
    """
    request = _build_request_for_qid(
        qid=qid, judge=judge, asi=asi, sql=sql, iteration=iteration,
    )
    response = LlmReasoningCall().invoke(w=w, request=request)
    if not response.succeeded or response.parsed_output is None:
        if response.declined is not None:
            logger.info(
                "rca_evidence_extractor.declined qid=%s reason=%s "
                "needed_evidence=%s",
                qid, response.declined.reason.value,
                list(response.declined.needed_evidence),
            )
        elif response.error is not None:
            logger.warning(
                "rca_evidence_extractor.error qid=%s error=%s",
                qid, response.error,
            )
        return None
    parsed = response.parsed_output
    return PerQidRcaEvidence(
        qid=str(parsed["qid"]),
        observed_failure=str(parsed["observed_failure"]),
        generated_sql_issue=str(parsed["generated_sql_issue"]),
        expected_sql_shape=str(parsed["expected_sql_shape"]),
        blame_set=tuple(str(x) for x in parsed.get("blame_set") or []),
        suggested_repair_family=str(parsed["suggested_repair_family"]),
        repair_hint_patch_type=PatchType(parsed["repair_hint_patch_type"]),
        confidence=parsed["confidence"],
        quoted_evidence=tuple(
            str(x) for x in parsed.get("quoted_evidence") or []
        ),
    )


def extract_evidence_for_all_qids(
    *,
    w: Any,
    qids: tuple[str, ...],
    judge_by_qid: dict[str, dict[str, Any]],
    asi_by_qid: dict[str, dict[str, Any]],
    sql_by_qid: dict[str, str],
    iteration: int,
) -> dict[str, PerQidRcaEvidence]:
    """Sequential per-qid driver.

    Returns a dict keyed by qid containing ONLY the qids for which
    the LLM successfully produced typed evidence. Qids missing from
    the returned dict (LLM declined or errored) signal to the caller
    that the deterministic fallback should be used for that qid.

    Per-iteration token budgeting is handled by Plan 2's
    ``IterationTokenBudget`` (a ContextVar consulted inside
    ``LlmReasoningCall.invoke``); the driver itself does no budget
    accounting.

    Dispatch is sequential (not concurrent) by design — see Plan 3
    Out of scope.
    """
    out: dict[str, PerQidRcaEvidence] = {}
    for qid in qids:
        qstr = str(qid)
        if not qstr:
            continue
        evidence = extract_evidence_for_qid(
            w=w,
            qid=qstr,
            judge=judge_by_qid.get(qstr) or {},
            asi=asi_by_qid.get(qstr) or {},
            sql=sql_by_qid.get(qstr) or "",
            iteration=iteration,
        )
        if evidence is not None:
            out[qstr] = evidence
    return out
