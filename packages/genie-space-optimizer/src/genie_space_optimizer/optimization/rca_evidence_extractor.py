"""Plan 3 — per-qid RCA evidence extractor.

Public surface:
  * ``extract_evidence_for_qid(w, qid, judge, asi, sql, iteration)``
    — dispatches one Plan-2 reasoning call for one qid; returns
    ``PerQidRcaEvidence`` on success, ``None`` on abstain/error
    (caller falls back to ``_asi_finding_from_metadata``).
  * ``extract_evidence_for_qid_batch(w, qids, ...)`` — Phase 1 P1.2
    batched driver: ONE LLM call returning typed evidence for the
    full batch of QIDs. Caller falls back to ``extract_evidence_for_qid``
    for any QIDs missing from the response.
  * ``extract_evidence_for_all_qids(w, qids, ...)`` — public driver
    that uses ``extract_evidence_for_qid_batch`` to chunk QIDs into
    batches sized below the prompt-token ceiling, then falls back to
    per-QID extraction for any QID the batched call did not return
    typed evidence for. Sequential driver from Task 11 retained as
    the per-QID fallback path.

Internal helpers:
  * ``_build_request_for_qid`` — pure: builds the LlmReasoningRequest
    without dispatching. Exposed for testability.
  * ``_render_user_prompt`` — pure: renders the per-qid user prompt.
  * ``_render_batched_user_prompt`` — pure: renders the multi-QID
    batched user prompt (P1.2). Trims judge / asi / sql payloads to
    only the surface area the skill's output schema requires.
  * ``_build_request_for_batch`` — pure: builds an ``LlmReasoningRequest``
    bound to ``BatchedPerQidRcaEvidenceOutput`` with a batched
    system prompt.
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

# Phase 1 P1.2 — batched extraction caps. The ceiling matches the
# Stage 3 batcher (P1.1) so the same MAX_PROMPT_INPUT_TOKENS gate
# (40k) catches over-large prompts at the same boundary.
BATCH_RCA_MIN_QIDS: int = 3
BATCH_RCA_MAX_INPUT_TOKENS: int = 35_000
# Default per-call qid count. Capped here so the per-batch prompt
# stays well under the per-call max even when individual judge / asi
# payloads are long; the caller can pass a different batch_size
# explicitly when it knows payload sizes.
BATCH_RCA_DEFAULT_BATCH_SIZE: int = 12


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


def _render_batched_user_prompt(
    *,
    qids: tuple[str, ...],
    judge_by_qid: dict[str, dict[str, Any]],
    asi_by_qid: dict[str, dict[str, Any]],
    sql_by_qid: dict[str, str],
) -> str:
    """Phase 1 P1.2 — render the batched user prompt as ONE JSON
    payload whose ``qids`` array lists each QID's trimmed context.

    The per-QID context shape matches :func:`_render_user_prompt` so
    the LLM sees the same fields it has been trained against. The
    only addition is a batched preamble naming the task explicitly.
    """
    entries = []
    for qid in qids:
        qs = str(qid)
        if not qs:
            continue
        judge = judge_by_qid.get(qs) or {}
        asi = asi_by_qid.get(qs) or {}
        sql = sql_by_qid.get(qs) or ""
        entries.append(
            {
                "qid": qs,
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
                    asi.get("expected_objects")
                    or asi.get("blame_set")
                    or []
                ),
            }
        )
    return json.dumps(
        {
            "batch_directive": (
                "Phase 1 P1.2 — Batched RCA evidence extraction. You are "
                "receiving multiple failing QIDs in ONE call. Emit a "
                "JSON object whose ``evidences`` array carries ONE entry "
                "per QID listed below, in any order. Each entry MUST "
                "echo its own ``qid`` so the caller can route. Treat "
                "each QID independently — do not let evidence from one "
                "QID bleed into another's diagnosis."
            ),
            "qids": entries,
        },
        indent=2,
        sort_keys=True,
    )


def _build_request_for_batch(
    *,
    qids: tuple[str, ...],
    judge_by_qid: dict[str, dict[str, Any]],
    asi_by_qid: dict[str, dict[str, Any]],
    sql_by_qid: dict[str, str],
    iteration: int,
) -> LlmReasoningRequest:
    """Phase 1 P1.2 — build a batched ``LlmReasoningRequest`` whose
    ``result_cls`` is :class:`BatchedPerQidRcaEvidenceOutput`.

    The skill body is loaded from SKILL.md, but the system prompt is
    extended with a short batched-mode preamble so the LLM knows to
    emit a list rather than a single object. The closed-vocab output
    schema enforces the contract; the preamble is purely advisory.
    """
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    if rsm is None:
        raise RuntimeError(
            f"{_SKILL_ID!r} is not a reasoning skill — check SKILL.md "
            "frontmatter"
        )
    from genie_space_optimizer.skills.rca_evidence_extraction.output_schema import (
        BatchedPerQidRcaEvidenceOutput,
    )
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=_PROMPT_CONST,
    )
    # Append a batched-mode addendum to the skill body. The skill
    # body is byte-identical across calls so the prompt cache (P0.5)
    # serves it at 0.1x cost after the cache warms.
    batched_addendum = (
        "\n\n<batched_mode_addendum>\n"
        "Phase 1 P1.2 — You are now in batched mode. The user "
        "message contains a LIST of failing QIDs under "
        "``qids[]``. Emit a JSON object with an ``evidences`` "
        "array. The array MUST contain exactly one entry per "
        "input QID (no extras, no missing entries). Each entry "
        "must echo its own ``qid``. Treat each QID independently "
        "— do not let one QID's evidence leak into another's "
        "diagnosis.\n"
        "</batched_mode_addendum>"
    )
    system_msg = system_body + batched_addendum
    user_prompt = _render_batched_user_prompt(
        qids=qids,
        judge_by_qid=judge_by_qid,
        asi_by_qid=asi_by_qid,
        sql_by_qid=sql_by_qid,
    )
    call_id = (
        f"rca_evidence.batched.iter_{int(iteration)}.n{len(qids)}"
    )
    return LlmReasoningRequest(
        call_id=call_id,
        skill_id=_HYPHEN_SKILL_ID,
        system_msg=system_msg,
        user_prompt=user_prompt,
        # The skill's per-QID max_tokens scaled to the batch size,
        # capped at a sane upper bound so a degenerate batch does
        # not request unbounded output tokens.
        max_tokens=min(
            rsm.max_tokens * max(1, len(qids)),
            8000,
        ),
        result_cls=BatchedPerQidRcaEvidenceOutput,
    )


def _evidence_from_parsed_entry(entry: dict[str, Any]) -> PerQidRcaEvidence:
    """Translate one parsed ``PerQidRcaEvidenceOutput`` dict into the
    dataclass carrier the rest of the optimizer consumes.

    Pulled out of ``extract_evidence_for_qid`` so both the per-QID
    and batched paths produce byte-identical typed evidence.
    """
    return PerQidRcaEvidence(
        qid=str(entry["qid"]),
        observed_failure=str(entry["observed_failure"]),
        generated_sql_issue=str(entry["generated_sql_issue"]),
        expected_sql_shape=str(entry["expected_sql_shape"]),
        blame_set=tuple(str(x) for x in entry.get("blame_set") or []),
        suggested_repair_family=str(entry["suggested_repair_family"]),
        repair_hint_patch_type=PatchType(entry["repair_hint_patch_type"]),
        confidence=entry["confidence"],
        quoted_evidence=tuple(
            str(x) for x in entry.get("quoted_evidence") or []
        ),
    )


def extract_evidence_for_qid_batch(
    *,
    w: Any,
    qids: tuple[str, ...],
    judge_by_qid: dict[str, dict[str, Any]],
    asi_by_qid: dict[str, dict[str, Any]],
    sql_by_qid: dict[str, str],
    iteration: int,
) -> dict[str, PerQidRcaEvidence]:
    """Phase 1 P1.2 — dispatch ONE batched LLM call for ``qids``.

    Returns a dict keyed by qid for ONLY the qids the LLM successfully
    diagnosed. Missing qids fall through to the per-QID extractor in
    the caller's fallback path. The function never raises on bad LLM
    output — entries that fail to translate into ``PerQidRcaEvidence``
    are silently dropped so degradation is graceful.
    """
    qids = tuple(str(q) for q in qids if str(q))
    if not qids:
        return {}
    request = _build_request_for_batch(
        qids=qids,
        judge_by_qid=judge_by_qid,
        asi_by_qid=asi_by_qid,
        sql_by_qid=sql_by_qid,
        iteration=iteration,
    )
    response = LlmReasoningCall().invoke(w=w, request=request)
    if not response.succeeded or response.parsed_output is None:
        if response.declined is not None:
            logger.info(
                "rca_evidence_extractor.batched_declined n=%d reason=%s",
                len(qids), response.declined.reason.value,
            )
        elif response.error is not None:
            logger.warning(
                "rca_evidence_extractor.batched_error n=%d error=%s",
                len(qids), response.error,
            )
        return {}
    parsed = response.parsed_output
    out: dict[str, PerQidRcaEvidence] = {}
    for entry in parsed.get("evidences") or ():
        if not isinstance(entry, dict):
            continue
        try:
            evidence = _evidence_from_parsed_entry(entry)
        except (KeyError, ValueError, TypeError):
            continue
        if evidence.qid in qids:
            out[evidence.qid] = evidence
    return out


def _estimate_batch_input_tokens(
    qids: tuple[str, ...],
    judge_by_qid: dict[str, dict[str, Any]],
    asi_by_qid: dict[str, dict[str, Any]],
    sql_by_qid: dict[str, str],
) -> int:
    """Conservative upper bound on the batched user_prompt token cost
    at 4 chars per token. Used to chunk QIDs into batches that fit
    under :data:`BATCH_RCA_MAX_INPUT_TOKENS`.
    """
    payload = _render_batched_user_prompt(
        qids=qids,
        judge_by_qid=judge_by_qid,
        asi_by_qid=asi_by_qid,
        sql_by_qid=sql_by_qid,
    )
    return (len(payload) + 3) // 4


def extract_evidence_for_all_qids(
    *,
    w: Any,
    qids: tuple[str, ...],
    judge_by_qid: dict[str, dict[str, Any]],
    asi_by_qid: dict[str, dict[str, Any]],
    sql_by_qid: dict[str, str],
    iteration: int,
    batch_size: int = BATCH_RCA_DEFAULT_BATCH_SIZE,
) -> dict[str, PerQidRcaEvidence]:
    """Public driver.

    Phase 1 P1.2: chunks ``qids`` into batches of size ``batch_size``
    and runs ONE LLM call per batch via
    :func:`extract_evidence_for_qid_batch`. Any qid the batched call
    did not return typed evidence for falls through to the per-QID
    extractor — this preserves the per-QID skill contract for the
    long-tail abstain cases the batched LLM can't handle.

    When the qid count is below :data:`BATCH_RCA_MIN_QIDS` the driver
    skips batching and goes straight to the per-QID path; this avoids
    paying the batched-mode addendum overhead on tiny iterations.

    Returns a dict keyed by qid containing ONLY the qids for which
    the LLM (batched OR per-QID) successfully produced typed evidence.
    Qids missing from the returned dict (LLM declined or errored on
    both paths) signal to the caller that the deterministic fallback
    should be used.

    Per-iteration token budgeting is handled by Plan 2's
    ``IterationTokenBudget`` (a ContextVar consulted inside
    ``LlmReasoningCall.invoke``); the driver itself does no budget
    accounting.
    """
    qid_tuple = tuple(str(q) for q in qids if str(q))
    if not qid_tuple:
        return {}

    out: dict[str, PerQidRcaEvidence] = {}

    # Below the batching floor, go straight to the per-QID path.
    if len(qid_tuple) < BATCH_RCA_MIN_QIDS:
        for qstr in qid_tuple:
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

    # Chunk qids. We shrink the batch_size adaptively when the
    # batched payload's estimated input tokens exceed the per-call
    # ceiling — protects against pathological cases where one batch
    # blows past MAX_PROMPT_INPUT_TOKENS.
    bs = max(1, int(batch_size))
    i = 0
    while i < len(qid_tuple):
        chunk = qid_tuple[i : i + bs]
        # Shrink the chunk until its estimate fits, but never below 1.
        while len(chunk) > 1 and _estimate_batch_input_tokens(
            chunk, judge_by_qid, asi_by_qid, sql_by_qid,
        ) > BATCH_RCA_MAX_INPUT_TOKENS:
            chunk = chunk[: max(1, len(chunk) // 2)]
        batched_out = extract_evidence_for_qid_batch(
            w=w,
            qids=chunk,
            judge_by_qid=judge_by_qid,
            asi_by_qid=asi_by_qid,
            sql_by_qid=sql_by_qid,
            iteration=iteration,
        )
        out.update(batched_out)
        i += len(chunk)

    # Per-QID fallback for any qid the batched calls did not cover.
    for qstr in qid_tuple:
        if qstr in out:
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
