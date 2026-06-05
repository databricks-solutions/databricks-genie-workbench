"""Plan 11 — Stage 1: LLM-driven per-QID diagnosis.

Replaces the deterministic RCA classifier. Each failing QID produces one
:class:`PerQidDiagnosis` with a free-text ``rca_kind_label``.

Entry point: :func:`diagnose_failing_qids`.

The handler is dormant during PR 1 (the feature flag is OFF). PR 2 wires
it into ``optimizer.py``.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from genie_space_optimizer.common.config import LLM_ENDPOINT
from genie_space_optimizer.optimization.llm_reasoning_call import LlmReasoningCall
from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningRequest
from genie_space_optimizer.optimization.run_analysis_contract import (
    llm_contract_failure_marker,
    plan11_stage1_diagnosis_marker,
    plan11_stage1_request_marker,
)
from genie_space_optimizer.optimization.stages.plan11_types import PerQidDiagnosis
from genie_space_optimizer.skills._loader import _SKILL_LOADER
from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
    Plan11DiagnoseOutput,
)


_SKILL_ID = "plan11_diagnose"
_PROMPT_CONST = "PLAN11_DIAGNOSE_PROMPT"


def _classify_llm_error(
    exception_class: str,
    error_message: str,
    tokens_input: int,
    request: LlmReasoningRequest,
) -> str:
    """Map an LLM-call exception class to a structured ``error_kind``.

    Phase 1.C of the SM cutover. The 2026-05-23 trial emitted
    ``outcome="llm_error"`` markers with ``tokens_input=0`` and no
    further detail, leaving postmortems unable to distinguish a
    pre-flight client failure (no endpoint reachable) from a mid-flight
    parse failure. This classifier maps the exception class name from
    ``LlmReasoningResponse.error`` (formatted as ``"ClassName: msg"``)
    into one of: client_construction, empty_prompt, endpoint_decline,
    timeout, parse, response_format_invalid, token_limit_exceeded,
    auth, unknown.

    PR-A (2026-05-23) — also inspects ``error_message`` so that
    ``BadRequestError`` (the dominant failure mode in the 98ec8950 /
    dc89d1a9 trials) maps to a specific cause rather than ``unknown``.
    The Databricks Foundation Model API embeds the rejection reason
    in the 400 body, which the OpenAI SDK surfaces via ``str(exc)``.

    PR-1C (2026-05-23) — adds a ``request_envelope_invalid`` arm
    specifically for the tool-name regex violation
    (``tools.0.custom.name failed ^[a-zA-Z0-9_-]{1,128}$``) and any
    runtime pre-flight failure that PR-2C will raise as
    ``RequestEnvelopeInvalidError``. Both share the property that the
    request envelope was malformed BEFORE inference, and the right
    follow-up is to fix our local request builder, not to retry, not
    to escalate to ops, and not to investigate the prompt.
    """
    msg = (error_message or "").lower()
    if not exception_class:
        # Empty prompt is the only way to get an "llm_error" without an
        # underlying exception class (the call short-circuits).
        if tokens_input == 0 and not (
            request.user_prompt and request.system_msg
        ):
            return "empty_prompt"
        return "unknown"

    cls = exception_class.lower()
    # PR-1C — local pre-flight (PR-2C) raises this typed exception
    # BEFORE the OpenAI client is invoked. Map by class name so the
    # arm fires even if the message format changes.
    if "requestenvelopeinvalid" in cls:
        return "request_envelope_invalid"
    # Phase 0 P0.3 — workspace-level Foundation Model API rate limit.
    # Surfaced by the OpenAI SDK as ``RateLimitError`` (and by
    # Databricks as ``REQUEST_LIMIT_EXCEEDED`` in the body) when the
    # workspace's ITPM / OTPM / QPH limits trip on the shared sliding
    # 60s window. Distinct from ``token_limit_exceeded`` (per-call
    # context cap) and ``endpoint_decline`` (per-call 400 body).
    # Marker arm must precede the generic ``client``/``connection``
    # checks below because ``RateLimitError`` would otherwise fall
    # through to ``client_construction``.
    if "ratelimit" in cls or "toomanyrequests" in cls:
        return "rate_limited"
    if (
        "request_limit_exceeded" in msg
        or "rate limit exceeded" in msg
        or "rate-limit exceeded" in msg
        or "too many requests" in msg
        or " 429" in msg
        or "code: 429" in msg
        or "input token rate limit" in msg
    ):
        return "rate_limited"
    if "timeout" in cls:
        return "timeout"
    # Trial 13 Track 4 — ``string_too_long`` errors raised by Pydantic
    # on a post-LLM-parse response. These fire when the LLM returned
    # semantically-correct content but local validation rejected it
    # because a field cap was set too low. Trial 13 also relaxes the
    # caps and replaces the rejection with a graceful-truncate
    # validator; this arm exists so future regressions (a new field
    # without the truncate validator, an LLM that emits structured
    # blobs we didn't anticipate) surface a typed error_kind instead
    # of falling through to ``unknown`` or ``parse``.
    if "string_too_long" in msg or "should have at most" in msg:
        return "response_post_parse_field_length"
    if "envelopecontract" in cls or "parse" in cls or "json" in cls:
        return "parse"
    # BadRequestError + status-400 rejections: inspect the body so the
    # marker tells postmortems which envelope/budget knob misfired.
    if "badrequest" in cls or " 400" in msg or "code: 400" in msg:
        if (
            "context length" in msg
            or "context_length" in msg
            or "maximum context" in msg
            or "token limit" in msg
            or "max_tokens" in msg
            or "too many tokens" in msg
        ):
            return "token_limit_exceeded"
        # PR-1C — tool-name regex rejection. The Databricks endpoint
        # translates ``response_format.json_schema.name`` into an
        # internal tool name and rejects it against
        # ``^[a-zA-Z0-9_-]{1,128}$``. The 98ec8950 / dc89d1a9 trials
        # surfaced this as ``tools.0.custom.name failed
        # ^[a-zA-Z0-9_-]{1,128}$`` in the 400 body. The regex itself
        # also appears in similar 400 bodies for tool-name violations
        # at other call sites — match on either signal so the arm
        # generalises beyond Plan 11.
        if (
            ("tools." in msg and ".custom.name" in msg)
            or "[a-za-z0-9_-]{1,128}" in msg
            or "request_envelope_invalid" in msg
        ):
            return "request_envelope_invalid"
        if (
            "response_format" in msg
            or "json_schema" in msg
            or "schema" in msg
            or "additionalproperties" in msg
            or "anyof" in msg
            or "oneof" in msg
            or "$ref" in msg
        ):
            return "response_format_invalid"
        return "endpoint_decline"
    if "unauthorized" in cls or "forbidden" in cls or "permission" in cls:
        return "auth"
    if "connection" in cls or "endpoint" in cls or "httpx" in cls:
        return "endpoint_decline"
    if "client" in cls or "config" in cls or "auth" in cls:
        return "client_construction"
    if tokens_input == 0 and not (request.user_prompt and request.system_msg):
        return "empty_prompt"
    return "unknown"


def _replay_request_envelope_violations(
    request: LlmReasoningRequest,
) -> list[dict[str, str]]:
    """Re-run ``DatabricksEndpointRequestContract.validate`` against
    the same wire envelope ``LlmReasoningCall.invoke`` would have
    dispatched, and return the violations as marker-friendly dicts.

    PR-2C — populates the ``constraint_violations`` field on
    ``GSO_PLAN11_STAGE1_REQUEST_V1`` whenever the classifier returned
    ``error_kind="request_envelope_invalid"``. Re-deriving locally
    (rather than threading the typed exception through
    ``LlmReasoningResponse``) keeps the request/response surface
    unchanged and stays correct even when the marker is being
    reconstructed after the fact (e.g. log replay).

    Returns an empty list if the local replay produces no violations
    — that can happen if the wire-format constraint that fired was
    one the validator does not yet pin (e.g. a brand-new endpoint
    rule). The marker still emits ``error_kind="request_envelope_invalid"``
    in that case; the empty ``constraint_violations`` list is the
    postmortem's cue to extend
    ``DatabricksEndpointRequestContract.validate``.
    """
    try:
        from genie_space_optimizer.optimization.databricks_request_contract import (
            DEFAULT_CONTRACT,
        )
        from genie_space_optimizer.optimization.llm_reasoning_io import (
            AbstainableEnvelope,
        )
        from genie_space_optimizer.optimization.prompt_io import (
            build_response_format,
        )

        envelope_cls = AbstainableEnvelope[request.result_cls]
        response_format = build_response_format(envelope_cls)
        messages: list[dict[str, str]] = []
        if request.system_msg and request.system_msg.strip():
            messages.append({"role": "system", "content": request.system_msg})
        messages.append({"role": "user", "content": request.user_prompt})
        call_kwargs: dict[str, Any] = {
            "model": LLM_ENDPOINT,
            "messages": messages,
            "temperature": 0.0,
            "max_tokens": int(request.max_tokens),
            "response_format": response_format,
        }
        violations = DEFAULT_CONTRACT.validate(call_kwargs)
        return [
            {"field": v.field, "constraint": v.constraint}
            for v in violations
        ]
    except Exception:
        return []


def _response_format_keywords(result_cls: type) -> list[str]:
    """Return a sorted union of keywords from the response_format the
    Stage 1 request will bind.

    PR-A diagnostic: postmortems for the 98ec8950 / dc89d1a9 trials
    observed ``BadRequestError`` on every Stage 1 call. The Databricks
    Foundation Model API rejects ``response_format`` payloads whose
    schema contains unsupported keywords (``$ref``, ``anyOf``,
    ``oneOf``, etc.). Surfacing the keyword set inline in the failure
    marker tells operators whether to suspect the envelope or the
    skill's output schema without re-running the lever loop.
    """
    try:
        from genie_space_optimizer.optimization.llm_reasoning_io import (
            AbstainableEnvelope,
        )
        from genie_space_optimizer.optimization.prompt_io import (
            build_response_format,
        )

        envelope_cls = AbstainableEnvelope[result_cls]
        rf = build_response_format(envelope_cls)
        keys: set[str] = set(rf.keys())
        json_schema = rf.get("json_schema") or {}
        if isinstance(json_schema, dict):
            keys.update(f"json_schema.{k}" for k in json_schema.keys())
            schema = json_schema.get("schema") or {}
            if isinstance(schema, dict):
                keys.update(f"schema.{k}" for k in schema.keys())
        return sorted(keys)
    except Exception:
        return []


def _persist_llm_error_dump(
    *,
    optimization_run_id: str,
    iteration: int,
    qid: str,
    call_id: str,
    error_message: str,
    request: LlmReasoningRequest,
) -> str:
    """Write the full untruncated error body + request fingerprint to
    ``{run_root}/llm_errors/stage1_{iteration}_{qid}.json``.

    Stdout markers truncate to 500 chars so the lever-loop log stays
    readable; the disk dump is the long-form record postmortems use
    when they need the *full* Databricks-side error body (which can
    exceed 2 KB for JSON-schema validation failures).

    Returns the absolute path to the dump file (or empty string on
    failure — disk persistence is best-effort and must never break
    the marker emission).
    """
    try:
        # 2026-05-26 hardening — replace hardcoded ``/tmp/gso/<run_id>``
        # fallback with the central resolver that handles
        # ``PermissionError`` from shared-tmp collisions on
        # Databricks Apps nodes (see ``run_root_resolver`` module).
        from genie_space_optimizer.optimization.run_root_resolver import (
            resolve_run_root as _resolve_run_root,
        )
        run_root = _resolve_run_root(str(optimization_run_id))
        out_dir = run_root / "llm_errors"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"stage1_{int(iteration)}_{qid}.json"
        payload = {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "qid": str(qid),
            "call_id": str(call_id),
            "skill_id": request.skill_id,
            "endpoint": LLM_ENDPOINT,
            "max_tokens": int(request.max_tokens),
            "system_msg_chars": len(request.system_msg or ""),
            "user_prompt_chars": len(request.user_prompt or ""),
            "error_message": str(error_message or ""),
        }
        path.write_text(json.dumps(payload, indent=2, default=str))
        return str(path)
    except Exception:
        return ""


def _build_request(
    *,
    failing_qids: list[dict[str, Any]],
    schema_columns: list[str],
    recent_diagnoses: list[dict[str, Any]],
    iteration: int,
) -> LlmReasoningRequest:
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

    # Phase 0 P0.4 — LRU compaction. Stage 1's recent_diagnoses
    # accumulates across iterations and was the dominant growth term
    # in the 22-of-32 rate-limited diagnoses postmortem
    # (e94376a3...). We compact in place so the most recent
    # diagnosis is always retained while older ones are shed first.
    # The ordering is "least valuable last" — recent_diagnoses go
    # first because schema_columns and failing_qids are load-bearing
    # for the current iteration's diagnosis.
    # Phase 1 P1.3 — fixed-window cap of recent_diagnoses to the last
    # 3 iterations BEFORE the LRU compactor sees the slot. Bounds the
    # prompt size structurally so the LRU compactor only fires in the
    # rare case where the 3-iteration window itself overflows.
    from genie_space_optimizer.optimization.llm_history_window import (
        cap_iteration_bucketed_history,
    )
    from genie_space_optimizer.optimization.llm_prompt_compaction import (
        compact_history_slots_to_fit,
    )
    from genie_space_optimizer.optimization.llm_reasoning_call import (
        MAX_PROMPT_INPUT_TOKENS,
    )

    # Work on local copies so we don't mutate caller-owned lists.
    # Phase 1 P1.3 — cap to last 3 iterations; older entries collapse
    # to a single digest dict so the LLM still sees that earlier
    # diagnoses existed but does not pay per-entry tokens for them.
    recent_diagnoses = cap_iteration_bucketed_history(
        recent_diagnoses or [], current_iteration=iteration,
    )
    static_chars = (
        len(system_body)
        + len(json.dumps(
            {
                "iteration": iteration,
                "failing_qids": failing_qids,
                "schema_columns": schema_columns,
                "recent_diagnoses_for_same_qids": [],
            },
            default=str,
        ))
    )
    compact_history_slots_to_fit(
        static_chars=static_chars,
        history_slots=[
            ("recent_diagnoses_for_same_qids", recent_diagnoses),
        ],
        target_token_cap=MAX_PROMPT_INPUT_TOKENS,
    )

    # Phase 0 P0.5 — split ``schema_columns`` into a cacheable block.
    # The column list is stable for the lifetime of one optimization
    # run (no schema changes between iterations) so caching it lets
    # every subsequent Stage 1 call pay 0.1x on the wire for that
    # block. The dynamic payload keeps iteration / failing_qids /
    # recent_diagnoses since they change every call.
    schema_columns_block = json.dumps(
        {"schema_columns": list(schema_columns)}, default=str,
    )
    user_prompt = json.dumps(
        {
            "iteration": iteration,
            "failing_qids": failing_qids,
            "recent_diagnoses_for_same_qids": recent_diagnoses,
        },
        default=str,
    )
    return LlmReasoningRequest(
        call_id=f"plan11_stage1_diagnose.iter_{int(iteration)}",
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
        cacheable_user_blocks=(schema_columns_block,),
    )


def _lookup_seed_for_qid(
    failing_qids: list[dict[str, Any]], qid: str
) -> tuple[str, ...]:
    """Return the ``blame_set_seed`` attached to ``qid`` in the input cards.

    Trial 13h seed-backfill safety net: when the Stage 1 LLM emits an empty
    ``blame_set`` (or emits entries that all fail the ``schema_columns``
    filter), we fall back on the seed already plumbed into the input card by
    ``build_stage1_evidence_card``. The seed is guaranteed non-empty by the
    Stage 1 input-evidence contract (``min_blame_set_size >= 1``), so this
    lookup is the load-bearing fallback for the ``effective_blame_set``
    chain at Stage 1 (analogous to the Stage 3 chain established in
    Trial 13g).

    Returns an empty tuple if the qid is not in the list (defensive — the
    parse loop only iterates over LLM-returned items, but the seed lookup
    is keyed by qid).
    """
    for card in failing_qids:
        if str(card.get("qid", "")) == qid:
            raw_seed = card.get("blame_set_seed") or ()
            return tuple(str(s) for s in raw_seed if str(s).strip())
    return ()


def diagnose_failing_qids(
    *,
    failing_qids: list[dict[str, Any]],
    schema_columns: list[str],
    optimization_run_id: str,
    iteration: int,
    w: Any,
    recent_diagnoses: list[dict[str, Any]] | None = None,
) -> list[PerQidDiagnosis]:
    """Plan 11 Stage 1 — batch-diagnose all failing QIDs in one LLM call.

    Returns a list of :class:`PerQidDiagnosis` (one per diagnosed QID).
    QIDs the LLM declines on, or for which the call fails, are omitted
    from the return; their markers carry ``outcome="declined"`` or
    ``"llm_error"``. The empty-input path returns ``[]`` with no marker.
    """
    if not failing_qids:
        return []

    request = _build_request(
        failing_qids=failing_qids,
        schema_columns=schema_columns,
        recent_diagnoses=recent_diagnoses or [],
        iteration=iteration,
    )

    t0 = time.monotonic()
    resp = LlmReasoningCall().invoke(w=w, request=request)
    duration_ms = int((time.monotonic() - t0) * 1000)
    tokens_in = int(getattr(resp, "tokens_input", 0) or 0)
    tokens_out = int(getattr(resp, "tokens_output", 0) or 0)

    if not resp.succeeded or resp.parsed_output is None:
        abstain_reason = ""
        abstain_explanation = ""
        if resp.declined is not None:
            abstain_reason = str(getattr(resp.declined, "reason", ""))
            abstain_explanation = str(getattr(resp.declined, "explanation", ""))
        outcome = "declined" if resp.declined is not None else "llm_error"
        # SM Cutover Phase 1.C — classify llm_error so the zero-token
        # marker postmortems saw in 2026-05-23 ("opaque llm_error with
        # tokens_input=0") becomes actionable.
        # PR-A (2026-05-23) — additionally surface error_message,
        # response_format_keywords, and persist a disk dump so postmortems
        # can pinpoint *why* the endpoint returned 400 without re-running.
        error_kind = ""
        exception_class = ""
        error_message_truncated = ""
        full_error_message = ""
        rf_keywords: list[str] = []
        constraint_violations: list[dict[str, str]] = []
        if outcome == "llm_error":
            full_error_message = str(getattr(resp, "error", "") or "")
            # Error format from llm_reasoning_call.invoke: "ClassName: message".
            exception_class = (
                full_error_message.split(":", 1)[0].strip()
                if full_error_message
                else ""
            )
            error_message_truncated = full_error_message[:500]
            error_kind = _classify_llm_error(
                exception_class,
                full_error_message,
                tokens_in,
                request,
            )
            rf_keywords = _response_format_keywords(request.result_cls)
            # PR-2C — when the failure is a pre-flight contract refusal,
            # re-run the validator deterministically against the wire
            # envelope so the marker carries the structured
            # ``constraint_violations`` list. This is cheaper than
            # threading the exception object through ``LlmReasoningCall``
            # and stays accurate even when the wire-format response
            # builder evolves.
            if error_kind == "request_envelope_invalid":
                constraint_violations = _replay_request_envelope_violations(
                    request,
                )
        for qid_input in failing_qids:
            qid = str(qid_input.get("qid", ""))
            print(
                plan11_stage1_diagnosis_marker(
                    optimization_run_id=optimization_run_id,
                    iteration=iteration,
                    qid=qid,
                    outcome=outcome,
                    abstain_reason=abstain_reason,
                    abstain_explanation=abstain_explanation,
                    duration_ms=duration_ms,
                    tokens_input=tokens_in,
                    tokens_output=tokens_out,
                    error_kind=error_kind,
                    exception_class=exception_class,
                    error_message=error_message_truncated,
                    endpoint=LLM_ENDPOINT if outcome == "llm_error" else "",
                )
            )
            if outcome == "llm_error":
                print(
                    plan11_stage1_request_marker(
                        optimization_run_id=optimization_run_id,
                        iteration=iteration,
                        qid=qid,
                        skill_id=request.skill_id,
                        call_id=request.call_id,
                        system_msg_chars=len(request.system_msg or ""),
                        user_prompt_chars=len(request.user_prompt or ""),
                        max_tokens=int(request.max_tokens),
                        response_format_keywords=rf_keywords,
                        endpoint=LLM_ENDPOINT,
                        constraint_violations=constraint_violations,
                    )
                )
                _persist_llm_error_dump(
                    optimization_run_id=optimization_run_id,
                    iteration=iteration,
                    qid=qid,
                    call_id=request.call_id,
                    error_message=full_error_message,
                    request=request,
                )
        return []

    # parsed_output is a dict (LlmReasoningResponse stores parsed.model_dump()).
    raw_diagnoses = resp.parsed_output.get("diagnoses", []) or []
    if not raw_diagnoses:
        # Pydantic validation succeeded but the model emitted no items —
        # treat as a contract failure so postmortem can grep for it.
        print(
            llm_contract_failure_marker(
                schema_name="Plan11DiagnoseOutput",
                failing_fields=["diagnoses"],
                raw_payload=resp.parsed_output,
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                skill_name=_SKILL_ID,
                error_repr="empty diagnoses list",
            )
        )
        for qid_input in failing_qids:
            qid = str(qid_input.get("qid", ""))
            print(
                plan11_stage1_diagnosis_marker(
                    optimization_run_id=optimization_run_id,
                    iteration=iteration,
                    qid=qid,
                    outcome="contract_failure",
                    duration_ms=duration_ms,
                    tokens_input=tokens_in,
                    tokens_output=tokens_out,
                )
            )
        return []

    diagnoses: list[PerQidDiagnosis] = []
    schema_col_set = set(schema_columns)
    for item in raw_diagnoses:
        # Drop blame_set entries that don't appear in schema_columns
        # (the prompt instructs the LLM to draw from this list).
        raw_blame = item.get("blame_set") or []
        valid_blame = (
            tuple(str(b) for b in raw_blame if str(b) in schema_col_set)
            if schema_col_set
            else tuple(str(b) for b in raw_blame)
        )

        # Trial 13h — seed-backfill safety net. The pre-13h failure mode
        # was a confident Stage 1 diagnosis with blame_set: [] that the
        # non-actionable gate (classify_non_actionable_reason ->
        # "zero_blame_set") correctly but unnecessarily terminated.
        # Backfill whenever the post-schema-filter result is empty —
        # covers both "LLM emitted []" and "LLM emitted entries that all
        # failed the schema_columns filter" (hallucinated FQNs). The
        # blame_set_seed on the input card is guaranteed non-empty by the
        # Stage 1 input-evidence contract and is the trusted last-mile
        # signal for what objects are implicated in the failure.
        blame_set_llm_emitted = len(raw_blame)
        blame_set_post_schema_dropped = blame_set_llm_emitted - len(valid_blame)
        blame_set_source = "llm" if valid_blame else "empty"
        if not valid_blame:
            raw_seed = _lookup_seed_for_qid(
                failing_qids, str(item.get("qid", ""))
            )
            seed_valid = (
                tuple(s for s in raw_seed if s in schema_col_set)
                if schema_col_set
                else raw_seed
            )
            if seed_valid:
                valid_blame = seed_valid
                blame_set_source = "seed_backfill"

        diag = PerQidDiagnosis(
            qid=str(item.get("qid", "")),
            rca_kind_label=str(item.get("rca_kind_label", "")),
            observed_failure=str(item.get("observed_failure", "")),
            generated_sql_issue=str(item.get("generated_sql_issue", "")),
            expected_sql_shape=str(item.get("expected_sql_shape", "")),
            blame_set=valid_blame,
            evidence_summary=str(item.get("evidence_summary", "")),
            confidence=item.get("confidence", "low"),  # type: ignore[arg-type]
            # Trial 19 B5 — carry the LLM-emitted repair intent into the
            # typed PerQidDiagnosis so Stage 2 / Stage 3 see it
            # verbatim. Empty default preserves byte-stable replay
            # against pre-Trial-19 fixtures where the field is absent.
            intended_patch_shape=str(item.get("intended_patch_shape", "")),
        )
        diagnoses.append(diag)
        print(
            plan11_stage1_diagnosis_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                qid=diag.qid,
                outcome="diagnosed",
                rca_kind_label=diag.rca_kind_label,
                confidence=str(diag.confidence),
                blame_set_size=len(diag.blame_set),
                evidence_summary_chars=len(diag.evidence_summary),
                duration_ms=duration_ms,
                tokens_input=tokens_in,
                tokens_output=tokens_out,
                blame_set_source=blame_set_source,
                blame_set_llm_emitted=blame_set_llm_emitted,
                blame_set_post_schema_dropped=blame_set_post_schema_dropped,
            )
        )

    return diagnoses
