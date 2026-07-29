"""Small optimizer utilities retained by the native v2 workflow."""

from __future__ import annotations

import logging

import re

from typing import Any, Callable

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.optimization.llm_client import get_openai_client as _get_openai_client

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    INSTRUCTION_SECTION_ORDER,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
    PROMPT_TOKEN_BUDGET,
    SPACE_DESCRIPTION_PROMPT,
    format_mlflow_template,
    get_llm_endpoint,
)

logger = logging.getLogger(__name__)

def _estimate_tokens(text: str) -> int:
    """Conservative token estimate (~4 chars per token)."""
    return len(text) // 4

def _truncate_to_budget(
    format_kwargs: dict[str, Any],
    prompt_template: str,
    priority_keys: list[str],
) -> dict[str, Any]:
    """Truncate low-priority context sections to fit within PROMPT_TOKEN_BUDGET.

    ``priority_keys`` lists context keys from LOWEST to HIGHEST priority.
    When the estimated prompt exceeds the budget, the lowest-priority keys
    are truncated first (keeping a summary prefix).
    """
    est = _estimate_tokens(prompt_template) + sum(
        _estimate_tokens(str(v)) for v in format_kwargs.values()
    )
    if est <= PROMPT_TOKEN_BUDGET:
        return format_kwargs

    overshoot = est - PROMPT_TOKEN_BUDGET
    result = dict(format_kwargs)

    for key in priority_keys:
        if overshoot <= 0:
            break
        val = str(result.get(key, ""))
        if not val:
            continue
        char_budget = max(200, len(val) - overshoot * 4)
        if char_budget < len(val):
            truncated = val[:char_budget]
            result[key] = truncated + f"\n... ({len(val) - char_budget} chars truncated for token budget)"
            overshoot -= _estimate_tokens(val) - _estimate_tokens(result[key])

    return result

def _attach_last_response(exc: BaseException, text: str) -> None:
    """Stamp the last LLM body onto an exception for downstream logging.

    When ``_traced_llm_call`` exhausts retries, callers need to know
    *what the model actually returned* — not just that parsing failed.
    Attaching via attribute (vs. wrapping the exception) preserves the
    original type/traceback so existing ``except`` chains and MLflow
    span events keep working unchanged.

    The attributes are best-effort: if ``exc`` is a frozen / C-level
    exception that rejects attribute assignment, we swallow the
    AttributeError silently (the caller falls back to an empty preview).
    """
    try:
        exc.last_response_text = text  # type: ignore[attr-defined]
        exc.last_response_chars = len(text)  # type: ignore[attr-defined]
    except (AttributeError, TypeError):
        pass

def _traced_llm_call(
    w: WorkspaceClient | None,
    system_msg: str,
    prompt: str,
    *,
    span_name: str,
    max_retries: int = LLM_MAX_RETRIES,
    temperature: float = LLM_TEMPERATURE,
    max_tokens: int | None = None,
    response_validator: Callable[[str], Any] | None = None,
) -> tuple[str, Any]:
    """Execute an LLM call via the OpenAI SDK with automatic MLflow tracing.

    ``mlflow.openai.autolog()`` instruments every OpenAI call with a
    ``CHAT_MODEL`` span that captures token usage, cost, and latency.
    This wrapper adds retry logic inside a ``CHAIN`` span and logs
    token usage on the span for visibility.

    Returns ``(raw_text, response_object)`` for the caller to parse.
    Raises the last exception if all retries are exhausted.

    ``temperature`` is accepted for backwards-compatible call sites but is
    not sent to Databricks, because some supported reasoning/frontier
    endpoints reject the parameter.

    ``response_validator`` (optional): a callable invoked with the
    trimmed completion text after each successful HTTP round-trip. If
    it raises, the exception is treated as a retryable failure (same
    exponential backoff as RPC failures). This closes the gap where a
    provider returns HTTP 200 with non-JSON / refusal content that
    callers downstream cannot parse — most notably
    ``_extract_json`` — and that previously surfaced as two identical
    tracebacks with no retry attempt. Callers that expect JSON can
    pass ``response_validator=_extract_json``. When ``None`` the
    legacy behaviour (return first HTTP 200, no post-success
    validation) is preserved for every existing call site.
    """
    import time

    import mlflow
    from mlflow.entities import SpanEvent, SpanType

    with mlflow.start_span(name=span_name, span_type=SpanType.CHAIN) as span:
        model = get_llm_endpoint()
        span.set_inputs({
            "model": model,
            "temperature_requested": temperature,
            "temperature_sent": False,
            "prompt_chars": len(prompt),
        })

        client = _get_openai_client(w)
        text = ""
        # F6 — track the most recent HTTP 200 body across attempts so we
        # can attach it to the raised exception if every retry ends in
        # validator/RPC failure. Callers (description enrichment, etc.)
        # read this off the exception to log a structured preview of
        # *what the model actually returned* rather than an empty string.
        last_response_text: str = ""
        last_err: Exception | None = None

        for attempt in range(max_retries):
            try:
                messages: list[dict[str, str]] = []
                if system_msg and system_msg.strip():
                    messages.append({"role": "system", "content": system_msg})
                messages.append({"role": "user", "content": prompt})
                from genie_space_optimizer.optimization.wide_schema_prompt import fit_messages

                messages, pack_stats = fit_messages(messages)
                span.set_inputs({
                    "model": model,
                    "temperature_requested": temperature,
                    "temperature_sent": False,
                    "prompt_chars": len(prompt),
                    "complete_request_chars": pack_stats["final_request_chars"],
                    "prompt_omitted_counts": pack_stats["omitted_counts"],
                })
                call_kwargs: dict[str, Any] = {
                    "model": model,
                    "messages": messages,
                }
                # Do not send temperature: Claude Opus 4.7/4.8 and some GPT 5.x endpoints reject it.
                if max_tokens is not None:
                    call_kwargs["max_tokens"] = max_tokens

                response = client.chat.completions.create(**call_kwargs)

                if not response.choices:
                    raise ValueError("LLM response had no choices")
                content = response.choices[0].message.content
                if not content:
                    raise ValueError("LLM response content is empty")
                text = str(content).strip()
                last_response_text = text

                if response_validator is not None:
                    try:
                        response_validator(text)
                    except Exception as exc:
                        last_err = exc
                        span.add_event(SpanEvent(
                            name=f"validator_reject_attempt_{attempt + 1}",
                            attributes={
                                "error": str(exc)[:500],
                                "response_preview": text[:200],
                                "response_chars": len(text),
                            },
                        ))
                        if attempt < max_retries - 1:
                            time.sleep(2**attempt)
                            continue
                        # F6 — attach the last-seen body to the
                        # exception so the caller's warning can log a
                        # real preview instead of an empty string.
                        _attach_last_response(exc, last_response_text)
                        raise

                _log_token_usage(span, response)

                span.set_outputs({
                    "response_chars": len(text),
                    "attempts": attempt + 1,
                })
                return text, response

            except Exception as exc:
                last_err = exc
                span.add_event(SpanEvent(
                    name=f"retry_attempt_{attempt + 1}",
                    attributes={"error": str(exc)[:500]},
                ))
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)

        span.set_outputs({
            "error": str(last_err)[:500] if last_err else "unknown",
            "attempts": max_retries,
        })
        # F6 — attach before re-raise on the non-validator exhaustion
        # path too (HTTP failures, empty-choice responses, etc.).
        if last_err is not None:
            _attach_last_response(last_err, last_response_text)
        raise last_err

def _log_token_usage(span: Any, response: Any) -> None:
    """Attach token usage from an OpenAI response to an MLflow span."""
    usage = getattr(response, "usage", None)
    if not usage:
        return
    try:
        span.set_attribute("mlflow.chat.tokenUsage", {
            "input_tokens": getattr(usage, "prompt_tokens", 0) or 0,
            "output_tokens": getattr(usage, "completion_tokens", 0) or 0,
            "total_tokens": getattr(usage, "total_tokens", 0) or 0,
        })
    except Exception:
        pass

def _resolve_scope(lever: int, apply_mode: str = APPLY_MODE) -> str:
    """Determine where a patch is applied based on lever and apply_mode.

    Levers 4-6 are always ``genie_config`` (Genie Agent native structures).
    Levers 1-3 are governed by ``apply_mode``.
    """
    if lever in (4, 5, 6):
        return "genie_config"
    return apply_mode


def _extract_instruction_default_filters(metadata_snapshot: dict) -> list[dict]:
    """Parse Genie Agent instructions for default filter rules."""
    from genie_space_optimizer.optimization.applier import _get_general_instructions

    instructions = _get_general_instructions(metadata_snapshot)
    if not instructions:
        return []

    filters: list[dict] = []
    patterns = [
        re.compile(r"(?:always|by default|default(?:s)? to)\s+(?:filter|use|apply|set)\s+.*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
        re.compile(r"(\w+)\s*=\s*['\"]?(\w+)['\"]?\s+(?:by default|unless|is the default)", re.IGNORECASE),
        re.compile(r"(?:unless\s+(?:explicitly|specifically)\s+(?:asked|requested|stated)\s+otherwise).*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
    ]
    for line in instructions.split("\n"):
        line = line.strip()
        if not line:
            continue
        for pattern in patterns:
            match = pattern.search(line)
            if match:
                filters.append({
                    "column": match.group(1).lower(),
                    "value": match.group(2),
                    "pattern": line[:200],
                })

    instr = metadata_snapshot.get("instructions", {})
    sql_snippets = instr.get("sql_snippets", {}) if isinstance(instr, dict) else {}
    for item in sql_snippets.get("filters", []):
        sql_raw = item.get("sql", "")
        sql = "".join(str(value) for value in sql_raw).strip() if isinstance(sql_raw, list) else str(sql_raw).strip()
        match = re.match(r"(\w+)\s*=\s*['\"]?(\w+)", sql)
        if match:
            filters.append({
                "column": match.group(1).lower(),
                "value": match.group(2),
                "pattern": f"sql_snippet: {sql}",
            })
    return filters

def _detect_instruction_contradictions(
    original_sections: dict[str, list[str]],
    proposed_sections: dict[str, str | list[str]],
) -> list[dict]:
    """Detect contradictions between user-authored and optimizer-proposed instruction sections.

    Compares filter polarity (always/default vs never/only-when-explicit) for
    the same column across original and proposed sections.  Returns a list of
    contradiction dicts with ``section``, ``original_rule``, ``proposed_line``,
    and ``contradiction_type``.

    Only flags clear inversions to avoid false positives on nuanced rewording.
    """
    _ALWAYS_PATTERNS = [
        re.compile(r"(?:always|by default|default(?:s)?\s+(?:to|filter))\s+.*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
        re.compile(r"default\s+filter[:\s]+(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
    ]
    _NEVER_PATTERNS = [
        re.compile(r"(?:never|do\s+not|don'?t)\s+(?:apply|add|filter|use|include)\s+.*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
        re.compile(r"only\s+(?:apply|add|filter|use)\s+.*?(\w+)\s*=\s*['\"]?(\w+)['\"]?\s+when\s+.*?(?:explicitly|specifically)", re.IGNORECASE),
        re.compile(r"(\w+)\s*=\s*['\"]?(\w+)['\"]?\s+only\s+when\s+.*?(?:explicitly|specifically)", re.IGNORECASE),
        re.compile(r"absolutely\s+never\s+(?:apply|add|filter|use).*?(\w+)\s*=\s*['\"]?(\w+)", re.IGNORECASE),
    ]

    def _extract_filter_rules(sections: dict, patterns: list, polarity: str) -> list[dict]:
        rules: list[dict] = []
        for section_name, lines in sections.items():
            if isinstance(lines, str):
                lines = [lines]
            for line in lines:
                if not isinstance(line, str):
                    continue
                for pat in patterns:
                    match = pat.search(line)
                    if match:
                        rules.append({
                            "column": match.group(1).lower(),
                            "value": match.group(2).lower(),
                            "polarity": polarity,
                            "section": section_name,
                            "line": line.strip(),
                        })
        return rules

    original_always = _extract_filter_rules(original_sections, _ALWAYS_PATTERNS, "always")
    proposed_never = _extract_filter_rules(
        {k: v if isinstance(v, list) else [v] for k, v in proposed_sections.items()},
        _NEVER_PATTERNS, "never",
    )
    original_never = _extract_filter_rules(original_sections, _NEVER_PATTERNS, "never")
    proposed_always = _extract_filter_rules(
        {k: v if isinstance(v, list) else [v] for k, v in proposed_sections.items()},
        _ALWAYS_PATTERNS, "always",
    )

    contradictions: list[dict] = []

    for orig in original_always:
        for prop in proposed_never:
            if orig["column"] == prop["column"]:
                contradictions.append({
                    "section": prop["section"],
                    "original_rule": orig["line"],
                    "proposed_line": prop["line"],
                    "contradiction_type": "filter_inversion",
                    "detail": f"Original says always/default {orig['column']}={orig['value']}, "
                              f"proposed says never/only-explicit",
                })

    for orig in original_never:
        for prop in proposed_always:
            if orig["column"] == prop["column"]:
                contradictions.append({
                    "section": prop["section"],
                    "original_rule": orig["line"],
                    "proposed_line": prop["line"],
                    "contradiction_type": "filter_inversion",
                    "detail": f"Original says never {orig['column']}={orig['value']}, "
                              f"proposed says always/default",
                })

    return contradictions

def _build_space_schema_context(metadata_snapshot: dict) -> dict[str, str]:
    """Build context strings for tables, metric views, and instructions."""
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", [])
    mvs = ds.get("metric_views", [])

    def _str_field(val: object) -> str:
        if isinstance(val, list):
            return " ".join(str(s) for s in val)
        return str(val) if val else ""

    table_lines: list[str] = []
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "")
        desc = _str_field(tbl.get("description", ""))
        cols = tbl.get("column_configs", tbl.get("columns", []))
        col_names = [
            c.get("column_name", c.get("name", ""))
            for c in cols if isinstance(c, dict)
        ]
        line = f"- {ident}"
        if desc:
            line += f": {desc[:120]}"
        if col_names:
            line += f"\n  Columns: {', '.join(col_names[:20])}"
            if len(col_names) > 20:
                line += f" (+{len(col_names) - 20} more)"
        table_lines.append(line)

    mv_lines: list[str] = []
    for mv in mvs:
        if not isinstance(mv, dict):
            continue
        ident = mv.get("identifier", "")
        desc = _str_field(mv.get("description", ""))
        cols = mv.get("column_configs", mv.get("columns", []))
        col_names = [
            c.get("column_name", c.get("name", ""))
            for c in cols if isinstance(c, dict)
        ]
        line = f"- {ident}"
        if desc:
            line += f": {desc[:120]}"
        if col_names:
            line += f"\n  Columns: {', '.join(col_names[:15])}"
        mv_lines.append(line)

    instr = metadata_snapshot.get("instructions", {})
    ti_list = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    instr_parts: list[str] = []
    for ti in ti_list:
        if not isinstance(ti, dict):
            continue
        raw = ti.get("content", "")
        if isinstance(raw, list):
            raw = "\n".join(str(s) for s in raw)
        if raw:
            instr_parts.append(str(raw)[:200])
    instr_text = "\n".join(instr_parts) or "(none)"

    return {
        "tables_context": "\n".join(table_lines) or "(none)",
        "metric_views_context": "\n".join(mv_lines) or "(none)",
        "instructions_context": instr_text,
    }

def _generate_space_description(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> str:
    """Generate a structured description for a Genie Agent from its schema.

    Returns the description text, or ``""`` on failure.
    """
    ctx = _build_space_schema_context(metadata_snapshot)
    format_kwargs = _truncate_to_budget(
        ctx, SPACE_DESCRIPTION_PROMPT,
        priority_keys=["tables_context"],
    )
    prompt = format_mlflow_template(SPACE_DESCRIPTION_PROMPT, **format_kwargs)
    system_msg = "You generate structured descriptions for Databricks Genie Agents."

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="generate_space_description",
            max_tokens=2048,
        )
        text = re.sub(r"```[a-z]*\n?", "", text).strip().rstrip("`")
        if len(text) < 30:
            logger.warning("Space description generation: result too short (%d chars)", len(text))
            return ""
        logger.info("Space description generation: produced %d chars", len(text))
        return text
    except Exception:
        logger.warning("Space description generation: LLM call failed", exc_info=True)
        return ""

_MARKDOWN_RESIDUE_RE = re.compile(
    r'(?m)'
    r'(?:^```[a-z]*\s*$)'                     # fenced code blocks
    r'|(?:^---+\s*$)'                         # horizontal rules
    r'|(?:^\*\*\*+\s*$)'
    r'|(?:^___+\s*$)'
    r'|(?:^#{1,6}\s+\S)'                      # leading ``## HEADER``
    r'|(?:\*\*[^*]+\*\*)'                     # bold
    r'|(?:`[^`]+`)'                           # inline backticks
    r'|(?:\[[^\]]+\]\([^)]+\))'               # markdown links
    r'|(?:\n{3,})'                            # excess blank lines
)

def _is_already_canonical_plaintext(text: str) -> bool:
    """Phase 3.3: cheap idempotency check for the sanitizer.

    Returns True if *text* contains no Markdown residue that
    :func:`_sanitize_plaintext_instructions` would otherwise touch. We
    skip the regex pipeline in that case so a second pass over already-
    canonical input doesn't generate spurious diffs (re-stripping
    backticks, re-flowing whitespace) on every iteration.
    """
    if not text:
        return True
    return _MARKDOWN_RESIDUE_RE.search(text) is None

def _sanitize_plaintext_instructions(text: str) -> str:
    """Strip residual Markdown from instruction text for plain-text display.

    Phase 3.3: idempotent — if the input already has no Markdown
    residue, the function returns the (stripped) text unchanged
    instead of running the regex pipeline. This eliminates the
    "every iteration produces a diff for unchanged content" symptom
    from the iter-1 lever loop.
    """
    if _is_already_canonical_plaintext(text):
        return text.strip() if text else ""
    text = re.sub(r'^```[a-z]*\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^---+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\*\*\*+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^___+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(
        r'^#{1,6}\s+(.+)$',
        lambda m: m.group(1).upper().rstrip() + ':',
        text,
        flags=re.MULTILINE,
    )
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

_SECTION_HEADER_RE = re.compile(
    r'^([A-Z][A-Z /]+):[ \t]*$', re.MULTILINE,
)

_KNOWN_SECTIONS = set(INSTRUCTION_SECTION_ORDER)

def _parse_sections(text: str) -> tuple[dict[str, list[str]], list[str]]:
    """Parse structured plain-text into {SECTION_HEADER: [lines]} and preamble lines."""
    lines = text.splitlines()
    sections: dict[str, list[str]] = {}
    preamble: list[str] = []
    current: str | None = None

    for line in lines:
        m = _SECTION_HEADER_RE.match(line)
        if m and m.group(1) in _KNOWN_SECTIONS:
            current = m.group(1)
            if current not in sections:
                sections[current] = []
        elif current is not None:
            sections[current].append(line)
        else:
            preamble.append(line)

    for key in sections:
        while sections[key] and not sections[key][-1].strip():
            sections[key].pop()

    return sections, preamble

def _merge_structured_instructions(
    existing: str,
    contributions: list[str],
    global_guidance: str = "",
) -> str:
    """Merge instruction fragments into a single structured document.

    Parses ``existing`` and each contribution by ALL-CAPS section header,
    deduplicates bullets within each section, then reassembles in
    ``INSTRUCTION_SECTION_ORDER``.  Unrecognized content goes into CONSTRAINTS.
    """
    merged: dict[str, list[str]] = {s: [] for s in INSTRUCTION_SECTION_ORDER}

    existing_sections, existing_preamble = _parse_sections(
        _sanitize_plaintext_instructions(existing) if existing else ""
    )
    for section, lines in existing_sections.items():
        if section in merged:
            merged[section].extend(lines)

    if existing_preamble:
        non_blank = [l for l in existing_preamble if l.strip()]
        if non_blank:
            if not merged["PURPOSE"]:
                merged["PURPOSE"].extend(non_blank)
            else:
                merged["CONSTRAINTS"].extend(non_blank)

    for fragment in contributions:
        sanitized = _sanitize_plaintext_instructions(fragment) if fragment else ""
        frag_sections, frag_preamble = _parse_sections(sanitized)
        for section, lines in frag_sections.items():
            if section in merged:
                merged[section].extend(lines)
            else:
                merged["CONSTRAINTS"].extend(lines)
        if frag_preamble:
            non_blank = [l for l in frag_preamble if l.strip()]
            if non_blank:
                merged["CONSTRAINTS"].extend(non_blank)

    if global_guidance:
        sanitized_g = _sanitize_plaintext_instructions(global_guidance)
        g_sections, g_preamble = _parse_sections(sanitized_g)
        for section, lines in g_sections.items():
            if section in merged:
                merged[section].extend(lines)
        if g_preamble:
            non_blank = [l for l in g_preamble if l.strip()]
            if non_blank:
                merged["CONSTRAINTS"].extend(non_blank)

    for section in merged:
        seen: set[str] = set()
        deduped: list[str] = []
        for line in merged[section]:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped not in seen:
                seen.add(stripped)
                deduped.append(line)
        merged[section] = deduped

    parts: list[str] = []
    for section in INSTRUCTION_SECTION_ORDER:
        if merged[section]:
            parts.append(f"{section}:")
            for line in merged[section]:
                stripped = line.strip()
                if not stripped:
                    continue
                if not stripped.startswith("- "):
                    stripped = f"- {stripped}"
                parts.append(stripped)
            parts.append("")

    result = "\n".join(parts).strip()
    return _sanitize_plaintext_instructions(result)

def normalize_instructions(text: str) -> str:
    """Parse text into canonical structured sections and reassemble."""
    return _merge_structured_instructions(existing=text, contributions=[], global_guidance="")

_pre_structure_cache: dict[int, dict[str, list[str]]] = {}

def _pre_structure_instructions(
    raw: str,
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict[str, list[str]]:
    """Heuristic fallback that groups free-form prose into legacy sections.

    Historical note: this function used to drive an LLM round-trip via
    ``INSTRUCTION_RESTRUCTURE_PROMPT`` to classify prose into the legacy
    12-section ALL-CAPS vocabulary. That prompt and the classifier were
    deleted as part of the 5-section schema migration — the prose rule
    miner (:func:`_convert_instructions_to_sql_expressions`) now performs
    canonical grouping as part of its rewrite step, which subsumes this
    concern for spaces touched by proactive enrichment.

    What remains is a heuristic fallback used by the in-loop lever
    machinery (:func:`_ensure_structured`) when a space's prose still
    lacks recognised section headers. The fallback preserves content
    rather than inventing structure; the miner re-homes it on the next
    optimisation run.
    """
    if not raw or not raw.strip():
        return {}

    cache_key = hash(raw)
    if cache_key in _pre_structure_cache:
        return _pre_structure_cache[cache_key]

    fallback_text = _merge_structured_instructions(
        existing=raw, contributions=[], global_guidance="",
    )
    sections, preamble = _parse_sections(fallback_text)
    if preamble:
        non_blank = [ln for ln in preamble if ln.strip()]
        if non_blank:
            target = "PURPOSE" if "PURPOSE" not in sections else "CONSTRAINTS"
            sections.setdefault(target, []).extend(non_blank)
    if not sections:
        # Nothing parseable — dump under CONSTRAINTS so content is
        # preserved, not silently dropped. The miner will promote or
        # re-home it on the next run.
        sections = {
            "CONSTRAINTS": [
                ln.strip() for ln in raw.splitlines() if ln.strip()
            ],
        }
    result: dict[str, list[str]] = {
        k: [ln for ln in v if ln.strip()]
        for k, v in sections.items()
    }
    _pre_structure_cache[cache_key] = result
    return result

def _ensure_structured(
    current_instructions: str,
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict[str, list[str]]:
    """Return existing instructions as a structured section dict.

    If already structured, parses directly.  If unstructured, calls
    ``_pre_structure_instructions`` to classify via LLM.
    """
    if not current_instructions or not current_instructions.strip():
        return {}

    sanitized = _sanitize_plaintext_instructions(current_instructions)
    sections, preamble = _parse_sections(sanitized)

    if sections and not preamble:
        return {k: [ln for ln in v if ln.strip()] for k, v in sections.items()}

    if sections and preamble:
        non_blank = [ln for ln in preamble if ln.strip()]
        if non_blank:
            target = "PURPOSE" if "PURPOSE" not in sections else "CONSTRAINTS"
            sections.setdefault(target, []).extend(non_blank)
        return {k: [ln for ln in v if ln.strip()] for k, v in sections.items()}

    return _pre_structure_instructions(current_instructions, metadata_snapshot, w=w)
