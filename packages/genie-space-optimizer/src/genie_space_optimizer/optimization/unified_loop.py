"""Native-only GSO optimization loop.

This module is the cutover path for the v2 optimizer: every iteration is
evaluated by the Databricks Genie Benchmark API, the LLM proposes ordinary
Patch DSL entries, and the existing applier owns config mutation plus rollback.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
from typing import Any

from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.optimization.applier import (
    apply_patch_set,
    classify_risk,
    rollback,
)
from genie_space_optimizer.optimization.eval_runner import (
    FULL,
    OfficialBenchmarkRunner,
    build_eval_output_from_official,
    resolve_space_benchmark_qids,
)
from genie_space_optimizer.optimization.evaluation import _extract_json
from genie_space_optimizer.optimization.leakage import BenchmarkCorpus
from genie_space_optimizer.optimization.llm_client import call_llm
from genie_space_optimizer.optimization.state import (
    mark_champion_iteration,
    mark_iteration_rolled_back,
    mark_patches_rolled_back,
    update_iteration_loop_state,
    update_run_status,
    write_iteration,
    write_patch,
)

logger = logging.getLogger(__name__)

_ALLOWED_PATCH_TYPES: frozenset[str] = frozenset(
    {
        "update_description",
        "update_column_description",
        "add_column_synonym",
        "add_instruction",
        "update_instruction_section",
        "add_join_spec",
        "update_join_spec",
        "add_sql_snippet_measure",
        "add_sql_snippet_filter",
        "add_sql_snippet_expression",
    }
)


def target_accuracy_percent(value: float) -> float:
    """Normalize a job target to the 0-100 scale used by eval rows."""
    return value * 100.0 if value <= 1.0 else value


def _parsed_space(config: dict[str, Any]) -> dict[str, Any]:
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return copy.deepcopy(parsed)
    return copy.deepcopy(config)


def _stable_config_id(config: dict[str, Any]) -> str:
    try:
        payload = json.dumps(config, sort_keys=True, default=str)
    except (TypeError, ValueError):
        payload = str(config)
    return "cfgsha:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _metric(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        result = float(value)
    except (TypeError, ValueError):
        return default
    return default if result != result else result


def _failure_rows(eval_result: dict[str, Any], *, limit: int = 12) -> list[dict[str, Any]]:
    rows = eval_result.get("rows")
    if not isinstance(rows, list):
        return []

    failures: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        assessment = str(row.get("assessment") or "").upper()
        if assessment == "GOOD":
            continue
        genie_eval = row.get("genie_equivalent_eval")
        reasons = []
        if isinstance(genie_eval, dict):
            reasons = genie_eval.get("assessment_reasons") or []
        failures.append(
            {
                "question_id": row.get("question_id") or row.get("inputs/question_id"),
                "question": row.get("question") or row.get("inputs/question"),
                "assessment": assessment or "NEEDS_REVIEW",
                "assessment_reasons": reasons,
                "generated_sql": row.get("generated_sql") or row.get("outputs/response"),
                "expected_sql": row.get("expected_sql") or row.get("inputs/expected_response"),
                "expected_asset_type": row.get("expected_asset_type"),
                "actual_asset_type": row.get("actual_asset_type"),
            }
        )
        if len(failures) >= limit:
            break
    return failures


def _config_prompt_projection(config: dict[str, Any]) -> dict[str, Any]:
    """Project the space config to the parts useful for patch selection."""
    parsed = config.get("_parsed_space") if isinstance(config.get("_parsed_space"), dict) else config
    if not isinstance(parsed, dict):
        return {}
    data_sources = parsed.get("data_sources") if isinstance(parsed.get("data_sources"), dict) else {}
    instructions = parsed.get("instructions") if isinstance(parsed.get("instructions"), dict) else {}

    def _table_projection(table: dict[str, Any]) -> dict[str, Any]:
        columns = []
        for col in table.get("column_configs") or table.get("columns") or []:
            if not isinstance(col, dict):
                continue
            columns.append(
                {
                    "column_name": col.get("column_name") or col.get("name"),
                    "description": col.get("description"),
                    "synonyms": col.get("synonyms") or [],
                    "data_type": col.get("data_type") or col.get("type"),
                }
            )
        return {
            "identifier": table.get("identifier") or table.get("name"),
            "description": table.get("description"),
            "columns": columns[:80],
        }

    text_instructions = []
    for item in instructions.get("text_instructions") or []:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if isinstance(content, list):
            content_text = "\n".join(str(part) for part in content)
        else:
            content_text = str(content or "")
        text_instructions.append(content_text[:2500])

    return {
        "title": parsed.get("title") or parsed.get("name"),
        "description": parsed.get("description"),
        "tables": [
            _table_projection(t)
            for t in (data_sources.get("tables") or [])[:20]
            if isinstance(t, dict)
        ],
        "metric_views": [
            _table_projection(t)
            for t in (data_sources.get("metric_views") or [])[:20]
            if isinstance(t, dict)
        ],
        "join_specs": (instructions.get("join_specs") or [])[:40],
        "sql_snippets": instructions.get("sql_snippets") or {},
        "text_instructions": text_instructions,
    }


def _compact_json(value: Any, *, max_chars: int = 30000) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str, indent=2)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n...<truncated>"


def _native_eval(
    w: Any,
    *,
    space_id: str,
    benchmarks: list[dict[str, Any]],
    iteration: int,
    model_id: str | None = None,
) -> dict[str, Any]:
    qids = resolve_space_benchmark_qids(w, space_id, benchmarks)
    if not qids:
        raise RuntimeError(
            "Native Genie Benchmark API evaluation requires every benchmark to "
            "resolve to a live space benchmark question id. Task benchmark_qc_and_repair "
            "must push benchmarks before optimize runs."
        )
    runner = OfficialBenchmarkRunner(w)
    result = runner.run(space_id, qids, eval_scope=FULL)
    return build_eval_output_from_official(
        result,
        iteration=iteration,
        eval_scope=FULL,
        model_id=model_id,
    )


def _llm_messages(
    *,
    allowed_levers: list[int],
    current_config: dict[str, Any],
    eval_result: dict[str, Any],
    reflections: list[dict[str, Any]],
) -> list[dict[str, str]]:
    failures = _failure_rows(eval_result)
    system = (
        "You are optimizing a Databricks Genie Space. Return only JSON. "
        "You may propose ordinary Patch DSL entries; enrichment is not a separate mode. "
        "Use expected_sql and generated_sql only as diagnostic evidence. Do not copy "
        "benchmark question text, expected SQL, or generated SQL into Genie-visible "
        "instructions, examples, descriptions, or snippets. Prefer the smallest patch "
        "that can improve the failing benchmark pattern."
    )
    user = {
        "allowed_levers": allowed_levers,
        "allowed_patch_types": sorted(_ALLOWED_PATCH_TYPES),
        "response_schema": {
            "lever": "integer lever id",
            "rationale": "short reason",
            "patches": [
                {
                    "type": "update_description | update_column_description | add_instruction | update_instruction_section | add_join_spec | ...",
                    "lever": "same integer lever id",
                    "target": "table identifier for table-level patches",
                    "table": "table identifier for column patches",
                    "column": "column name for column patches",
                    "new_text": "natural-language patch text",
                    "structured_sections": "optional dict for description patches",
                    "join_spec": "optional Genie join spec object for join patches",
                }
            ],
        },
        "patch_rules": [
            "Use update_description for table descriptions.",
            "Use update_column_description with table and column for column descriptions.",
            "Use update_instruction_section for narrow instruction changes; use Markdown ## sections when adding text.",
            "Use add_join_spec only when the relationship is clear and include a relationship annotation.",
            "Do not propose add_example_sql or update_example_sql from benchmark SQL.",
            "Do not include raw SELECT statements in text instructions.",
        ],
        "previous_eval": {
            "accuracy": eval_result.get("overall_accuracy"),
            "total_questions": eval_result.get("total_questions"),
            "correct_count": eval_result.get("correct_count"),
            "failure_count": len(failures),
            "failures": failures,
        },
        "current_space_config": _config_prompt_projection(current_config),
        "recent_reflections": reflections[-2:],
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": _compact_json(user)},
    ]


def _normalize_llm_patches(raw: Any, *, allowed_levers: list[int]) -> tuple[int | None, str, list[dict[str, Any]]]:
    if isinstance(raw, list):
        parsed = {"patches": raw}
    elif isinstance(raw, dict):
        parsed = raw
    else:
        return None, "LLM returned no parseable JSON object", []

    try:
        proposal_lever = int(parsed.get("lever")) if parsed.get("lever") is not None else None
    except (TypeError, ValueError):
        proposal_lever = None
    if proposal_lever not in allowed_levers:
        proposal_lever = allowed_levers[0] if allowed_levers else None

    rationale = str(parsed.get("rationale") or parsed.get("reason") or "").strip()
    patches_raw = parsed.get("patches")
    if patches_raw is None and isinstance(parsed.get("patch"), dict):
        patches_raw = [parsed["patch"]]
    if not isinstance(patches_raw, list):
        return proposal_lever, rationale or "LLM returned no patches", []

    patches: list[dict[str, Any]] = []
    for idx, item in enumerate(patches_raw):
        if not isinstance(item, dict):
            continue
        patch = dict(item)
        ptype = str(patch.get("type") or patch.get("patch_type") or "").strip()
        if ptype not in _ALLOWED_PATCH_TYPES:
            logger.info("Dropping unsupported LLM patch type %r", ptype)
            continue
        patch["type"] = ptype
        try:
            patch_lever = int(patch.get("lever")) if patch.get("lever") is not None else proposal_lever
        except (TypeError, ValueError):
            patch_lever = proposal_lever
        if patch_lever not in allowed_levers:
            patch_lever = proposal_lever
        if patch_lever is not None:
            patch["lever"] = patch_lever
        patch.setdefault("risk_level", classify_risk(ptype))
        patch.setdefault("proposal_id", f"unified-{idx + 1}")
        patch.setdefault("source_proposal_id", patch["proposal_id"])
        patch.setdefault("patch_family", "unified_llm")
        if rationale:
            patch.setdefault("rationale", rationale)
        patches.append(patch)
    return proposal_lever, rationale, patches


def propose_patches(
    w: Any,
    *,
    allowed_levers: list[int],
    current_config: dict[str, Any],
    eval_result: dict[str, Any],
    reflections: list[dict[str, Any]],
) -> tuple[int | None, str, list[dict[str, Any]], str]:
    text, _response = call_llm(
        w,
        messages=_llm_messages(
            allowed_levers=allowed_levers,
            current_config=current_config,
            eval_result=eval_result,
            reflections=reflections,
        ),
        max_tokens=4096,
    )
    parsed = _extract_json(text)
    lever, rationale, patches = _normalize_llm_patches(
        parsed,
        allowed_levers=allowed_levers,
    )
    return lever, rationale, patches, text


def _patch_record(entry: dict[str, Any], *, lever: int, apply_mode: str) -> dict[str, Any]:
    patch = entry.get("patch", {})
    action = entry.get("action", {})
    applied_type = entry.get("applied_patch_type") or patch.get(
        "type",
        action.get("action_type", "unknown"),
    )
    proposal_type = (
        entry.get("proposal_patch_type")
        or patch.get("_proposal_patch_type")
        or patch.get("type", applied_type)
    )
    return {
        "patch_type": proposal_type,
        "scope": apply_mode if lever <= 3 else "genie_config",
        "risk_level": action.get("risk_level", patch.get("risk_level", "medium")),
        "target_object": action.get("target", patch.get("target", "")),
        "patch": patch,
        "command": action.get("command"),
        "rollback": action.get("rollback_command"),
        "proposal_id": patch.get("source_proposal_id") or patch.get("proposal_id", ""),
        "applied_patch_type": applied_type,
        "applied_patch_detail": entry.get("applied_patch_detail"),
        "patch_family": patch.get("patch_family"),
        "target_qids": patch.get("target_qids", []),
        "rationale": patch.get("rationale"),
    }


def _loop_state(
    *,
    attempt_no: int,
    attempt_mode: str,
    best_accuracy: float,
    current_hypothesis: dict[str, Any] | None = None,
    decision: str | None = None,
    decision_reason: str | None = None,
    surgical_attempts_used: int,
    target_accuracy: float,
    max_attempts: int,
    terminal_reason: str | None = None,
    config: dict[str, Any] | None = None,
    do_not_repeat: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "attempt_no": attempt_no,
        "attempt_mode": attempt_mode,
        "best_accuracy": best_accuracy,
        "best_config_version_id": _stable_config_id(config or {}),
        "current_hypothesis": current_hypothesis,
        "do_not_repeat": do_not_repeat or [],
        "terminal_reason": terminal_reason,
        "decision": decision,
        "decision_reason": decision_reason,
        "surgical_attempts_used": surgical_attempts_used,
        "next_hypothesis": None,
        "target_accuracy": target_accuracy,
        "max_attempts": max_attempts,
    }


def _stamp_terminal(
    spark: Any,
    *,
    run_id: str,
    iteration: int,
    reason: str,
    best_accuracy: float,
    surgical_attempts_used: int,
    target_accuracy: float,
    max_attempts: int,
    config: dict[str, Any],
    catalog: str,
    schema: str,
) -> None:
    update_iteration_loop_state(
        spark,
        run_id,
        iteration,
        catalog=catalog,
        schema=schema,
        eval_scope=FULL,
        loop_state=_loop_state(
            attempt_no=iteration,
            attempt_mode="baseline" if iteration == 0 else "llm_patch",
            best_accuracy=best_accuracy,
            decision="terminal",
            decision_reason=reason,
            surgical_attempts_used=surgical_attempts_used,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            terminal_reason=reason,
            config=config,
        ),
    )
    mark_champion_iteration(
        spark,
        run_id,
        iteration,
        catalog=catalog,
        schema=schema,
        eval_scope=FULL,
    )
    update_run_status(
        spark,
        run_id,
        catalog,
        schema,
        best_iteration=iteration,
        best_accuracy=best_accuracy,
        convergence_reason=reason,
    )


def run_unified_optimization_loop(
    w: Any,
    spark: Any,
    *,
    run_id: str,
    space_id: str,
    domain: str,
    benchmarks: list[dict[str, Any]],
    catalog: str,
    schema: str,
    levers: list[int],
    max_attempts: int,
    target_accuracy: float,
    apply_mode: str = "genie_config",
    triggered_by: str = "",
    human_corrections: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run baseline eval plus bounded LLM patch attempts."""
    _ = (domain, triggered_by, human_corrections)
    target_accuracy = target_accuracy_percent(float(target_accuracy))
    allowed_levers = [int(l) for l in levers if int(l) in {1, 2, 3, 4, 5, 6}]
    if not allowed_levers:
        allowed_levers = [1, 2, 3, 4, 5, 6]
    max_attempts = max(0, int(max_attempts))

    current_config = _parsed_space(fetch_space_config(w, space_id))
    benchmark_corpus = BenchmarkCorpus.from_benchmarks(benchmarks)

    baseline_eval = _native_eval(
        w,
        space_id=space_id,
        benchmarks=benchmarks,
        iteration=0,
    )
    best_accuracy = _metric(baseline_eval.get("overall_accuracy"))
    best_iteration = 0
    best_eval = baseline_eval
    attempts_used = 0
    accepted = 0
    rolled_back = 0
    reflections: list[dict[str, Any]] = []

    write_iteration(
        spark,
        run_id,
        0,
        baseline_eval,
        catalog=catalog,
        schema=schema,
        lever=None,
        eval_scope=FULL,
        reflection_json={"phase": "baseline"},
        config_snapshot=current_config,
        loop_state=_loop_state(
            attempt_no=0,
            attempt_mode="baseline",
            best_accuracy=best_accuracy,
            decision="accept",
            decision_reason="baseline",
            surgical_attempts_used=0,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            config=current_config,
        ),
    )
    update_run_status(
        spark,
        run_id,
        catalog,
        schema,
        best_iteration=0,
        best_accuracy=best_accuracy,
    )

    if baseline_eval.get("eval_run_failed"):
        terminal_reason = "EVAL_INVALID"
        _stamp_terminal(
            spark,
            run_id=run_id,
            iteration=best_iteration,
            reason=terminal_reason,
            best_accuracy=best_accuracy,
            surgical_attempts_used=attempts_used,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            config=current_config,
            catalog=catalog,
            schema=schema,
        )
        return {
            "run_id": run_id,
            "accuracy": best_accuracy,
            "best_iteration": best_iteration,
            "iteration_counter": 0,
            "terminal_reason": terminal_reason,
            "surgical_attempts_used": attempts_used,
            "levers_accepted": [],
            "levers_rolled_back": [],
        }

    if best_accuracy >= target_accuracy:
        terminal_reason = "TARGET_REACHED"
        _stamp_terminal(
            spark,
            run_id=run_id,
            iteration=best_iteration,
            reason=terminal_reason,
            best_accuracy=best_accuracy,
            surgical_attempts_used=attempts_used,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            config=current_config,
            catalog=catalog,
            schema=schema,
        )
        return {
            "run_id": run_id,
            "accuracy": best_accuracy,
            "best_iteration": best_iteration,
            "iteration_counter": 0,
            "terminal_reason": terminal_reason,
            "surgical_attempts_used": attempts_used,
            "levers_accepted": [],
            "levers_rolled_back": [],
        }

    levers_accepted: list[int] = []
    levers_rolled_back: list[int] = []
    terminal_reason: str | None = None

    for iteration in range(1, max_attempts + 1):
        attempts_used = iteration
        lever, rationale, patches, raw_response = propose_patches(
            w,
            allowed_levers=allowed_levers,
            current_config=current_config,
            eval_result=best_eval,
            reflections=reflections,
        )
        hypothesis = {
            "lever": lever,
            "rationale": rationale,
            "patch_count": len(patches),
            "raw_response_preview": raw_response[:1000],
        }
        if not patches or lever is None:
            terminal_reason = "NO_NEW_HYPOTHESIS"
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": terminal_reason,
                    "rationale": rationale,
                }
            )
            break

        apply_log = apply_patch_set(
            w,
            space_id,
            patches,
            current_config,
            apply_mode=apply_mode,
            benchmark_corpus=benchmark_corpus,
        )
        applied_entries = apply_log.get("applied") or []
        for idx, entry in enumerate(applied_entries):
            patch_lever = int(entry.get("patch", {}).get("lever", lever) or lever)
            write_patch(
                spark,
                run_id,
                iteration,
                patch_lever,
                idx,
                _patch_record(entry, lever=patch_lever, apply_mode=apply_mode),
                catalog,
                schema,
            )

        if not apply_log.get("patch_deployed") or not applied_entries:
            terminal_reason = "NO_NEW_HYPOTHESIS"
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": terminal_reason,
                    "rationale": rationale or apply_log.get("patch_error"),
                    "dropped_patches": apply_log.get("dropped_patches") or [],
                }
            )
            break

        candidate_config = copy.deepcopy(apply_log.get("post_snapshot") or current_config)
        candidate_eval = _native_eval(
            w,
            space_id=space_id,
            benchmarks=benchmarks,
            iteration=iteration,
        )
        candidate_accuracy = _metric(candidate_eval.get("overall_accuracy"))

        write_iteration(
            spark,
            run_id,
            iteration,
            candidate_eval,
            catalog=catalog,
            schema=schema,
            lever=lever,
            eval_scope=FULL,
            reflection_json={
                "rationale": rationale,
                "patch_count": len(patches),
                "applied_count": len(applied_entries),
            },
            config_snapshot=candidate_config,
            loop_state=_loop_state(
                attempt_no=iteration,
                attempt_mode="llm_patch",
                best_accuracy=best_accuracy,
                current_hypothesis=hypothesis,
                decision="pending",
                surgical_attempts_used=attempts_used,
                target_accuracy=target_accuracy,
                max_attempts=max_attempts,
                config=candidate_config,
                do_not_repeat=reflections[-2:],
            ),
        )

        if candidate_eval.get("eval_run_failed"):
            rollback(apply_log, w, space_id, metadata_snapshot=current_config)
            mark_patches_rolled_back(
                spark,
                run_id,
                iteration,
                "candidate eval invalid",
                catalog,
                schema,
            )
            terminal_reason = "EVAL_INVALID"
            rolled_back += 1
            levers_rolled_back.append(int(lever))
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": "rolled_back",
                    "reason": terminal_reason,
                    "accuracy": candidate_accuracy,
                }
            )
            break

        if candidate_accuracy > best_accuracy:
            best_accuracy = candidate_accuracy
            best_iteration = iteration
            best_eval = candidate_eval
            current_config = candidate_config
            accepted += 1
            levers_accepted.append(int(lever))
            decision_reason = (
                f"accuracy improved to {candidate_accuracy:.2f} "
                f"from previous best"
            )
            update_iteration_loop_state(
                spark,
                run_id,
                iteration,
                catalog=catalog,
                schema=schema,
                eval_scope=FULL,
                loop_state=_loop_state(
                    attempt_no=iteration,
                    attempt_mode="llm_patch",
                    best_accuracy=best_accuracy,
                    current_hypothesis=hypothesis,
                    decision="accept",
                    decision_reason=decision_reason,
                    surgical_attempts_used=attempts_used,
                    target_accuracy=target_accuracy,
                    max_attempts=max_attempts,
                    config=current_config,
                    do_not_repeat=reflections[-2:],
                ),
            )
            update_run_status(
                spark,
                run_id,
                catalog,
                schema,
                best_iteration=best_iteration,
                best_accuracy=best_accuracy,
            )
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": "accepted",
                    "lever": lever,
                    "accuracy": candidate_accuracy,
                    "rationale": rationale,
                }
            )
            if best_accuracy >= target_accuracy:
                terminal_reason = "TARGET_REACHED"
                break
            continue

        reason = (
            f"candidate accuracy {candidate_accuracy:.2f} did not improve "
            f"on best {best_accuracy:.2f}"
        )
        rollback(apply_log, w, space_id, metadata_snapshot=current_config)
        mark_patches_rolled_back(spark, run_id, iteration, reason, catalog, schema)
        mark_iteration_rolled_back(
            spark,
            run_id,
            iteration,
            catalog=catalog,
            schema=schema,
            eval_scope=FULL,
            reason=reason,
        )
        update_iteration_loop_state(
            spark,
            run_id,
            iteration,
            catalog=catalog,
            schema=schema,
            eval_scope=FULL,
            loop_state=_loop_state(
                attempt_no=iteration,
                attempt_mode="llm_patch",
                best_accuracy=best_accuracy,
                current_hypothesis=hypothesis,
                decision="reject",
                decision_reason=reason,
                surgical_attempts_used=attempts_used,
                target_accuracy=target_accuracy,
                max_attempts=max_attempts,
                config=current_config,
                do_not_repeat=reflections[-2:],
            ),
        )
        rolled_back += 1
        levers_rolled_back.append(int(lever))
        reflections.append(
            {
                "iteration": iteration,
                "decision": "rolled_back",
                "lever": lever,
                "accuracy": candidate_accuracy,
                "reason": reason,
                "rationale": rationale,
            }
        )

    if terminal_reason is None:
        terminal_reason = "MAX_ATTEMPTS"

    _stamp_terminal(
        spark,
        run_id=run_id,
        iteration=best_iteration,
        reason=terminal_reason,
        best_accuracy=best_accuracy,
        surgical_attempts_used=attempts_used,
        target_accuracy=target_accuracy,
        max_attempts=max_attempts,
        config=current_config,
        catalog=catalog,
        schema=schema,
    )

    return {
        "run_id": run_id,
        "accuracy": best_accuracy,
        "best_iteration": best_iteration,
        "iteration_counter": attempts_used,
        "terminal_reason": terminal_reason,
        "surgical_attempts_used": attempts_used,
        "levers_accepted": levers_accepted,
        "levers_rolled_back": levers_rolled_back,
        "accepted_attempts": accepted,
        "rolled_back_attempts": rolled_back,
        "reflections": reflections,
    }
