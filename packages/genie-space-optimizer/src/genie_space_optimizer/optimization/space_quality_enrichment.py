"""Narrow pre-baseline Genie Space quality enrichment.

This phase runs inside the Optimize task before iteration-0 benchmark
evaluation.  It is intentionally small and auditable: fix only basic,
low-risk curation gaps, record what changed, and never fail the run.
"""

from __future__ import annotations

import copy
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.common.config import (
    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS,
    PROPAGATION_WAIT_SECONDS,
)
from genie_space_optimizer.common.genie_client import (
    fetch_space_config,
    patch_space_config,
    update_space_description,
)
from genie_space_optimizer.iq_scan.context import build_space_quality_scan_context
from genie_space_optimizer.iq_scan.scoring import calculate_score
from genie_space_optimizer.optimization.applier import (
    _get_general_instructions,
    _set_general_instructions,
    auto_apply_prompt_matching,
    validate_instruction_text,
)
from genie_space_optimizer.optimization.state import (
    write_artifact,
    write_patch,
    write_stage,
)
from genie_space_optimizer.optimization.wide_schema import (
    _identifier_parts,
    normalize_component,
)

logger = logging.getLogger(__name__)

_STAGE = "SPACE_QUALITY_ENRICHMENT"
_TASK_KEY = "space_quality_enrichment"
_INSTRUCTION_SEED_THRESHOLD = 50
_MAX_SOURCE_NAMES = 6
_MAX_PROMPT_COLUMNS_PER_ASSET = 50
_PROMPT_MATCHING_CONTEXT_VERSION = 1


def build_prompt_matching_context(config: dict[str, Any]) -> dict[str, Any]:
    """Build a compact, non-sensitive handoff for prompt matching.

    Preflight profiles may contain sampled distinct values. The prompt matcher
    only needs row counts and cardinalities, so the artifact deliberately drops
    those values while retaining UC types, RLS verdicts, and asset semantics.
    """
    # ``_uc_columns`` is expected to be the active wide-schema projection.
    # Keep a defensive per-asset cap here because this function is also used by
    # legacy and replay paths that may attach an older, complete UC snapshot.
    uc_columns: list[dict[str, Any]] = []
    columns_per_asset: dict[tuple[str, str, str], int] = {}
    omitted_columns = 0
    for raw in config.get("_uc_columns") or []:
        if not isinstance(raw, dict):
            continue
        column_name = str(raw.get("column_name") or "").strip()
        table_name = str(raw.get("table_name") or "").strip()
        if not column_name or not table_name:
            continue
        asset_key = (
            str(raw.get("catalog_name") or "").strip().casefold(),
            str(raw.get("schema_name") or "").strip().casefold(),
            table_name.casefold(),
        )
        if columns_per_asset.get(asset_key, 0) >= _MAX_PROMPT_COLUMNS_PER_ASSET:
            omitted_columns += 1
            continue
        columns_per_asset[asset_key] = columns_per_asset.get(asset_key, 0) + 1
        projected_column = {
            "catalog_name": str(raw.get("catalog_name") or "").strip(),
            "schema_name": str(raw.get("schema_name") or "").strip(),
            "table_name": table_name,
            "column_name": column_name,
            "data_type": str(raw.get("data_type") or "").strip(),
        }
        if raw.get("stable_rank") is not None:
            projected_column["stable_rank"] = raw["stable_rank"]
        if raw.get("reason_codes"):
            projected_column["reason_codes"] = copy.deepcopy(raw["reason_codes"])
        if raw.get("profile_status") is not None:
            projected_column["profile_status"] = raw["profile_status"]
        uc_columns.append(projected_column)
    by_asset: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for column in uc_columns:
        key = (
            column["catalog_name"],
            column["schema_name"],
            column["table_name"],
        )
        by_asset.setdefault(key, []).append(column)
    for columns in by_asset.values():
        columns.sort(key=lambda column: (
            int(column.get("stable_rank") or 999999),
            column["column_name"],
        ))
    uc_columns = []
    offset = 0
    while True:
        added = False
        for key in sorted(by_asset):
            columns = by_asset[key]
            if offset < len(columns):
                uc_columns.append(columns[offset])
                added = True
        if not added:
            break
        offset += 1

    active_columns_by_asset: dict[tuple[str, str, str], set[str]] = {}
    for column in uc_columns:
        asset_key = (
            normalize_component(column.get("catalog_name")),
            normalize_component(column.get("schema_name")),
            normalize_component(column.get("table_name")),
        )
        column_name = normalize_component(column.get("column_name"))
        if all(asset_key) and column_name:
            active_columns_by_asset.setdefault(asset_key, set()).add(column_name)
    has_active_projection = isinstance(config.get("_uc_columns"), list)

    data_profile: dict[str, Any] = {}
    for identifier, raw_table in (config.get("_data_profile") or {}).items():
        if not isinstance(raw_table, dict):
            continue
        asset_key = _identifier_parts(identifier)
        allowed_columns = active_columns_by_asset.get(asset_key, set())
        columns: dict[str, Any] = {}
        for column_name, raw_column in (raw_table.get("columns") or {}).items():
            if not isinstance(raw_column, dict):
                continue
            if (
                has_active_projection
                and normalize_component(column_name) not in allowed_columns
            ):
                continue
            cardinality = raw_column.get("cardinality")
            if cardinality is None:
                continue
            columns[str(column_name)] = {"cardinality": cardinality}
        data_profile[str(identifier)] = {
            "row_count": raw_table.get("row_count", 0),
            "columns": columns,
        }

    rls_audit: dict[str, Any] = {}
    for identifier, raw_verdict in (config.get("_rls_audit") or {}).items():
        if not isinstance(raw_verdict, dict):
            continue
        verdict = str(raw_verdict.get("verdict") or "unknown")
        if verdict not in {"clean", "tainted", "unknown"}:
            verdict = "unknown"
        rls_audit[str(identifier)] = {"verdict": verdict}

    asset_semantics: dict[str, Any] = {}
    for identifier, raw_asset in (config.get("_asset_semantics") or {}).items():
        if not isinstance(raw_asset, dict):
            continue
        asset_semantics[str(identifier)] = {
            key: copy.deepcopy(raw_asset[key])
            for key in ("kind", "outcome", "short_name", "measures")
            if key in raw_asset
        }

    return {
        "version": _PROMPT_MATCHING_CONTEXT_VERSION,
        "uc_columns": uc_columns,
        "data_profile": data_profile,
        "rls_audit": rls_audit,
        "asset_semantics": asset_semantics,
        "inventory_hash": config.get("_wide_schema_inventory_hash"),
        "plan_hash": config.get("_wide_schema_plan_hash"),
        "omitted_context_summary": {
            "columns_omitted_by_handoff_cap": omitted_columns,
            "full_inventory_columns": config.get("_wide_schema_inventory_column_count"),
        },
    }


def apply_prompt_matching_context(
    config: dict[str, Any],
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach a persisted prompt-matching handoff to a fresh space config."""
    if not isinstance(context, dict):
        return config
    config["_uc_columns"] = copy.deepcopy(context.get("uc_columns") or [])
    config["_data_profile"] = copy.deepcopy(context.get("data_profile") or {})
    config["_rls_audit"] = copy.deepcopy(context.get("rls_audit") or {})
    config["_asset_semantics"] = copy.deepcopy(context.get("asset_semantics") or {})
    config["_wide_schema_inventory_hash"] = context.get("inventory_hash")
    config["_wide_schema_plan_hash"] = context.get("plan_hash")
    config["_wide_schema_omitted_context_summary"] = copy.deepcopy(
        context.get("omitted_context_summary") or {}
    )
    return config


@dataclass
class SpaceQualityEnrichmentResult:
    """Result payload returned to the unified optimization loop."""

    raw_config: dict[str, Any]
    current_config: dict[str, Any]
    scan_before: dict[str, Any] | None = None
    scan_after: dict[str, Any] | None = None
    space_quality_scan: dict[str, Any] | None = None
    applied_count: int = 0
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def parsed_space_from_config(config: dict[str, Any] | None) -> dict[str, Any]:
    """Extract a parsed ``serialized_space`` object from a raw or parsed config."""
    if not isinstance(config, dict):
        return {}
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return copy.deepcopy(parsed)
    serialized = config.get("serialized_space")
    if isinstance(serialized, dict):
        return copy.deepcopy(serialized)
    if isinstance(serialized, str) and serialized.strip():
        try:
            loaded = json.loads(serialized)
        except (json.JSONDecodeError, TypeError):
            loaded = None
        if isinstance(loaded, dict):
            return copy.deepcopy(loaded)
    return copy.deepcopy(config)


def top_level_space_description(config: dict[str, Any] | None) -> str:
    """Return the real top-level Genie Space description, if present."""
    if not isinstance(config, dict):
        return ""
    for key in ("description", "_gso_top_level_description"):
        value = config.get(key)
        if isinstance(value, list):
            text = " ".join(str(v) for v in value if v is not None).strip()
        else:
            text = str(value or "").strip()
        if text:
            return text
    return ""


def scan_input_for_iq(config: dict[str, Any] | None) -> dict[str, Any]:
    """Build IQ Scan input without moving top-level metadata into patches.

    Genie stores the Space ``description`` as top-level metadata, while IQ Scan
    expects to score a single dict.  This helper merges the description only in
    memory so scoring sees the real Space state without polluting
    ``serialized_space``.
    """
    parsed = parsed_space_from_config(config)
    description = top_level_space_description(config)
    if description:
        parsed["description"] = description
    return parsed


def attach_top_level_description(
    parsed_config: dict[str, Any],
    raw_config: dict[str, Any] | None,
) -> dict[str, Any]:
    """Carry top-level description as optimizer runtime metadata."""
    out = copy.deepcopy(parsed_config)
    description = top_level_space_description(raw_config)
    if description:
        out["_gso_top_level_description"] = description
    return out


def run_space_quality_enrichment(
    w: Any,
    spark: Any,
    *,
    run_id: str,
    space_id: str,
    raw_config: dict[str, Any],
    catalog: str,
    schema: str,
    prompt_matching_context: dict[str, Any] | None = None,
    benchmarks: list[dict[str, Any]] | None = None,
) -> SpaceQualityEnrichmentResult:
    """Apply narrow, non-fatal quality curation before baseline eval."""
    working_raw = copy.deepcopy(raw_config)
    parsed = parsed_space_from_config(working_raw)
    result = SpaceQualityEnrichmentResult(
        raw_config=working_raw,
        current_config=attach_top_level_description(parsed, working_raw),
    )

    try:
        result.scan_before = calculate_score(scan_input_for_iq(working_raw))
    except Exception as exc:
        msg = f"iq_scan_before_failed:{type(exc).__name__}"
        logger.warning("Space quality enrichment: initial IQ Scan failed", exc_info=True)
        result.errors.append(msg)
        _write_description_artifact(
            spark, run_id, working_raw, catalog=catalog, schema=schema,
        )
        return result

    write_stage(
        spark,
        run_id,
        _STAGE,
        "STARTED",
        task_key=_TASK_KEY,
        catalog=catalog,
        schema=schema,
    )

    if _data_source_count(parsed) == 0:
        result.skipped.append("no_data_sources")
        result.scan_after = result.scan_before
        result.space_quality_scan = build_space_quality_scan_context(result.scan_before)
        write_stage(
            spark,
            run_id,
            _STAGE,
            "COMPLETE",
            task_key=_TASK_KEY,
            detail=_stage_detail(result),
            catalog=catalog,
            schema=schema,
        )
        _write_description_artifact(
            spark, run_id, working_raw, catalog=catalog, schema=schema,
        )
        return result

    patch_index = 0

    def _record_error(label: str, exc: Exception) -> None:
        err = f"{label}:{type(exc).__name__}: {exc}"
        logger.warning("Space quality enrichment %s failed", label, exc_info=True)
        result.errors.append(err[:500])

    try:
        patch_index = _maybe_enrich_description(
            w,
            spark,
            run_id=run_id,
            space_id=space_id,
            raw_config=working_raw,
            parsed=parsed,
            scan_result=result.scan_before,
            patch_index=patch_index,
            catalog=catalog,
            schema=schema,
            on_error=_record_error,
        )

        patch_index = _maybe_seed_instructions(
            w,
            spark,
            run_id=run_id,
            space_id=space_id,
            parsed=parsed,
            scan_result=result.scan_before,
            patch_index=patch_index,
            catalog=catalog,
            schema=schema,
            skipped=result.skipped,
            on_error=_record_error,
        )

        patch_index = _maybe_apply_prompt_matching(
            w,
            spark,
            run_id=run_id,
            space_id=space_id,
            parsed=parsed,
            prompt_matching_context=prompt_matching_context,
            benchmarks=benchmarks,
            scan_result=result.scan_before,
            patch_index=patch_index,
            catalog=catalog,
            schema=schema,
            skipped=result.skipped,
            on_error=_record_error,
        )

        result.applied_count = patch_index
        if result.applied_count:
            try:
                refreshed_raw = fetch_space_config(w, space_id)
                # The description PATCH already succeeded and the local raw
                # snapshot carries its exact value. Keep it authoritative if a
                # read-after-write response is stale while refreshing the
                # serialized-space portion.
                if "description" in working_raw:
                    refreshed_raw["description"] = working_raw["description"]
                working_raw = refreshed_raw
                parsed = parsed_space_from_config(working_raw)
            except Exception:
                logger.warning(
                    "Space quality enrichment: could not refresh space after writes; "
                    "continuing with local post-write snapshot",
                    exc_info=True,
                )
                working_raw["_parsed_space"] = copy.deepcopy(parsed)

        result.raw_config = working_raw
        result.current_config = attach_top_level_description(parsed, working_raw)

        try:
            result.scan_after = calculate_score(scan_input_for_iq(working_raw))
            result.space_quality_scan = build_space_quality_scan_context(
                result.scan_after,
            )
        except Exception:
            logger.debug("Space quality enrichment: post IQ Scan failed", exc_info=True)
            result.scan_after = None
            result.space_quality_scan = build_space_quality_scan_context(
                result.scan_before,
            )

        write_stage(
            spark,
            run_id,
            _STAGE,
            "COMPLETE",
            task_key=_TASK_KEY,
            detail=_stage_detail(result),
            catalog=catalog,
            schema=schema,
        )
        _write_description_artifact(
            spark, run_id, working_raw, catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:  # pragma: no cover - defensive non-fatal boundary
        _record_error("unexpected", exc)
        write_stage(
            spark,
            run_id,
            _STAGE,
            "COMPLETE",
            task_key=_TASK_KEY,
            detail=_stage_detail(result),
            catalog=catalog,
            schema=schema,
        )
        result.current_config = attach_top_level_description(parsed, working_raw)
        _write_description_artifact(
            spark, run_id, working_raw, catalog=catalog, schema=schema,
        )
        return result


def _maybe_apply_prompt_matching(
    w: Any,
    spark: Any,
    *,
    run_id: str,
    space_id: str,
    parsed: dict[str, Any],
    prompt_matching_context: dict[str, Any] | None,
    benchmarks: list[dict[str, Any]] | None,
    scan_result: dict[str, Any] | None,
    patch_index: int,
    catalog: str,
    schema: str,
    skipped: list[str],
    on_error,
) -> int:
    """Run deterministic format/entity matching and persist its audit trail."""
    if not prompt_matching_context:
        skipped.append("prompt_matching_context_unavailable")
        return patch_index

    runtime_config: dict[str, Any] = {"_parsed_space": parsed}
    apply_prompt_matching_context(runtime_config, prompt_matching_context)

    try:
        apply_log = auto_apply_prompt_matching(
            w,
            space_id,
            runtime_config,
            benchmarks=benchmarks,
        )
    except Exception as exc:
        on_error("prompt_matching", exc)
        return patch_index

    applied = list(apply_log.get("applied") or [])
    if not applied:
        skipped.append("prompt_matching_already_configured")
        return patch_index

    check = _check(scan_result, 8)
    inverse_by_type = {
        "enable_example_values": {"enable_format_assistance": False},
        "enable_value_dictionary": {"enable_entity_matching": False},
        "disable_value_dictionary": {"enable_entity_matching": True},
    }
    command_by_type = {
        "enable_example_values": {"enable_format_assistance": True},
        "enable_value_dictionary": {"enable_entity_matching": True},
        "disable_value_dictionary": {"enable_entity_matching": False},
    }

    for offset, entry in enumerate(applied):
        patch_type = str(entry.get("type") or "prompt_matching")
        table = str(entry.get("table") or "")
        column = str(entry.get("column") or "")
        command = command_by_type.get(patch_type)
        rollback = inverse_by_type.get(patch_type)
        try:
            write_patch(
                spark,
                run_id,
                0,
                0,
                patch_index + offset,
                {
                    "patch_type": patch_type,
                    "scope": "genie_config",
                    "risk_level": "low",
                    "target_object": f"{table}.{column}".strip("."),
                    "patch": entry,
                    "command": (
                        {
                            "op": "update",
                            "section": "column_configs",
                            "table": table,
                            "column": column,
                            **command,
                        }
                        if command else None
                    ),
                    "rollback": (
                        {
                            "op": "update",
                            "section": "column_configs",
                            "table": table,
                            "column": column,
                            **rollback,
                        }
                        if rollback else None
                    ),
                    "proposal_id": "space_quality_enrichment.prompt_matching",
                    "provenance": {"iq_check_id": 8, "iq_check": check},
                },
                catalog,
                schema,
            )
        except Exception as exc:
            on_error("prompt_matching_audit", exc)

    entity_changes = sum(
        1
        for entry in applied
        if entry.get("type") in {"enable_value_dictionary", "disable_value_dictionary"}
    )
    wait_seconds = (
        PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS
        if entity_changes
        else PROPAGATION_WAIT_SECONDS
    )
    if wait_seconds > 0:
        time.sleep(wait_seconds)
    return patch_index + len(applied)


def _write_description_artifact(
    spark: Any,
    run_id: str,
    raw_config: dict[str, Any] | None,
    *,
    catalog: str,
    schema: str,
) -> None:
    """Persist exact post-enrichment description metadata for history revert."""
    present = isinstance(raw_config, dict) and "description" in raw_config
    value = raw_config.get("description") if present and raw_config is not None else None
    if isinstance(value, list):
        description = " ".join(str(item) for item in value if item is not None)
    else:
        description = "" if value is None else str(value)
    write_artifact(
        spark,
        run_id,
        "space_quality_enrichment",
        {
            "description_present": present,
            "description": description,
        },
        catalog=catalog,
        schema=schema,
        stage_name=_STAGE,
        iteration=0,
        source_notebook="run_optimize.py",
    )


def _check(scan_result: dict[str, Any] | None, check_id: int) -> dict[str, Any]:
    checks = scan_result.get("checks") if isinstance(scan_result, dict) else None
    if not isinstance(checks, list) or check_id <= 0 or check_id > len(checks):
        return {"passed": True, "severity": "pass"}
    raw = checks[check_id - 1]
    return raw if isinstance(raw, dict) else {"passed": True, "severity": "pass"}


def _maybe_enrich_description(
    w: Any,
    spark: Any,
    *,
    run_id: str,
    space_id: str,
    raw_config: dict[str, Any],
    parsed: dict[str, Any],
    scan_result: dict[str, Any] | None,
    patch_index: int,
    catalog: str,
    schema: str,
    on_error,
) -> int:
    check = _check(scan_result, 1)
    if bool(check.get("passed")):
        return patch_index

    try:
        from genie_space_optimizer.optimization.optimizer_utils import (
            _generate_space_description,
        )

        old_desc = top_level_space_description(raw_config)
        desc_text = _generate_space_description(parsed, w)
        if not desc_text:
            return patch_index

        update_space_description(w, space_id, desc_text)
        raw_config["description"] = desc_text
        write_patch(
            spark,
            run_id,
            0,
            0,
            patch_index,
            {
                "patch_type": "update_space_description",
                "scope": "genie_space",
                "risk_level": "low",
                "target_object": "space.description",
                "patch": {
                    "old_chars": len(old_desc),
                    "new_chars": len(desc_text),
                    "description_preview": desc_text[:200],
                },
                "command": {"op": "update", "section": "space", "field": "description"},
                "rollback": {
                    "op": "update",
                    "section": "space",
                    "field": "description",
                    "description": old_desc,
                },
                "proposal_id": "space_quality_enrichment.description",
                "provenance": {"iq_check_id": 1, "iq_check": check},
            },
            catalog,
            schema,
        )
        return patch_index + 1
    except Exception as exc:
        on_error("description", exc)
        return patch_index


def _maybe_seed_instructions(
    w: Any,
    spark: Any,
    *,
    run_id: str,
    space_id: str,
    parsed: dict[str, Any],
    scan_result: dict[str, Any] | None,
    patch_index: int,
    catalog: str,
    schema: str,
    skipped: list[str],
    on_error,
) -> int:
    check = _check(scan_result, 4)
    if bool(check.get("passed")):
        return patch_index

    current = _get_general_instructions(parsed)
    if current and len(current.strip()) >= _INSTRUCTION_SEED_THRESHOLD:
        return patch_index

    seed_text = _minimal_instruction_seed(parsed, current)
    ok, errors = validate_instruction_text(seed_text, strict=True)
    if not ok:
        skipped.append("instruction_seed_validation_failed")
        logger.warning("Space quality instruction seed declined: %s", errors)
        return patch_index

    try:
        _set_general_instructions(parsed, seed_text)
        patch_space_config(w, space_id, parsed)
        write_patch(
            spark,
            run_id,
            0,
            0,
            patch_index,
            {
                "patch_type": "add_instruction",
                "scope": "genie_config",
                "risk_level": "low",
                "target_object": "instructions.text_instructions[0].content",
                "patch": {
                    "old_chars": len(current or ""),
                    "new_chars": len(seed_text),
                    "sections": [
                        "PURPOSE",
                        "DISAMBIGUATION",
                        "CONSTRAINTS",
                        "Instructions you must follow when providing summaries",
                    ],
                },
                "command": {
                    "op": "add",
                    "section": "instructions",
                    "source": "space_quality_enrichment",
                },
                "rollback": {
                    "op": "update",
                    "section": "instructions",
                    "old_text": current,
                },
                "proposal_id": "space_quality_enrichment.instructions",
                "provenance": {"iq_check_id": 4, "iq_check": check},
            },
            catalog,
            schema,
        )
        return patch_index + 1
    except Exception as exc:
        on_error("instructions", exc)
        return patch_index


def _minimal_instruction_seed(parsed: dict[str, Any], existing_text: str = "") -> str:
    source_phrase = _source_phrase(parsed)
    purpose = f"Answer questions using the configured Genie Space data sources: {source_phrase}."
    if existing_text.strip():
        existing = existing_text.strip().replace("\n", " ")
        if len(existing) > 140:
            existing = existing[:137].rstrip() + "..."
        purpose = f"{purpose} Existing guidance remains in effect: {existing}."

    return "\n\n".join(
        [
            "## PURPOSE\n"
            f"- {purpose}",
            "## DISAMBIGUATION\n"
            "- When a request is missing a time range, metric, entity, or grouping, ask one concise clarification question before querying.",
            "## CONSTRAINTS\n"
            "- Use only configured data sources and documented relationships in this Genie Space.\n"
            "- Do not expose columns that are hidden or excluded from the space.",
            "## Instructions you must follow when providing summaries\n"
            "- State relevant filters, grouping, and date range when they affect the answer.",
        ]
    )


def _source_phrase(parsed: dict[str, Any]) -> str:
    ds = parsed.get("data_sources") if isinstance(parsed, dict) else {}
    if not isinstance(ds, dict):
        return "the configured data sources"

    names: list[str] = []
    for key in ("tables", "metric_views", "functions"):
        values = ds.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            ident = str(
                item.get("identifier")
                or item.get("name")
                or item.get("table_name")
                or ""
            ).strip()
            if ident:
                names.append(ident)
            if len(names) >= _MAX_SOURCE_NAMES:
                break
        if len(names) >= _MAX_SOURCE_NAMES:
            break

    if not names:
        return "the configured data sources"

    display = [f"`{name}`" for name in names[:_MAX_SOURCE_NAMES]]
    extra = _data_source_count(parsed) - len(display)
    if extra > 0:
        return ", ".join(display) + f", and {extra} other source(s)"
    return ", ".join(display)


def _data_source_count(parsed: dict[str, Any]) -> int:
    ds = parsed.get("data_sources") if isinstance(parsed, dict) else {}
    if not isinstance(ds, dict):
        return 0
    return sum(len(ds.get(k) or []) for k in ("tables", "metric_views", "functions"))


def _stage_detail(result: SpaceQualityEnrichmentResult) -> dict[str, Any]:
    before = result.scan_before or {}
    after = result.scan_after or {}
    return {
        "applied_count": result.applied_count,
        "skipped": result.skipped,
        "errors": result.errors[:5],
        "score_before": before.get("score"),
        "score_after": after.get("score"),
        "maturity_before": before.get("maturity"),
        "maturity_after": after.get("maturity"),
    }
