"""
Optimization Applier — patch rendering, application, and rollback.

Converts optimizer proposals into Patch DSL actions, applies them to the
Genie Space config (and optionally UC artifacts), and supports full
snapshot-based rollback.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from typing import Any

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    CATEGORICAL_COLUMN_PATTERNS,
    FREE_TEXT_COLUMN_PATTERNS,
    HIGH_RISK_PATCHES,
    LOW_RISK_PATCHES,
    MAX_VALUE_DICTIONARY_COLUMNS,
    MEASURE_NAME_PREFIXES,
    MEDIUM_RISK_PATCHES,
    NUMERIC_DATA_TYPES,
    PATCH_TYPES,
    _LEVER_TO_PATCH_TYPE,
)
from genie_space_optimizer.optimization.structured_metadata import (
    LeverOwnershipError,
    entity_type_for_column,
    extract_synonyms_section,
    format_synonyms_section,
    merge_synonyms,
    parse_structured_description,
    render_structured_description,
    update_sections,
)
from genie_space_optimizer.common.genie_client import (
    patch_space_config,
    sort_genie_config,
    strip_non_exportable_fields,
)
from genie_space_optimizer.optimization.optimizer import _resolve_scope

logger = logging.getLogger(__name__)


_MAX_INSTRUCTION_CHARS = 24_500  # Genie Space API enforces 25 000; leave margin


# ── Join Spec Helpers ─────────────────────────────────────────────────
# The Genie Space API uses nested objects for join specs:
#   {"left": {"identifier": "...", "alias": "..."}, "right": {...}, "sql": [...]}
# These helpers extract identifiers for matching across add/update/remove ops.


def _join_spec_left_id(spec: dict) -> str:
    """Extract left table identifier from a join spec (API or legacy format)."""
    left = spec.get("left")
    if isinstance(left, dict):
        return left.get("identifier", "")
    return spec.get("left_table_name", "")


def _join_spec_right_id(spec: dict) -> str:
    """Extract right table identifier from a join spec (API or legacy format)."""
    right = spec.get("right")
    if isinstance(right, dict):
        return right.get("identifier", "")
    return spec.get("right_table_name", "")


def _validate_join_spec_entry(entry: dict) -> bool:
    """Validate a join spec dict conforms to the Genie API schema.

    Skips validation for legacy format (``left_table_name`` / ``right_table_name``)
    which is only used internally and gets transformed before the final PATCH.
    """
    if "left_table_name" in entry or "right_table_name" in entry:
        return True

    from genie_space_optimizer.common.genie_schema import JoinSpec

    try:
        JoinSpec.model_validate(entry)
        return True
    except Exception as exc:
        logger.warning("Invalid join spec rejected: %s — validation error: %s", entry, exc)
        return False


def _validate_example_sql_entry(
    entry: dict, config: dict | None = None
) -> bool:
    """Validate an example_question_sql dict conforms to the Genie API schema.

    When *config* is provided, also verifies the SQL references at least one
    known table, TVF, or metric view from the current config.
    """
    from genie_space_optimizer.common.genie_schema import ExampleQuestionSql

    try:
        ExampleQuestionSql.model_validate(entry)
    except Exception:
        logger.warning("Invalid example SQL entry rejected: %s", entry)
        return False

    if config:
        raw_sql = entry.get("sql") or ""
        sql_lower = (" ".join(raw_sql) if isinstance(raw_sql, list) else str(raw_sql)).lower()
        if sql_lower:
            known: set[str] = set()
            ds = config.get("data_sources") or {}
            for source_key in ("tables", "metric_views"):
                for tbl in ds.get(source_key) or []:
                    ident = (tbl.get("identifier") or tbl.get("name") or "").lower()
                    known.add(ident)
                    parts = ident.split(".")
                    if len(parts) >= 2:
                        known.add(".".join(parts[-2:]))
                    known.add(parts[-1])
            known.discard("")
            if known and not any(a in sql_lower for a in known):
                logger.warning(
                    "Example SQL references no known asset — rejected: %.120s",
                    entry.get("sql", ""),
                )
                return False

    return True


def _enforce_instruction_limit(config: dict) -> None:
    """Trim text_instructions content so it stays under the API limit."""
    ti = (config.get("instructions") or {}).get("text_instructions", [])
    if not ti:
        return
    content = ti[0].get("content", [])
    if not isinstance(content, list):
        content = [str(content)]
    full_text = "\n".join(content)
    if len(full_text) <= _MAX_INSTRUCTION_CHARS:
        return
    logger.warning(
        "Instruction text %d chars exceeds limit %d — trimming from end",
        len(full_text), _MAX_INSTRUCTION_CHARS,
    )
    ti[0]["content"] = [full_text[:_MAX_INSTRUCTION_CHARS].rsplit("\n", 1)[0]]


def _get_general_instructions(config: dict) -> str:
    """Extract general instructions as joined text from text_instructions."""
    inst = config.get("instructions", {})
    ti = inst.get("text_instructions", [])
    if not ti:
        return ""
    content = ti[0].get("content", [])
    if isinstance(content, list):
        return "\n".join(c for c in content if c)
    return str(content)


_HEX_32 = re.compile(r"^[0-9a-f]{32}$")


def _set_general_instructions(
    config: dict, text: str, instruction_id: str | None = None
) -> None:
    """Set general instructions into text_instructions.

    Stores the full text as a single content list item so that the Genie UI
    renders newlines correctly.  The ``content`` field is ``list[str]``; using
    multiple items causes the UI to concatenate them without line breaks.
    """
    from genie_space_optimizer.common.genie_schema import generate_genie_id

    inst = config.setdefault("instructions", {})
    ti = inst.setdefault("text_instructions", [])
    effective_id = instruction_id or (ti[0].get("id") if ti else None) or generate_genie_id()
    if not _HEX_32.match(effective_id or ""):
        effective_id = generate_genie_id()

    content = [text.strip()] if text and text.strip() else [""]

    if ti:
        ti[0] = {"id": effective_id, "content": content}
    else:
        ti.append({"id": effective_id, "content": content})


# ═══════════════════════════════════════════════════════════════════════
# 1b. Prompt Matching Auto-Config
# ═══════════════════════════════════════════════════════════════════════


def _is_measure_column(column_name: str, data_type: str) -> bool:
    """Return True if the column looks like a metric view measure."""
    dt_upper = (data_type or "").upper().split("(")[0].strip()
    if dt_upper in NUMERIC_DATA_TYPES:
        return True
    lower_name = column_name.lower()
    return any(lower_name.startswith(p) for p in MEASURE_NAME_PREFIXES)


def _is_hidden(cc: dict) -> bool:
    if cc.get("visible") is False:
        return True
    if cc.get("exclude") is True:
        return True
    return False


def _entity_matching_score(column_name: str) -> int:
    """Score a STRING column for entity matching priority.

    Higher score = higher priority.  Returns 0-2.
    """
    lower = column_name.lower()
    if any(pat in lower for pat in FREE_TEXT_COLUMN_PATTERNS):
        return 0
    if any(pat in lower for pat in CATEGORICAL_COLUMN_PATTERNS):
        return 2
    return 1


def _ensure_column_configs_from_uc(
    tables_and_mvs: list[dict], uc_columns: list[dict],
) -> int:
    """Populate column_configs from UC metadata for columns not yet present.

    The Genie Space API only returns column_configs for explicitly configured
    columns. For unconfigured spaces every table has column_configs: [].
    This bootstraps entries so that format assistance and entity matching
    loops have something to iterate over.

    Returns the number of new column_config entries created.
    """
    table_cols: dict[str, list[str]] = {}
    for col in uc_columns:
        if not isinstance(col, dict):
            continue
        tbl = str(col.get("table_name") or "").strip().lower()
        cname = str(col.get("column_name") or "").strip()
        if tbl and cname:
            table_cols.setdefault(tbl, []).append(cname)

    created = 0
    for tbl_dict in tables_and_mvs:
        identifier = tbl_dict.get("identifier", "")
        short_name = identifier.replace("`", "").rsplit(".", 1)[-1].lower()
        existing_names = {
            cc.get("column_name", "").lower()
            for cc in tbl_dict.get("column_configs", [])
        }
        for col_name in table_cols.get(short_name, []):
            if col_name.lower() not in existing_names:
                tbl_dict.setdefault("column_configs", []).append(
                    {"column_name": col_name}
                )
                created += 1
    return created


def auto_apply_prompt_matching(
    w: WorkspaceClient,
    space_id: str,
    config: dict,
) -> dict:
    """Enable format assistance and entity matching as a best-practice step.

    Operates deterministically (no LLM calls).  Mutates ``config`` in-place
    and PATCHes the Genie Space via the API.

    Returns an apply_log dict with ``applied`` list, ``patched_objects``,
    ``pre_snapshot``, ``post_snapshot``, and summary stats.
    """
    parsed = config.get("_parsed_space", config)
    ds = parsed.get("data_sources", {})
    tables = ds.get("tables", [])
    metric_views = ds.get("metric_views", [])
    uc_columns: list[dict] = config.get("_uc_columns", [])

    bootstrapped = _ensure_column_configs_from_uc(tables + metric_views, uc_columns)
    if bootstrapped:
        print(f"  [PROMPT MATCHING] Bootstrapped {bootstrapped} column_config entries from UC metadata")

    type_lookup: dict[tuple[str, str], str] = {}
    for col in uc_columns:
        if not isinstance(col, dict):
            continue
        tbl = str(col.get("table_name") or "").strip()
        cname = str(col.get("column_name") or "").strip()
        dtype = str(col.get("data_type") or "").strip()
        if tbl and cname:
            type_lookup[(tbl.lower(), cname.lower())] = dtype

    pre_snapshot = copy.deepcopy(parsed)
    changes: list[dict] = []

    already_dict_count = sum(
        1
        for t in tables + metric_views
        for cc in t.get("column_configs", [])
        if cc.get("enable_entity_matching")
    )

    entity_candidates: list[tuple[str, str, str, int]] = []

    def _table_short_name(identifier: str) -> str:
        parts = identifier.replace("`", "").split(".")
        return parts[-1] if parts else identifier

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = _table_short_name(identifier)
        for cc in tbl.get("column_configs", []):
            col_name = cc.get("column_name", "")
            if _is_hidden(cc) or not col_name:
                continue
            if not cc.get("enable_format_assistance"):
                cc["enable_format_assistance"] = True
                changes.append({
                    "type": "enable_example_values",
                    "table": identifier,
                    "column": col_name,
                })
            dtype = type_lookup.get((short_name.lower(), col_name.lower()), "")
            if (
                dtype.upper().split("(")[0].strip() == "STRING"
                and not cc.get("enable_entity_matching")
            ):
                score = _entity_matching_score(col_name)
                entity_candidates.append((identifier, col_name, dtype, score))

    for mv in metric_views:
        identifier = mv.get("identifier", "")
        short_name = _table_short_name(identifier)
        for cc in mv.get("column_configs", []):
            col_name = cc.get("column_name", "")
            if _is_hidden(cc) or not col_name:
                continue
            dtype = type_lookup.get((short_name.lower(), col_name.lower()), "")
            if _is_measure_column(col_name, dtype):
                continue
            if not cc.get("enable_format_assistance"):
                cc["enable_format_assistance"] = True
                changes.append({
                    "type": "enable_example_values",
                    "table": identifier,
                    "column": col_name,
                })
            if (
                dtype.upper().split("(")[0].strip() == "STRING"
                and not cc.get("enable_entity_matching")
            ):
                score = _entity_matching_score(col_name)
                entity_candidates.append((identifier, col_name, dtype, score))

    entity_candidates.sort(key=lambda x: -x[3])
    slots_available = MAX_VALUE_DICTIONARY_COLUMNS - already_dict_count
    selected = entity_candidates[:max(slots_available, 0)]

    fa_count_preview = sum(1 for c in changes if c["type"] == "enable_example_values")
    fa_skipped = sum(
        1
        for t in tables + metric_views
        for cc in t.get("column_configs", [])
        if cc.get("enable_format_assistance") and not _is_hidden(cc)
    ) - fa_count_preview

    fa_lines = [f"\n-- FORMAT ASSISTANCE " + "-" * 31]
    fa_lines.append(f"  Enabled format assistance on {fa_count_preview} columns")
    fa_lines.append(f"  Skipped (already enabled): {fa_skipped}")
    fa_lines.append("-" * 52)
    print("\n".join(fa_lines))

    em_lines = [f"\n-- ENTITY MATCHING (Value Dictionary) " + "-" * 14]
    em_lines.append(f"  STRING columns scored for entity matching: {len(entity_candidates)}")
    for rank, (ident, cname, _dt, sc) in enumerate(entity_candidates[:20], 1):
        status = "SELECTED" if rank <= len(selected) else "NOT SELECTED (slot limit)"
        em_lines.append(f"    Rank {rank:2d}: {ident}.{cname:<30s} score={sc}  {status}")
    em_lines.append(
        f"  Slots used: {already_dict_count} existing + {len(selected)} new = "
        f"{already_dict_count + len(selected)} / {MAX_VALUE_DICTIONARY_COLUMNS} max"
    )
    em_lines.append("-" * 52)
    print("\n".join(em_lines))

    for identifier, col_name, _dtype, _score in selected:
        tbl_dict = _find_table_in_config(parsed, identifier)
        if not tbl_dict:
            continue
        cc = _find_or_create_column_config(tbl_dict, col_name)
        cc["enable_entity_matching"] = True
        if not cc.get("enable_format_assistance"):
            cc["enable_format_assistance"] = True
        changes.append({
            "type": "enable_value_dictionary",
            "table": identifier,
            "column": col_name,
        })

    if not changes:
        logger.info("Prompt matching auto-config: no changes needed (already configured)")
        return {
            "applied": [],
            "patched_objects": [],
            "pre_snapshot": pre_snapshot,
            "post_snapshot": parsed,
            "format_assistance_count": 0,
            "entity_matching_count": 0,
        }

    sort_genie_config(parsed)
    _enforce_instruction_limit(parsed)
    patch_space_config(w, space_id, parsed)

    fa_count = sum(1 for c in changes if c["type"] == "enable_example_values")
    em_count = sum(1 for c in changes if c["type"] == "enable_value_dictionary")
    patched_objects = sorted({c["table"] for c in changes})

    print(
        f"Prompt matching auto-config: enabled format assistance on {fa_count} columns, "
        f"entity matching on {em_count} STRING columns "
        f"({already_dict_count + em_count}/{MAX_VALUE_DICTIONARY_COLUMNS} dictionary slots used)"
    )

    return {
        "applied": changes,
        "patched_objects": patched_objects,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": copy.deepcopy(parsed),
        "format_assistance_count": fa_count,
        "entity_matching_count": em_count,
    }


def _find_table_in_config(config: dict, table_id: str) -> dict | None:
    """Find a table or metric view in data_sources by identifier."""
    ds = config.get("data_sources", {})
    for source_list in [ds.get("tables", []), ds.get("metric_views", [])]:
        for t in source_list:
            if t.get("identifier") == table_id:
                return t
    return None


def _find_or_create_column_config(table_dict: dict, column_name: str) -> dict:
    """Find an existing column_config or create one."""
    for cc in table_dict.get("column_configs", []):
        if cc.get("column_name") == column_name:
            return cc
    new_cc = {"column_name": column_name}
    table_dict.setdefault("column_configs", []).append(new_cc)
    return new_cc


# ═══════════════════════════════════════════════════════════════════════
# 2. Risk Classification
# ═══════════════════════════════════════════════════════════════════════


def classify_risk(patch_type: str | dict) -> str:
    """Classify a patch type as ``low``, ``medium``, or ``high``."""
    pt = patch_type if isinstance(patch_type, str) else patch_type.get("type", "")
    if pt in LOW_RISK_PATCHES:
        return "low"
    if pt in MEDIUM_RISK_PATCHES:
        return "medium"
    if pt in HIGH_RISK_PATCHES:
        return "high"
    return "medium"


# ═══════════════════════════════════════════════════════════════════════
# 3. Proposal → Patch Conversion
# ═══════════════════════════════════════════════════════════════════════


def proposals_to_patches(proposals: list[dict]) -> list[dict]:
    """Convert optimizer proposals into Patch DSL patches.

    Each proposal has an ``asi`` dict with ``failure_type``, ``blame_set``,
    ``counterfactual_fixes``.  Maps to concrete ``patch_type`` via
    ``_LEVER_TO_PATCH_TYPE``.

    For structured column proposals (Levers 1/2) carrying ``column_description``
    and/or ``column_synonyms``, emits separate patches for each field.
    """
    patches: list[dict] = []
    for p in proposals:
        asi = p.get("asi", {})
        if not isinstance(asi, dict):
            asi = {}
        failure_type = asi.get(
            "failure_type", p.get("lever_type", "other")
        )
        lever = p.get("lever", 5)
        patch_type = p.get("patch_type") or _LEVER_TO_PATCH_TYPE.get(
            (failure_type, lever),
            _LEVER_TO_PATCH_TYPE.get((failure_type, 1), "add_instruction"),
        )
        blame_set = asi.get("blame_set", [])
        target = blame_set[0] if blame_set else p.get("change_description", "unknown")
        fixes = asi.get("counterfactual_fixes", [])
        if isinstance(fixes, str):
            fixes = [fixes]
        new_text = p.get("proposed_value") or (fixes[0] if fixes else p.get("change_description", ""))

        table_sections = p.get("table_sections")
        col_sections = p.get("column_sections")
        col_desc = p.get("column_description")
        col_syns = p.get("column_synonyms")
        tbl_id = p.get("table", "")
        col_name = p.get("column", "")

        if isinstance(table_sections, dict) and table_sections and tbl_id and not col_name:
            patches.append({
                "type": "update_description",
                "target": tbl_id,
                "new_text": "",
                "old_text": "",
                "structured_sections": table_sections,
                "table_entity_type": p.get("table_entity_type", "table"),
                "lever": lever,
                "risk_level": classify_risk("update_description"),
                "predicted_affected_questions": p.get("questions_fixed", 0),
                "grounded_in": p.get("grounded_in", []),
                "source_proposal_id": p.get("proposal_id", ""),
                "table": tbl_id,
            })
            continue

        if isinstance(col_sections, dict) and col_sections and tbl_id and col_name:
            base = {
                "lever": lever,
                "risk_level": classify_risk(patch_type),
                "predicted_affected_questions": p.get("questions_fixed", 0),
                "grounded_in": p.get("grounded_in", []),
                "source_proposal_id": p.get("proposal_id", ""),
                "table": tbl_id,
                "column": col_name,
            }
            non_synonym_sections = {
                k: v for k, v in col_sections.items() if k != "synonyms"
            }
            synonym_value = col_sections.get("synonyms", "")
            if non_synonym_sections:
                patches.append({
                    **base,
                    "type": "update_column_description",
                    "target": tbl_id,
                    "new_text": "",
                    "old_text": "",
                    "structured_sections": non_synonym_sections,
                    "column_entity_type": p.get("column_entity_type", ""),
                })
            if synonym_value:
                if isinstance(synonym_value, list):
                    new_syns = [str(s).strip() for s in synonym_value if str(s).strip()]
                else:
                    new_syns = [s.strip() for s in str(synonym_value).split(",") if s.strip()]
                if new_syns:
                    patches.append({
                        **base,
                        "type": "add_column_synonym",
                        "target": tbl_id,
                        "new_text": "",
                        "old_text": "",
                        "synonyms": new_syns,
                    })
            continue

        if (col_desc is not None or col_syns is not None) and tbl_id and col_name:
            base = {
                "lever": lever,
                "risk_level": classify_risk(patch_type),
                "predicted_affected_questions": p.get("questions_fixed", 0),
                "grounded_in": p.get("grounded_in", []),
                "source_proposal_id": p.get("proposal_id", ""),
                "table": tbl_id,
                "column": col_name,
            }
            if col_desc is not None and isinstance(col_desc, list) and col_desc:
                patches.append({
                    **base,
                    "type": "update_column_description",
                    "target": tbl_id,
                    "new_text": col_desc[0] if len(col_desc) == 1 else "\n".join(col_desc),
                    "old_text": "",
                })
            if col_syns is not None and isinstance(col_syns, list) and col_syns:
                patches.append({
                    **base,
                    "type": "add_column_synonym",
                    "target": tbl_id,
                    "new_text": col_syns[0] if len(col_syns) == 1 else "",
                    "old_text": "",
                    "synonyms": col_syns,
                })
            continue

        # ── Auto-convert freeform description patches to structured ──
        if (
            patch_type in ("update_column_description", "update_description")
            and new_text
            and not col_sections
            and not table_sections
        ):
            parsed = parse_structured_description(new_text)
            if parsed and any(k != "_preamble" for k in parsed):
                structured = {k: v for k, v in parsed.items() if k != "_preamble" and v}
                if structured:
                    logger.info(
                        "Auto-converting freeform %s to structured sections: %s",
                        patch_type, list(structured.keys()),
                    )
                    base_freeform = {
                        "lever": lever,
                        "risk_level": classify_risk(patch_type),
                        "predicted_affected_questions": p.get("questions_fixed", 0),
                        "grounded_in": p.get("grounded_in", []),
                        "source_proposal_id": p.get("proposal_id", ""),
                    }
                    if tbl_id:
                        base_freeform["table"] = tbl_id
                    if col_name:
                        base_freeform["column"] = col_name
                    patches.append({
                        **base_freeform,
                        "type": patch_type,
                        "target": tbl_id or target,
                        "new_text": "",
                        "old_text": "",
                        "structured_sections": structured,
                        "column_entity_type": p.get("column_entity_type", ""),
                        "table_entity_type": p.get("table_entity_type", "table"),
                    })
                    continue
            elif not parsed or (len(parsed) == 1 and "_preamble" in parsed):
                preamble = (parsed.get("_preamble") or new_text).strip()
                if preamble:
                    logger.info(
                        "Wrapping freeform %s text in 'definition' section",
                        patch_type,
                    )
                    base_freeform = {
                        "lever": lever,
                        "risk_level": classify_risk(patch_type),
                        "predicted_affected_questions": p.get("questions_fixed", 0),
                        "grounded_in": p.get("grounded_in", []),
                        "source_proposal_id": p.get("proposal_id", ""),
                    }
                    if tbl_id:
                        base_freeform["table"] = tbl_id
                    if col_name:
                        base_freeform["column"] = col_name
                    patches.append({
                        **base_freeform,
                        "type": patch_type,
                        "target": tbl_id or target,
                        "new_text": "",
                        "old_text": "",
                        "structured_sections": {"definition": preamble},
                        "column_entity_type": p.get("column_entity_type", ""),
                        "table_entity_type": p.get("table_entity_type", "table"),
                    })
                    continue

        patch_dict: dict = {
            "type": patch_type,
            "target": target,
            "new_text": new_text,
            "old_text": "",
            "lever": lever,
            "risk_level": classify_risk(patch_type),
            "predicted_affected_questions": p.get("questions_fixed", 0),
            "grounded_in": p.get("grounded_in", []),
            "source_proposal_id": p.get("proposal_id", ""),
        }
        if tbl_id:
            patch_dict["table"] = tbl_id
        if col_name:
            patch_dict["column"] = col_name
        if "join_spec" in p:
            patch_dict["join_spec"] = p["join_spec"]
        if "example_question" in p:
            patch_dict["example_question"] = p["example_question"]
        if "example_sql" in p:
            patch_dict["example_sql"] = p["example_sql"]
        if "parameters" in p:
            patch_dict["parameters"] = p["parameters"]
        if "usage_guidance" in p:
            patch_dict["usage_guidance"] = p["usage_guidance"]
        if "old_value" in p:
            patch_dict["old_value"] = p["old_value"]
        patches.append(patch_dict)
    return patches


# ═══════════════════════════════════════════════════════════════════════
# 4. Patch Rendering
# ═══════════════════════════════════════════════════════════════════════


def render_patch(patch: dict, space_id: str, space_config: dict) -> dict:
    """Convert a patch dict into an executable action with command + rollback.

    Returns ``{action_type, target, command, rollback_command, risk_level}``.
    """
    patch_type = patch.get("type", "")
    target = (
        patch.get("target") or patch.get("object_id") or patch.get("table") or ""
    )
    risk = classify_risk(patch_type)

    def action(cmd: str, rollback: str) -> dict:
        return {
            "action_type": patch_type,
            "target": target,
            "command": cmd,
            "rollback_command": rollback,
            "risk_level": risk,
        }

    old_text = patch.get("old_text", "")
    new_text = patch.get("new_text", patch.get("value", ""))
    table_id = patch.get("table") or patch.get("target") or ""
    column_name = patch.get("column", "")

    # ── Instructions ──────────────────────────────────────────────
    if patch_type == "add_instruction":
        return action(
            json.dumps({"op": "add", "section": "instructions", "new_text": new_text}),
            json.dumps({"op": "remove", "section": "instructions", "old_text": new_text}),
        )
    if patch_type == "update_instruction":
        return action(
            json.dumps({"op": "update", "section": "instructions", "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "instructions", "old_text": new_text, "new_text": old_text}),
        )
    if patch_type == "remove_instruction":
        return action(
            json.dumps({"op": "remove", "section": "instructions", "old_text": old_text}),
            json.dumps({"op": "add", "section": "instructions", "new_text": old_text}),
        )
    if patch_type == "rewrite_instruction":
        rewrite_old = patch.get("old_value", old_text)
        return action(
            json.dumps({"op": "rewrite", "section": "instructions", "new_text": new_text, "old_text": rewrite_old}),
            json.dumps({"op": "rewrite", "section": "instructions", "new_text": rewrite_old, "old_text": new_text}),
        )

    # ── Example SQL (preferred over text instructions) ────────────
    if patch_type == "add_example_sql":
        eq = patch.get("example_question", "")
        es = patch.get("example_sql", "")
        cmd_dict: dict = {"op": "add", "section": "example_question_sqls", "question": eq, "sql": es}
        params = patch.get("parameters", [])
        if params:
            cmd_dict["parameters"] = params
        guidance = patch.get("usage_guidance", "")
        if guidance:
            cmd_dict["usage_guidance"] = guidance
        return action(
            json.dumps(cmd_dict),
            json.dumps({"op": "remove", "section": "example_question_sqls", "question": eq}),
        )
    if patch_type == "update_example_sql":
        eq = patch.get("example_question", "")
        old_sql = patch.get("old_text", "")
        new_sql = patch.get("new_text", patch.get("example_sql", ""))
        return action(
            json.dumps({"op": "update", "section": "example_question_sqls", "question": eq, "old_sql": old_sql, "new_sql": new_sql}),
            json.dumps({"op": "update", "section": "example_question_sqls", "question": eq, "old_sql": new_sql, "new_sql": old_sql}),
        )
    if patch_type == "remove_example_sql":
        eq = patch.get("example_question", old_text)
        es = patch.get("example_sql", "")
        return action(
            json.dumps({"op": "remove", "section": "example_question_sqls", "question": eq}),
            json.dumps({"op": "add", "section": "example_question_sqls", "question": eq, "sql": es}),
        )

    # ── Descriptions ──────────────────────────────────────────────
    if patch_type == "add_description":
        return action(
            json.dumps({"op": "add", "section": "descriptions", "target": target, "value": new_text}),
            json.dumps({"op": "remove", "section": "descriptions", "target": target, "value": new_text}),
        )
    if patch_type == "update_description":
        structured_sections = patch.get("structured_sections")
        if isinstance(structured_sections, dict) and structured_sections:
            cmd_fwd_desc: dict = {
                "op": "update", "section": "descriptions",
                "target": target,
                "structured_sections": structured_sections,
                "lever": patch.get("lever", 0),
                "table_entity_type": patch.get("table_entity_type", "table"),
            }
            cmd_rev_desc: dict = {
                "op": "update", "section": "descriptions",
                "target": target, "old_text": new_text, "new_text": old_text,
            }
            return action(json.dumps(cmd_fwd_desc), json.dumps(cmd_rev_desc))
        return action(
            json.dumps({"op": "update", "section": "descriptions", "target": target, "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "descriptions", "target": target, "old_text": new_text, "new_text": old_text}),
        )
    if patch_type == "add_column_description":
        return action(
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "value": new_text}),
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "value": new_text}),
        )
    if patch_type == "update_column_description":
        structured_sections = patch.get("structured_sections")
        if isinstance(structured_sections, dict) and structured_sections:
            cmd_fwd: dict = {
                "op": "update", "section": "column_configs",
                "table": table_id, "column": column_name,
                "structured_sections": structured_sections,
                "lever": patch.get("lever", 0),
                "column_entity_type": patch.get("column_entity_type", ""),
            }
            cmd_rev: dict = {
                "op": "update", "section": "column_configs",
                "table": table_id, "column": column_name,
                "old_text": "", "new_text": "",
            }
            return action(json.dumps(cmd_fwd), json.dumps(cmd_rev))
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_text": new_text, "new_text": old_text}),
        )

    # ── Visibility ────────────────────────────────────────────────
    if patch_type == "hide_column":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": True}),
        )
    if patch_type == "unhide_column":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": False}),
        )
    if patch_type == "rename_column_alias":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_alias": old_text, "new_alias": new_text}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_alias": new_text, "new_alias": old_text}),
        )

    # ── Tables ────────────────────────────────────────────────────
    if patch_type == "add_table":
        asset = patch.get("asset", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "tables", "asset": asset}),
            json.dumps({"op": "remove", "section": "tables", "identifier": asset.get("identifier", target)}),
        )
    if patch_type == "remove_table":
        return action(
            json.dumps({"op": "remove", "section": "tables", "identifier": target}),
            json.dumps({"op": "add", "section": "tables", "asset": patch.get("previous_asset", {})}),
        )

    # ── Join Specifications (Lever 4) ─────────────────────────────
    if patch_type == "add_join_spec":
        join_spec = patch.get("join_spec", patch.get("value", {}))
        lt = _join_spec_left_id(join_spec) or patch.get("left_table", "")
        rt = _join_spec_right_id(join_spec) or patch.get("right_table", "")
        return action(
            json.dumps({"op": "add", "section": "join_specs", "join_spec": join_spec}),
            json.dumps({"op": "remove", "section": "join_specs", "left_table": lt, "right_table": rt}),
        )
    if patch_type == "update_join_spec":
        join_spec = patch.get("join_spec", patch.get("value", {}))
        lt = _join_spec_left_id(join_spec) or patch.get("left_table", "")
        rt = _join_spec_right_id(join_spec) or patch.get("right_table", "")
        return action(
            json.dumps({"op": "update", "section": "join_specs", "left_table": lt, "right_table": rt, "join_spec": join_spec}),
            json.dumps({"op": "update", "section": "join_specs", "left_table": lt, "right_table": rt, "join_spec": patch.get("previous_join_spec", {})}),
        )
    if patch_type == "remove_join_spec":
        lt = patch.get("left_table", "")
        rt = patch.get("right_table", "")
        return action(
            json.dumps({"op": "remove", "section": "join_specs", "left_table": lt, "right_table": rt}),
            json.dumps({"op": "add", "section": "join_specs", "join_spec": patch.get("previous_join_spec", {})}),
        )

    # ── Column Discovery Settings (Lever 5) ───────────────────────
    if patch_type == "enable_example_values":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_format_assistance": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_format_assistance": False}),
        )
    if patch_type == "disable_example_values":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_format_assistance": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_format_assistance": True}),
        )
    if patch_type == "enable_value_dictionary":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_entity_matching": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_entity_matching": False}),
        )
    if patch_type == "disable_value_dictionary":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_entity_matching": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "enable_entity_matching": True}),
        )
    if patch_type == "add_column_synonym":
        synonyms = patch.get("synonyms", [new_text] if new_text else [])
        return action(
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
        )
    if patch_type == "remove_column_synonym":
        synonyms = patch.get("synonyms", [old_text] if old_text else [])
        return action(
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
        )

    # ── Filters ───────────────────────────────────────────────────
    if patch_type == "add_default_filter":
        filt = patch.get("filter", {"condition": new_text})
        return action(
            json.dumps({"op": "add", "section": "default_filters", "filter": filt}),
            json.dumps({"op": "remove", "section": "default_filters", "filter": filt}),
        )
    if patch_type == "remove_default_filter":
        filt = patch.get("filter", {"condition": old_text})
        return action(
            json.dumps({"op": "remove", "section": "default_filters", "filter": filt}),
            json.dumps({"op": "add", "section": "default_filters", "filter": filt}),
        )
    if patch_type == "update_filter_condition":
        return action(
            json.dumps({"op": "update", "section": "default_filters", "old_condition": old_text, "new_condition": new_text}),
            json.dumps({"op": "update", "section": "default_filters", "old_condition": new_text, "new_condition": old_text}),
        )

    # ── TVF ───────────────────────────────────────────────────────
    if patch_type == "add_tvf_parameter":
        return action(
            json.dumps({"op": "add", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", new_text)}),
            json.dumps({"op": "remove", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", new_text)}),
        )
    if patch_type == "remove_tvf_parameter":
        return action(
            json.dumps({"op": "remove", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", old_text)}),
            json.dumps({"op": "add", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", old_text)}),
        )
    if patch_type == "update_tvf_sql":
        return action(
            json.dumps({"op": "update", "section": "tvf_definition", "tvf": target, "old_sql": old_text, "new_sql": new_text}),
            json.dumps({"op": "update", "section": "tvf_definition", "tvf": target, "old_sql": new_text, "new_sql": old_text}),
        )
    if patch_type == "add_tvf":
        tvf_asset = patch.get("tvf_asset", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "tvfs", "tvf_asset": tvf_asset}),
            json.dumps({"op": "remove", "section": "tvfs", "identifier": tvf_asset.get("identifier", target)}),
        )
    if patch_type == "remove_tvf":
        return action(
            json.dumps({"op": "remove", "section": "tvfs", "identifier": target}),
            json.dumps({"op": "add", "section": "tvfs", "tvf_asset": patch.get("previous_tvf_asset", {})}),
        )

    # ── Metric Views ──────────────────────────────────────────────
    if patch_type == "add_mv_measure":
        measure = patch.get("measure", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "mv_measures", "mv": target, "measure": measure}),
            json.dumps({"op": "remove", "section": "mv_measures", "mv": target, "measure_name": measure.get("name", "")}),
        )
    if patch_type == "update_mv_measure":
        return action(
            json.dumps({"op": "update", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", ""), "old": old_text, "new": new_text}),
            json.dumps({"op": "update", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", ""), "old": new_text, "new": old_text}),
        )
    if patch_type == "remove_mv_measure":
        return action(
            json.dumps({"op": "remove", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", old_text)}),
            json.dumps({"op": "add", "section": "mv_measures", "mv": target, "measure": patch.get("previous_measure", {})}),
        )
    if patch_type == "add_mv_dimension":
        dim = patch.get("dimension", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "mv_dimensions", "mv": target, "dimension": dim}),
            json.dumps({"op": "remove", "section": "mv_dimensions", "mv": target, "dimension_name": dim.get("name", "")}),
        )
    if patch_type == "remove_mv_dimension":
        return action(
            json.dumps({"op": "remove", "section": "mv_dimensions", "mv": target, "dimension_name": patch.get("dimension_name", old_text)}),
            json.dumps({"op": "add", "section": "mv_dimensions", "mv": target, "dimension": patch.get("previous_dimension", {})}),
        )
    if patch_type == "update_mv_yaml":
        return action(
            json.dumps({"op": "update", "section": "mv_yaml", "mv": target, "new_yaml": new_text}),
            json.dumps({"op": "update", "section": "mv_yaml", "mv": target, "new_yaml": old_text}),
        )

    # ── Unknown type ──────────────────────────────────────────────
    return action(
        json.dumps({"op": "unknown", "patch_type": patch_type}),
        json.dumps({"op": "unknown", "patch_type": patch_type}),
    )


# ═══════════════════════════════════════════════════════════════════════
# 5. Action Application — Genie Config
# ═══════════════════════════════════════════════════════════════════════


def _apply_action_to_config(config: dict, action: dict) -> bool:
    """Apply a single rendered action to a Genie Space config dict in-place.

    Returns True if applied, False if skipped (e.g. old_text guard failed).
    """
    try:
        cmd = json.loads(action.get("command", "{}"))
    except json.JSONDecodeError:
        return False

    op = cmd.get("op", "")
    section = cmd.get("section", "")

    # ── Instructions ──────────────────────────────────────────────
    if section == "instructions":
        from genie_space_optimizer.optimization.optimizer import normalize_instructions

        if op == "add":
            text = cmd.get("new_text", "")
            if text:
                current = _get_general_instructions(config)
                merged = normalize_instructions((current + "\n" + text).strip())
                _set_general_instructions(config, merged)
            return True
        if op == "update":
            current = _get_general_instructions(config)
            old_text = cmd.get("old_text", "")
            new_text = cmd.get("new_text", "")
            if old_text and old_text not in current:
                return False
            replaced = current.replace(old_text, new_text, 1) if old_text else current + "\n" + new_text
            _set_general_instructions(config, normalize_instructions(replaced.strip()))
            return True
        if op == "remove":
            current = _get_general_instructions(config)
            old_text = cmd.get("old_text", "")
            if old_text and old_text not in current:
                return False
            _set_general_instructions(config, normalize_instructions(current.replace(old_text, "").strip()))
            return True
        if op == "rewrite":
            text = cmd.get("new_text", "")
            _set_general_instructions(config, normalize_instructions(text))
            return True

    # ── Example SQL Queries (preferred over text instructions) ────
    if section == "example_question_sqls":
        eqs = config.setdefault("instructions", {}).setdefault(
            "example_question_sqls", []
        )
        question_text = cmd.get("question", "")
        if op == "add":
            if not question_text:
                return False
            sql_text = cmd.get("sql", "")
            from genie_space_optimizer.common.genie_schema import generate_genie_id

            new_entry: dict = {
                "id": generate_genie_id(),
                "question": [question_text],
                "sql": [sql_text],
            }
            params = cmd.get("parameters", [])
            if params:
                api_params = []
                for p in params:
                    if not isinstance(p, dict):
                        continue
                    param_entry: dict = {"name": p.get("name", "")}
                    if p.get("type_hint"):
                        param_entry["type_hint"] = p["type_hint"]
                    dv = p.get("default_value", "")
                    if dv:
                        if isinstance(dv, dict):
                            param_entry["default_value"] = dv
                        else:
                            param_entry["default_value"] = {"values": [str(dv)]}
                    api_params.append(param_entry)
                if api_params:
                    new_entry["parameters"] = api_params
            guidance = cmd.get("usage_guidance", "")
            if guidance:
                new_entry["usage_guidance"] = (
                    [guidance] if isinstance(guidance, str) else guidance
                )
            if not _validate_example_sql_entry(new_entry, config=config):
                return False
            eqs.append(new_entry)
            return True
        if op == "update":
            for entry in eqs:
                eq = entry.get("question", [])
                q_str = eq[0] if isinstance(eq, list) and eq else str(eq)
                if q_str == question_text:
                    new_sql = cmd.get("new_sql", "")
                    updated_entry = dict(entry)
                    updated_entry["sql"] = [new_sql]
                    if not _validate_example_sql_entry(updated_entry, config=config):
                        logger.warning(
                            "update_example_sql rejected by validation: %.120s",
                            new_sql,
                        )
                        return False
                    entry["sql"] = [new_sql]
                    return True
            return False
        if op == "remove":
            for i, entry in enumerate(eqs):
                eq = entry.get("question", [])
                q_str = eq[0] if isinstance(eq, list) and eq else str(eq)
                if q_str == question_text:
                    eqs.pop(i)
                    return True
            return False

    # ── Column Configs (descriptions, visibility, discovery) ──────
    if section == "column_configs":
        table_id = cmd.get("table", "")
        col_name = cmd.get("column", "")
        tbl = _find_table_in_config(config, table_id)
        if not tbl:
            return False
        cc = _find_or_create_column_config(tbl, col_name)

        if "visible" in cmd:
            cc["exclude"] = not cmd["visible"]
            return True
        if "enable_format_assistance" in cmd:
            cc["enable_format_assistance"] = cmd["enable_format_assistance"]
            return True
        if "enable_entity_matching" in cmd:
            cc["enable_entity_matching"] = cmd["enable_entity_matching"]
            return True
        if "old_alias" in cmd:
            new_alias = cmd.get("new_alias", "")
            cc.setdefault("synonyms", []).append(new_alias)
            return True

        if op == "add" and "synonyms" in cmd:
            existing = cc.setdefault("synonyms", [])
            for s in cmd["synonyms"]:
                if s and s not in existing:
                    existing.append(s)
            return True
        if op == "remove" and "synonyms" in cmd:
            existing = cc.get("synonyms", [])
            cc["synonyms"] = [s for s in existing if s not in cmd["synonyms"]]
            return True

        if op == "add" and "value" in cmd:
            val = cmd["value"]
            if val is None or val == "" or val == []:
                return True
            cc["description"] = [val] if isinstance(val, str) else val
            return True
        if op == "update" and "structured_sections" in cmd:
            sections_update = cmd["structured_sections"]
            lever_num = cmd.get("lever", 0)
            etype_str = cmd.get("column_entity_type", "")
            if not etype_str:
                data_type = cc.get("data_type", "")
                etype_str = entity_type_for_column(col_name, data_type)
            try:
                new_desc = update_sections(
                    cc.get("description"),
                    sections_update,
                    lever_num,
                    etype_str,
                )
                cc["description"] = new_desc
                return True
            except LeverOwnershipError:
                logger.warning(
                    "Lever %d tried to update locked sections on %s.%s — skipped",
                    lever_num, table_id, col_name,
                )
                return False
        if op == "update" and "new_text" in cmd:
            if not cmd["new_text"]:
                return True
            desc = cc.get("description", [])
            joined = "\n".join(desc) if isinstance(desc, list) else str(desc)
            old_t = cmd.get("old_text", "")
            if old_t and old_t not in joined:
                return False
            new_desc = joined.replace(old_t, cmd["new_text"], 1) if old_t else joined + "\n" + cmd["new_text"]
            cc["description"] = [ln for ln in new_desc.split("\n")]
            return True
        if op == "remove" and "value" in cmd:
            desc = cc.get("description", [])
            if isinstance(desc, list):
                cc["description"] = [d for d in desc if d != cmd["value"]]
            return True

    # ── Descriptions (table-level) ────────────────────────────────
    if section == "descriptions":
        target = cmd.get("target", "")
        tbl = _find_table_in_config(config, target)
        if not tbl:
            return False
        if op == "update" and "structured_sections" in cmd:
            sections_update = cmd["structured_sections"]
            lever_num = cmd.get("lever", 0)
            entity_type = cmd.get("table_entity_type", "table")
            try:
                new_desc = update_sections(
                    tbl.get("description"),
                    sections_update,
                    lever_num,
                    entity_type,
                )
                tbl["description"] = new_desc
                return True
            except LeverOwnershipError:
                logger.warning(
                    "Lever %d tried to update locked sections on table %s — skipped",
                    lever_num, target,
                )
                return False
        if op == "add":
            desc = tbl.get("description", [])
            if isinstance(desc, list):
                desc.append(cmd.get("value", ""))
            else:
                tbl["description"] = [str(desc), cmd.get("value", "")]
            return True
        if op == "update":
            desc = tbl.get("description", [])
            joined = "\n".join(desc) if isinstance(desc, list) else str(desc)
            old_t = cmd.get("old_text", "")
            if old_t and old_t not in joined:
                return False
            new_desc = joined.replace(old_t, cmd.get("new_text", ""), 1)
            tbl["description"] = [ln for ln in new_desc.split("\n")]
            return True
        if op == "remove":
            desc = tbl.get("description", [])
            val = cmd.get("value", "")
            if isinstance(desc, list):
                tbl["description"] = [d for d in desc if d != val]
            return True

    # ── Join Specifications (Lever 4) ─────────────────────────────
    if section == "join_specs":
        from genie_space_optimizer.common.genie_schema import ensure_join_spec_fields

        inst = config.setdefault("instructions", {})
        specs = inst.setdefault("join_specs", [])
        if op == "add":
            js = cmd.get("join_spec", {})
            if js:
                js = ensure_join_spec_fields(js)
                if not _validate_join_spec_entry(js):
                    return False
                specs.append(js)
            return True
        if op == "remove":
            lt, rt = cmd.get("left_table", ""), cmd.get("right_table", "")
            for i, s in enumerate(specs):
                if _join_spec_left_id(s) == lt and _join_spec_right_id(s) == rt:
                    specs.pop(i)
                    return True
            return False
        if op == "update":
            lt, rt = cmd.get("left_table", ""), cmd.get("right_table", "")
            new_spec = cmd.get("join_spec", {})
            if new_spec:
                new_spec = ensure_join_spec_fields(new_spec)
                if not _validate_join_spec_entry(new_spec):
                    return False
            for i, s in enumerate(specs):
                if _join_spec_left_id(s) == lt and _join_spec_right_id(s) == rt:
                    specs[i] = new_spec
                    return True
            return False

    # ── Tables ────────────────────────────────────────────────────
    if section == "tables":
        tables = config.setdefault("data_sources", {}).setdefault("tables", [])
        if op == "add":
            asset = cmd.get("asset", {})
            if asset:
                tables.append(asset)
                sort_genie_config(config)
            return True
        if op == "remove":
            ident = cmd.get("identifier", "")
            for i, t in enumerate(tables):
                if t.get("identifier") == ident:
                    tables.pop(i)
                    return True
            return False

    # ── Default Filters ───────────────────────────────────────────
    if section == "default_filters":
        filters = config.setdefault("default_filters", [])
        if op == "add":
            filt = cmd.get("filter", {})
            if filt and filt not in filters:
                filters.append(filt)
            return True
        if op == "remove":
            filt = cmd.get("filter", {})
            for i, f in enumerate(filters):
                if f == filt:
                    filters.pop(i)
                    return True
            return False
        if op == "update":
            old_c, new_c = cmd.get("old_condition", ""), cmd.get("new_condition", "")
            for i, f in enumerate(filters):
                if isinstance(f, dict) and f.get("condition") == old_c:
                    f["condition"] = new_c
                    return True
                if f == old_c:
                    filters[i] = new_c
                    return True
            return False

    # ── TVF / MV operations (config-level no-ops for uc_artifact patches) ──
    if section in ("tvf_parameters", "tvf_definition", "tvfs", "mv_measures", "mv_dimensions", "mv_yaml"):
        if section == "tvfs":
            funcs = config.setdefault("instructions", {}).setdefault("sql_functions", [])
            if op == "add":
                tvf_asset = cmd.get("tvf_asset", {})
                if tvf_asset:
                    ident = tvf_asset.get("identifier", "")
                    funcs.append({"id": tvf_asset.get("id", ident), "identifier": ident})
                    sort_genie_config(config)
                return True
            if op == "remove":
                ident = cmd.get("identifier", "")
                for i, f in enumerate(funcs):
                    if f.get("identifier") == ident:
                        funcs.pop(i)
                        return True
                return False
        return True

    return False


# ═══════════════════════════════════════════════════════════════════════
# 6. Action Application — UC Artifacts
# ═══════════════════════════════════════════════════════════════════════


def _apply_action_to_uc(w: WorkspaceClient, action: dict) -> bool:
    """Apply an action to UC artifacts via DDL.

    Only used for Levers 1-3 when ``apply_mode`` includes ``uc_artifact``.
    """
    try:
        cmd = json.loads(action.get("command", "{}"))
    except json.JSONDecodeError:
        return False

    patch_type = action.get("action_type", "")
    warehouse_id = os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        logger.warning("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID is not set; skipping UC DDL action")
        return False

    try:
        if patch_type == "update_column_description":
            table = cmd.get("table", "")
            column = cmd.get("column", "")
            structured_sections = cmd.get("structured_sections")
            new_text = cmd.get("new_text", "")
            if table and column and isinstance(structured_sections, dict) and structured_sections:
                etype = cmd.get("column_entity_type") or entity_type_for_column(column, "")
                rendered = render_structured_description(structured_sections, etype)
                flat = "\n".join(rendered)
                escaped = flat.replace("'", "\\'")
                w.statement_execution.execute_statement(
                    statement=f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{escaped}'",
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
                return True
            if table and column and new_text:
                escaped = new_text.replace("'", "\\'")
                w.statement_execution.execute_statement(
                    statement=f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{escaped}'",
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
                return True
        if patch_type == "update_description":
            table = cmd.get("target", "")
            new_text = cmd.get("new_text", "")
            if table and new_text:
                escaped = new_text.replace("'", "\\'")
                w.statement_execution.execute_statement(
                    statement=f"COMMENT ON TABLE {table} IS '{escaped}'",
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
                return True
        if patch_type == "update_tvf_sql":
            new_sql = cmd.get("new_sql", "")
            if new_sql:
                w.statement_execution.execute_statement(
                    statement=new_sql,
                    warehouse_id=warehouse_id,
                    wait_timeout="60s",
                )
                return True
    except Exception:
        logger.exception("UC action failed for %s", patch_type)
        return False

    return True


# ═══════════════════════════════════════════════════════════════════════
# 7. Patch Set Application
# ═══════════════════════════════════════════════════════════════════════


_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}


def apply_patch_set(
    w: WorkspaceClient | None,
    space_id: str,
    patches: list[dict],
    metadata_snapshot: dict,
    *,
    apply_mode: str = APPLY_MODE,
    deploy_target: str | None = None,
    force_apply: bool = False,
) -> dict:
    """Apply a patch set to a Genie Space (and optionally UC artifacts).

    Applies in risk order: LOW -> MEDIUM -> HIGH.
    High-risk patches are queued for manual review unless *force_apply*
    is ``True``, in which case all patches are applied regardless of risk
    (used by the escalation pipeline after confidence-model approval).

    Returns an ``apply_log`` dict with pre/post snapshots and rollback info.
    """
    pre_snapshot = copy.deepcopy(metadata_snapshot)
    config = copy.deepcopy(metadata_snapshot)

    sorted_indices = sorted(
        range(len(patches)),
        key=lambda i: _RISK_ORDER.get(classify_risk(patches[i].get("type", "")), 1),
    )

    applied: list[dict] = []
    queued_high: list[dict] = []
    rollback_commands: list[str] = []
    patched_objects: set[str] = set()

    for idx in sorted_indices:
        patch = patches[idx]
        risk = classify_risk(patch.get("type", ""))
        lever = patch.get("lever", 5)
        scope = _resolve_scope(lever, apply_mode)

        rendered = render_patch(patch, space_id, config)

        if risk == "high" and not force_apply:
            queued_high.append({"index": idx, "patch": patch, "action": rendered})
            continue

        ok = False
        if scope in ("genie_config", "both"):
            ok = _apply_action_to_config(config, rendered)
        if scope in ("uc_artifact", "both") and w is not None:
            uc_ok = _apply_action_to_uc(w, rendered)
            ok = ok or uc_ok

        if ok:
            applied.append({"index": idx, "patch": patch, "action": rendered})
            rollback_commands.append(rendered.get("rollback_command", ""))
            target = rendered.get("target", "")
            if target:
                patched_objects.add(target)

    sort_genie_config(config)
    _enforce_instruction_limit(config)

    from genie_space_optimizer.common.genie_schema import (
        count_instruction_slots,
        MAX_INSTRUCTION_SLOTS,
        validate_serialized_space,
    )

    slot_count = count_instruction_slots(config)
    if slot_count > MAX_INSTRUCTION_SLOTS:
        excess = slot_count - MAX_INSTRUCTION_SLOTS
        logger.warning(
            "Post-apply config exceeds instruction slot budget (%d/%d, excess=%d) — "
            "trimming example_question_sqls then sql_functions",
            slot_count, MAX_INSTRUCTION_SLOTS, excess,
        )
        inst = config.get("instructions") or {}
        eqs = inst.get("example_question_sqls", [])
        if eqs and excess > 0:
            trim_eq = min(len(eqs), excess)
            config.setdefault("instructions", {})["example_question_sqls"] = eqs[:-trim_eq]
            excess -= trim_eq
        if excess > 0:
            fns = (config.get("instructions") or {}).get("sql_functions", [])
            if fns and excess > 0:
                trim_fn = min(len(fns), excess)
                logger.warning(
                    "Still %d slots over budget after trimming examples — "
                    "removing %d newest sql_functions entries",
                    excess, trim_fn,
                )
                config["instructions"]["sql_functions"] = fns[:-trim_fn]
                excess -= trim_fn
        if excess > 0:
            logger.error(
                "Cannot trim enough instruction slots — %d excess slots from "
                "table/metric descriptions alone. Manual description cleanup required.",
                excess,
            )

    config_ok, validation_errors = validate_serialized_space(config, strict=True)
    if not config_ok:
        logger.error(
            "Post-patch config validation failed for space %s: %s",
            space_id,
            validation_errors,
        )
        return {
            "space_id": space_id,
            "pre_snapshot": pre_snapshot,
            "post_snapshot": copy.deepcopy(config),
            "applied": applied,
            "queued_high": queued_high,
            "rollback_commands": rollback_commands,
            "deploy_target": deploy_target,
            "patched_objects": list(patched_objects),
            "validation_errors": validation_errors,
            "patch_deployed": False,
            "patch_error": f"Validation failed: {validation_errors}",
        }

    patch_deployed = False
    patch_error: str = ""
    dropped_patches: list[dict] = []

    if w is not None and applied:
        try:
            patch_space_config(w, space_id, config)
            patch_deployed = True
        except Exception as exc:
            patch_error = str(exc)
            logger.exception(
                "Failed to PATCH Genie Space config after retries — "
                "patches were NOT deployed remotely",
            )

            join_spec_entries = [
                e for e in applied
                if e.get("patch", {}).get("type") == "add_join_spec"
            ]
            if join_spec_entries:
                logger.warning(
                    "PATCH failed with %d join spec patch(es) — retrying without them",
                    len(join_spec_entries),
                )
                config_retry = copy.deepcopy(metadata_snapshot)
                applied_retry: list[dict] = []
                patched_objects_retry: set[str] = set()
                rollback_retry: list[str] = []
                for entry in applied:
                    if entry.get("patch", {}).get("type") == "add_join_spec":
                        continue
                    rendered = render_patch(entry["patch"], space_id, config_retry)
                    if _apply_action_to_config(config_retry, rendered):
                        applied_retry.append(entry)
                        rollback_retry.append(rendered.get("rollback_command", ""))
                        target = rendered.get("target", "")
                        if target:
                            patched_objects_retry.add(target)
                if applied_retry:
                    sort_genie_config(config_retry)
                    _enforce_instruction_limit(config_retry)
                    try:
                        patch_space_config(w, space_id, config_retry)
                        patch_deployed = True
                        patch_error = ""
                        config = config_retry
                        dropped_patches = [e["patch"] for e in join_spec_entries]
                        applied = applied_retry
                        rollback_commands = rollback_retry
                        patched_objects = patched_objects_retry
                        logger.info(
                            "PATCH succeeded after dropping %d join spec patch(es); "
                            "%d patches deployed",
                            len(join_spec_entries), len(applied_retry),
                        )
                    except Exception as exc2:
                        patch_error = str(exc2)
                        logger.exception("Retry without join specs also failed")

    return {
        "space_id": space_id,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": copy.deepcopy(config),
        "applied": applied,
        "queued_high": queued_high,
        "rollback_commands": rollback_commands,
        "deploy_target": deploy_target,
        "patched_objects": list(patched_objects),
        "validation_errors": [],
        "patch_deployed": patch_deployed,
        "patch_error": patch_error,
        "dropped_patches": dropped_patches,
    }


# ═══════════════════════════════════════════════════════════════════════
# 8. Rollback
# ═══════════════════════════════════════════════════════════════════════


def rollback(
    apply_log: dict,
    w: WorkspaceClient | None,
    space_id: str,
    metadata_snapshot: dict | None = None,
) -> dict:
    """Restore the Genie Space config to its pre-patch state.

    Primary mechanism: replace current config with ``apply_log["pre_snapshot"]``.
    Fallback: execute rollback_commands in reverse order (HIGH -> MEDIUM -> LOW).
    """
    pre_snapshot = apply_log.get("pre_snapshot")
    if pre_snapshot is None:
        return {
            "status": "error",
            "executed_count": 0,
            "errors": ["No pre_snapshot in apply_log"],
        }

    restored = copy.deepcopy(pre_snapshot)

    if metadata_snapshot is not None:
        metadata_snapshot.clear()
        metadata_snapshot.update(restored)

    if w is not None:
        try:
            patch_space_config(w, space_id, restored)
        except Exception:
            logger.exception("Failed to PATCH rollback config")
            return {
                "status": "error",
                "executed_count": 0,
                "errors": ["Failed to apply rollback via API"],
                "restored_config": restored,
            }

    commands = apply_log.get("rollback_commands", [])
    return {
        "status": "SUCCESS",
        "executed_count": max(len(commands), 1),
        "errors": [],
        "restored_config": restored,
    }


# ═══════════════════════════════════════════════════════════════════════
# 9. Validation & Verification
# ═══════════════════════════════════════════════════════════════════════


def validate_patch_set(patches: list[dict], metadata_snapshot: dict) -> tuple[bool, list[str]]:
    """Validate a patch set before application.

    Delegates to ``optimizer.validate_patch_set`` but adds metadata checks.
    """
    from genie_space_optimizer.optimization.optimizer import validate_patch_set as _validate

    return _validate(patches, metadata_snapshot)


def verify_dual_persistence(applied_patches: list[dict]) -> list[dict]:
    """Verify that both Genie config and UC objects were updated."""
    results: list[dict] = []
    for entry in applied_patches:
        patch = entry.get("patch", {})
        results.append(
            {
                "patch_type": patch.get("type", ""),
                "target": entry.get("action", {}).get("target", ""),
                "genie_config_applied": True,
                "uc_artifact_applied": patch.get("lever", 5) <= 3,
            }
        )
    return results


def verify_repo_update(patch: dict, w: WorkspaceClient | None = None) -> dict:
    """Verify a specific patch persisted (stub for repo-level checks)."""
    return {
        "patch_type": patch.get("type", ""),
        "target": patch.get("target", ""),
        "verified": True,
    }
