# Lever Loop Applyability Contract Implementation Plan

> **Superseded by:** [`2026-04-30-lever-loop-rca-convergence-plan-v2.md`](./2026-04-30-lever-loop-rca-convergence-plan-v2.md). This document is a milestone record; convergence wiring lives in the v2 plan.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent the lever loop from selecting no-op patch bundles by requiring every cap-eligible patch to be structurally valid, asset-aligned, and dry-run applyable against the current Genie Space metadata snapshot.

**Architecture:** The fix makes "applyability" an explicit control-plane contract between proposal generation, patch expansion, patch capping, and applier execution. First, normalize RCA-generated Lever-1 column proposals into concrete `table` + single bare `column` shapes before `proposals_to_patches(...)`. Second, run the exact patch objects through a dry-run `render_patch(...)` + `_apply_action_to_config(...)` gate before `select_target_aware_causal_patch_cap(...)`. Third, make `no_applied_patches` a deterministic rejected bundle with retry suppression and strategist recovery instead of a silent skip.

**Tech Stack:** Python 3.11, pytest, Databricks Genie Space config dicts, internal `genie_space_optimizer.optimization` modules.

---

## Why This Plan Exists

Two independent runs failed at the same boundary:

- `7now_delivery_analytics_space`: baseline `90.0%`, five iterations attempted, one AG repeated, selected patches `P005#1`, `P006#1`, `P008#1`, zero patches applied, zero evals run, final accuracy `90.0%`.
- `esr_daily_sales_performance_analytics_space`: baseline `64.3%`, five iterations attempted, one buffered AG repeated, selected patches `P002#1`, `P003#1`, `P004#1`, zero patches applied, zero evals run, final accuracy `64.3%`.

Both runs printed the new reconciliation diagnostic:

```text
CAP-VS-APPLIED RECONCILIATION: selected_but_not_applied=(...) applied_but_not_selected=()
```

That means proposals survived grounding and the causal-first cap, but `apply_patch_set(...)` produced no candidate state. The applier was correct to avoid mutating invalid patches; the cap was wrong to rank invalid patches.

The concrete code boundary is:

- `harness.py` converts proposals to patches, expands rewrite splits, then caps `patches` via `select_target_aware_causal_patch_cap(...)`.
- `patch_selection.py` ranks by relevance and `causal_attribution_tier(...)`, but it does not know whether a patch can mutate the current metadata snapshot.
- `applier.py` can render a patch into an action, but `_apply_action_to_config(...)` returns `False` when the target table cannot be found or the action is otherwise a no-op.

Current critical paths:

```python
# harness.py
patches = proposals_to_patches(all_proposals)
patches = _harness_expand_splits(patches)
patches, _patch_cap_decisions = select_target_aware_causal_patch_cap(
    _before_cap,
    target_qids=_patch_cap_target_qids,
    max_patches=MAX_AG_PATCHES,
)
apply_log = apply_patch_set(
    w, space_id, patches, metadata_snapshot, apply_mode=apply_mode,
)
```

```python
# applier.py
def _single_column_target(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], str):
        return value[0].strip()
    return ""
```

```python
# applier.py
if section == "column_configs":
    table_id = cmd.get("table", "")
    col_name = cmd.get("column", "")
    tbl = _find_table_in_config(config, table_id)
    if not tbl:
        return False
```

The missing invariant:

> A patch may enter the causal-first cap only if it can render and dry-run mutate the current metadata snapshot.

---

## File Structure

**Create:**

- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_applyability.py`
  - Pure dry-run applyability helper used by the harness before patch-cap ranking.
  - Owns `PatchApplyabilityDecision`, `check_patch_applyability(...)`, and `filter_applyable_patches(...)`.
- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_shape.py`
  - Pure RCA Lever-1 table/column normalization helper.
  - Owns `normalize_column_proposals(...)` and `ColumnProposalDecision`.
- `packages/genie-space-optimizer/tests/unit/test_patch_applyability.py`
  - Unit tests for dry-run render/apply checks.
- `packages/genie-space-optimizer/tests/unit/test_proposal_shape.py`
  - Unit tests for missing table, missing column, qualified column, multi-column fan-out, and ambiguous table handling.
- `packages/genie-space-optimizer/tests/unit/test_no_applied_recovery.py`
  - Source-level and pure helper tests for no-applied deterministic rejection and strategist recovery hooks.
- `packages/genie-space-optimizer/tests/unit/test_static_replay_applyability_contract.py`
  - Static replay fixtures for the two observed no-op logs.

**Modify:**

- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
  - Normalize RCA column proposals before `proposals_to_patches(all_proposals)`.
  - Run dry-run applyability before `select_target_aware_causal_patch_cap(...)`.
  - Print applyability-drop summary.
  - Treat `no_applied_patches` as deterministic rejection and do not replay the same selected patch IDs.
- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py`
  - Add root-cause-aware bundle composition so behavior failures keep at least one applyable Lever-5/Lever-6 patch when present.
  - Fix cap decision accounting so selected + dropped rows cover every input patch.
- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py`
  - Preserve existing renderer guards.
  - Add typed drop reason metadata to no-op paths that are visible in `apply_log["applier_decisions"]`.
- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier_audit.py`
  - Reuse or extend existing reconciliation helpers to aggregate decision counts for skip-eval output.
- `packages/genie-space-optimizer/tests/unit/test_applier_audit.py`
  - Add decision-count assertions for missing table, invalid column, and no-op actions.
- `packages/genie-space-optimizer/tests/unit/test_proposal_grounding.py`
  - Add cap-eligibility tests that confirm applyability is enforced before ranking.

---

## Execution Order

1. Add dry-run patch applyability helper.
2. Normalize RCA Lever-1 table/column proposal shapes.
3. Gate patch list by applyability before the causal-first cap.
4. Make cap composition root-cause-aware for filter/aggregation failures.
5. Convert `no_applied_patches` from skip to deterministic rejected bundle.
6. Complete applier-decision observability in skip-eval output.
7. Fix cap decision accounting and printed truncation.
8. Add static replay fixtures for the two observed no-op runs.
9. Run focused regression suite and manual one-iteration validation.

---

## Task 1: Add Dry-Run Patch Applyability Helper

**Files:**

- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_applyability.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_patch_applyability.py`

**Why:** `select_target_aware_causal_patch_cap(...)` currently ranks patch dictionaries without knowing if they can mutate the current metadata snapshot. The helper must validate the exact patch object after `proposals_to_patches(...)` and rewrite-split expansion.

- [ ] **Step 1: Write the failing tests**

Create `packages/genie-space-optimizer/tests/unit/test_patch_applyability.py`:

```python
from __future__ import annotations

from genie_space_optimizer.optimization.patch_applyability import (
    PatchApplyabilityDecision,
    check_patch_applyability,
    filter_applyable_patches,
)


def _snapshot() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.mv_esr_store_sales",
                    "name": "mv_esr_store_sales",
                    "column_configs": [
                        {
                            "column_name": "apsd_sales_usd_py_day",
                            "data_type": "DOUBLE",
                            "description": [],
                        },
                        {
                            "column_name": "is_finance_monthly_same_store",
                            "data_type": "STRING",
                            "description": [],
                        },
                    ],
                }
            ]
        },
        "instructions": {"text_instructions": [{"content": "PURPOSE:\n- test"}]},
    }


def test_column_patch_without_table_is_not_applyable() -> None:
    patch = {
        "proposal_id": "P003#1",
        "type": "update_column_description",
        "column": "apsd_sales_usd_py_day",
        "structured_sections": {"description": "Prior-year APSD sales."},
        "lever": 1,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision == PatchApplyabilityDecision(
        proposal_id="P003#1",
        expanded_patch_id="P003#1",
        patch_type="update_column_description",
        target="",
        table="",
        column="apsd_sales_usd_py_day",
        applyable=False,
        reason="missing_table",
        error_excerpt="",
    )


def test_column_patch_with_multi_column_target_is_not_applyable() -> None:
    patch = {
        "proposal_id": "P002#1",
        "type": "update_column_description",
        "table": "main.sales.mv_esr_store_sales",
        "column": ["apsd_sales_usd_py_day", "apsd_sales_usd_day"],
        "structured_sections": {"description": "APSD fields."},
        "lever": 1,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision.applyable is False
    assert decision.reason == "invalid_column_target"
    assert decision.table == "main.sales.mv_esr_store_sales"


def test_column_patch_with_missing_config_table_is_not_applyable() -> None:
    patch = {
        "proposal_id": "P004#1",
        "type": "update_column_description",
        "table": "main.sales.missing_table",
        "column": "apsd_sales_usd_py_day",
        "structured_sections": {"description": "Prior-year APSD sales."},
        "lever": 1,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision.applyable is False
    assert decision.reason == "missing_table"
    assert decision.table == "main.sales.missing_table"


def test_well_formed_column_patch_is_applyable() -> None:
    patch = {
        "proposal_id": "P005#1",
        "type": "update_column_description",
        "table": "main.sales.mv_esr_store_sales",
        "column": "apsd_sales_usd_py_day",
        "structured_sections": {"description": "Prior-year APSD sales."},
        "lever": 1,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision.applyable is True
    assert decision.reason == "applyable"


def test_instruction_section_patch_is_applyable() -> None:
    patch = {
        "proposal_id": "P001#1",
        "type": "update_instruction_section",
        "target": "QUERY RULES",
        "section_name": "QUERY RULES",
        "new_text": "- Always filter same-store APSD questions.",
        "lever": 5,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision.applyable is True
    assert decision.reason == "applyable"


def test_filter_applyable_patches_splits_kept_and_dropped() -> None:
    patches = [
        {
            "proposal_id": "bad",
            "type": "update_column_description",
            "column": "apsd_sales_usd_py_day",
            "structured_sections": {"description": "missing table"},
            "lever": 1,
        },
        {
            "proposal_id": "good",
            "type": "update_instruction_section",
            "target": "QUERY RULES",
            "section_name": "QUERY RULES",
            "new_text": "- Rule",
            "lever": 5,
        },
    ]
    kept, decisions = filter_applyable_patches(
        patches=patches,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert [p["proposal_id"] for p in kept] == ["good"]
    assert [(d.proposal_id, d.applyable, d.reason) for d in decisions] == [
        ("bad", False, "missing_table"),
        ("good", True, "applyable"),
    ]
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_applyability.py -q
```

Expected:

```text
FAILED tests/unit/test_patch_applyability.py::test_column_patch_without_table_is_not_applyable
```

- [ ] **Step 3: Implement `patch_applyability.py`**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_applyability.py`:

```python
"""Dry-run applyability contract for lever-loop patch selection.

This module is intentionally pure from the caller's perspective: it deep-copies
the provided metadata snapshot, renders a patch, and attempts to apply the
rendered action to the copy. It never calls the Genie API and never mutates the
caller's snapshot.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from typing import Any


_COLUMN_PATCH_TYPES = frozenset(
    {
        "add_column_description",
        "update_column_description",
        "add_column_synonym",
        "hide_column",
        "unhide_column",
        "rename_column_alias",
    }
)


@dataclass(frozen=True)
class PatchApplyabilityDecision:
    proposal_id: str
    expanded_patch_id: str
    patch_type: str
    target: str
    table: str
    column: str
    applyable: bool
    reason: str
    error_excerpt: str = ""


def _patch_id(patch: dict[str, Any]) -> str:
    return str(
        patch.get("expanded_patch_id")
        or patch.get("id")
        or patch.get("proposal_id")
        or ""
    )


def _patch_type(patch: dict[str, Any]) -> str:
    return str(patch.get("type") or patch.get("patch_type") or "")


def _target(patch: dict[str, Any]) -> str:
    return str(
        patch.get("target")
        or patch.get("target_object")
        or patch.get("target_table")
        or patch.get("table")
        or ""
    )


def _scalar(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _decision(
    *,
    patch: dict[str, Any],
    applyable: bool,
    reason: str,
    table: str = "",
    column: str = "",
    error: str = "",
) -> PatchApplyabilityDecision:
    pid = _patch_id(patch)
    return PatchApplyabilityDecision(
        proposal_id=str(patch.get("proposal_id") or pid),
        expanded_patch_id=pid,
        patch_type=_patch_type(patch),
        target=_target(patch),
        table=table,
        column=column,
        applyable=applyable,
        reason=reason,
        error_excerpt=str(error)[:500] if error else "",
    )


def _action_command(action: dict[str, Any]) -> dict[str, Any]:
    try:
        parsed = json.loads(str(action.get("command") or "{}"))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def check_patch_applyability(
    *,
    patch: dict[str, Any],
    metadata_snapshot: dict[str, Any],
    space_id: str,
) -> PatchApplyabilityDecision:
    """Return whether ``patch`` can mutate ``metadata_snapshot`` in dry-run."""
    patch_type = _patch_type(patch)
    table = _scalar(patch.get("table") or patch.get("target"))
    raw_column = patch.get("column", "")
    column = _scalar(raw_column)

    if patch_type in _COLUMN_PATCH_TYPES:
        if not table:
            return _decision(
                patch=patch,
                applyable=False,
                reason="missing_table",
                table=table,
                column=column,
            )
        if not column or not isinstance(raw_column, str):
            return _decision(
                patch=patch,
                applyable=False,
                reason="invalid_column_target",
                table=table,
                column=column,
            )

    from genie_space_optimizer.optimization.applier import (
        _apply_action_to_config,
        _find_table_in_config,
        render_patch,
    )

    config_copy = copy.deepcopy(metadata_snapshot or {})
    try:
        rendered = render_patch(patch, space_id, config_copy)
    except RuntimeError as exc:
        return _decision(
            patch=patch,
            applyable=False,
            reason="render_validation_error",
            table=table,
            column=column,
            error=str(exc),
        )
    except Exception as exc:
        return _decision(
            patch=patch,
            applyable=False,
            reason="render_exception",
            table=table,
            column=column,
            error=str(exc),
        )

    command = _action_command(rendered)
    if command.get("section") == "column_configs":
        cmd_table = _scalar(command.get("table"))
        cmd_column = _scalar(command.get("column"))
        if not cmd_table:
            return _decision(
                patch=patch,
                applyable=False,
                reason="missing_table",
                table=cmd_table,
                column=cmd_column,
            )
        if not cmd_column:
            return _decision(
                patch=patch,
                applyable=False,
                reason="invalid_column_target",
                table=cmd_table,
                column=cmd_column,
            )
        if _find_table_in_config(config_copy, cmd_table) is None:
            return _decision(
                patch=patch,
                applyable=False,
                reason="missing_table",
                table=cmd_table,
                column=cmd_column,
            )
        table = cmd_table
        column = cmd_column

    try:
        applied = _apply_action_to_config(config_copy, rendered)
    except Exception as exc:
        return _decision(
            patch=patch,
            applyable=False,
            reason="apply_exception",
            table=table,
            column=column,
            error=str(exc),
        )

    if not applied:
        return _decision(
            patch=patch,
            applyable=False,
            reason="apply_action_returned_false",
            table=table,
            column=column,
        )
    return _decision(
        patch=patch,
        applyable=True,
        reason="applyable",
        table=table,
        column=column,
    )


def filter_applyable_patches(
    *,
    patches: list[dict[str, Any]],
    metadata_snapshot: dict[str, Any],
    space_id: str,
) -> tuple[list[dict[str, Any]], list[PatchApplyabilityDecision]]:
    """Return patches that pass dry-run applyability plus all decisions."""
    kept: list[dict[str, Any]] = []
    decisions: list[PatchApplyabilityDecision] = []
    for patch in patches or []:
        decision = check_patch_applyability(
            patch=patch,
            metadata_snapshot=metadata_snapshot,
            space_id=space_id,
        )
        decisions.append(decision)
        if decision.applyable:
            kept.append(patch)
        else:
            patch["_drop_reason"] = decision.reason
            patch["_applyability_error_excerpt"] = decision.error_excerpt
    return kept, decisions
```

- [ ] **Step 4: Run tests and verify pass**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_applyability.py -q
```

Expected:

```text
6 passed
```

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_applyability.py packages/genie-space-optimizer/tests/unit/test_patch_applyability.py
git commit -m "feat(optimizer): add dry-run patch applyability contract"
```

---

## Task 2: Normalize RCA Lever-1 Column Proposal Shapes

**Files:**

- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_shape.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_proposal_shape.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:11525-11526`

**Why:** RCA-driven Lever-1 proposals in both logs had invalid or incomplete shape:

- `column=[]`
- `column=[a, b, c]`
- `column=[table.column]`
- scalar `column` but no `table`

The applier can only mutate column config when it receives one concrete table and one concrete column.

- [ ] **Step 1: Write failing tests**

Create `packages/genie-space-optimizer/tests/unit/test_proposal_shape.py`:

```python
from __future__ import annotations

from genie_space_optimizer.optimization.proposal_shape import (
    normalize_column_proposals,
)


def _uc_columns() -> list[dict]:
    return [
        {
            "table_full_name": "main.sales.mv_esr_store_sales",
            "table_name": "mv_esr_store_sales",
            "column_name": "apsd_sales_usd_py_day",
        },
        {
            "table_full_name": "main.sales.mv_esr_store_sales",
            "table_name": "mv_esr_store_sales",
            "column_name": "apsd_sales_usd_day",
        },
        {
            "table_full_name": "main.sales.mv_7now_fact_sales",
            "table_name": "mv_7now_fact_sales",
            "column_name": "time_window",
        },
        {
            "table_full_name": "main.sales.mv_7now_store_sales",
            "table_name": "mv_7now_store_sales",
            "column_name": "time_window",
        },
    ]


def _proposal(**overrides: object) -> dict:
    base = {
        "id": "P001",
        "proposal_id": "P001",
        "patch_type": "update_column_description",
        "type": "update_column_description",
        "lever": 1,
        "rca_id": "rca_q1_measure_swap",
        "target_qids": ["q1"],
        "column_description": ["description"],
    }
    base.update(overrides)
    return base


def test_empty_column_is_dropped_with_reason() -> None:
    out, decisions = normalize_column_proposals(
        [_proposal(column=[])],
        uc_columns=_uc_columns(),
    )
    assert out == []
    assert decisions[0]["decision"] == "dropped"
    assert decisions[0]["reason"] == "missing_column"


def test_multi_column_list_fans_out_into_single_column_proposals() -> None:
    out, decisions = normalize_column_proposals(
        [
            _proposal(
                column=["apsd_sales_usd_py_day", "apsd_sales_usd_day"],
                table="main.sales.mv_esr_store_sales",
            )
        ],
        uc_columns=_uc_columns(),
    )
    assert [p["column"] for p in out] == [
        "apsd_sales_usd_py_day",
        "apsd_sales_usd_day",
    ]
    assert [p["table"] for p in out] == [
        "main.sales.mv_esr_store_sales",
        "main.sales.mv_esr_store_sales",
    ]
    assert [p["proposal_id"] for p in out] == ["P001#col1", "P001#col2"]
    assert decisions[0]["decision"] == "expanded"
    assert decisions[0]["reason"] == "multi_column_fanout"


def test_qualified_column_splits_table_and_column() -> None:
    out, decisions = normalize_column_proposals(
        [_proposal(column="mv_7now_fact_sales.time_window")],
        uc_columns=_uc_columns(),
    )
    assert len(out) == 1
    assert out[0]["table"] == "main.sales.mv_7now_fact_sales"
    assert out[0]["column"] == "time_window"
    assert decisions[0]["decision"] == "normalized"
    assert decisions[0]["reason"] == "qualified_column_split"


def test_missing_table_is_inferred_when_unique_column_match_exists() -> None:
    out, decisions = normalize_column_proposals(
        [_proposal(column="apsd_sales_usd_py_day")],
        uc_columns=_uc_columns(),
    )
    assert len(out) == 1
    assert out[0]["table"] == "main.sales.mv_esr_store_sales"
    assert out[0]["column"] == "apsd_sales_usd_py_day"
    assert decisions[0]["decision"] == "normalized"
    assert decisions[0]["reason"] == "inferred_table_from_uc_columns"


def test_missing_table_is_dropped_when_column_match_is_ambiguous() -> None:
    out, decisions = normalize_column_proposals(
        [_proposal(column="time_window")],
        uc_columns=_uc_columns(),
    )
    assert out == []
    assert decisions[0]["decision"] == "dropped"
    assert decisions[0]["reason"] == "ambiguous_table_for_column"


def test_non_column_proposal_passes_through() -> None:
    proposal = {
        "proposal_id": "P010",
        "type": "add_instruction",
        "lever": 5,
        "proposed_value": "Add rule",
    }
    out, decisions = normalize_column_proposals(
        [proposal],
        uc_columns=_uc_columns(),
    )
    assert out == [proposal]
    assert decisions == []
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_proposal_shape.py -q
```

Expected:

```text
FAILED tests/unit/test_proposal_shape.py::test_empty_column_is_dropped_with_reason
```

- [ ] **Step 3: Implement `proposal_shape.py`**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_shape.py`:

```python
"""Normalize RCA-generated column proposal shapes before patch expansion."""

from __future__ import annotations

import copy
from typing import Any


_COLUMN_PATCH_TYPES = frozenset({"update_column_description", "add_column_synonym"})


def _proposal_id(proposal: dict[str, Any]) -> str:
    return str(proposal.get("proposal_id") or proposal.get("id") or "")


def _patch_type(proposal: dict[str, Any]) -> str:
    return str(proposal.get("patch_type") or proposal.get("type") or "")


def _column_value(proposal: dict[str, Any]) -> Any:
    return (
        proposal.get("column")
        or proposal.get("column_name")
        or proposal.get("target_column")
        or proposal.get("target")
    )


def _table_value(proposal: dict[str, Any]) -> str:
    raw = proposal.get("table") or proposal.get("target_table") or ""
    return str(raw).strip() if raw is not None else ""


def _decision(
    proposal: dict[str, Any],
    *,
    decision: str,
    reason: str,
    output_count: int = 0,
) -> dict[str, Any]:
    return {
        "proposal_id": _proposal_id(proposal),
        "patch_type": _patch_type(proposal),
        "decision": decision,
        "reason": reason,
        "output_count": int(output_count),
    }


def _uc_matches_for_column(
    column: str,
    uc_columns: list[dict[str, Any]],
) -> list[str]:
    matches: list[str] = []
    for row in uc_columns or []:
        if str(row.get("column_name") or "").strip() != column:
            continue
        table = str(
            row.get("table_full_name")
            or row.get("table")
            or row.get("table_name")
            or ""
        ).strip()
        if table and table not in matches:
            matches.append(table)
    return matches


def _resolve_qualified_column(
    value: str,
    uc_columns: list[dict[str, Any]],
) -> tuple[str, str] | None:
    if "." not in value:
        return None
    table_part, column = value.rsplit(".", 1)
    table_part = table_part.strip()
    column = column.strip()
    if not table_part or not column:
        return None
    for row in uc_columns or []:
        row_column = str(row.get("column_name") or "").strip()
        if row_column != column:
            continue
        full_name = str(row.get("table_full_name") or row.get("table") or "").strip()
        table_name = str(row.get("table_name") or "").strip()
        if table_part in {full_name, table_name} or full_name.endswith("." + table_part):
            return full_name or table_part, column
    return table_part, column


def _normalise_one(
    proposal: dict[str, Any],
    *,
    column: str,
    table: str,
    uc_columns: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    qualified = _resolve_qualified_column(column, uc_columns)
    if qualified is not None:
        table, column = qualified
        out = copy.deepcopy(proposal)
        out["table"] = table
        out["column"] = column
        out["target"] = table
        return out, "qualified_column_split"

    if not table:
        matches = _uc_matches_for_column(column, uc_columns)
        if len(matches) == 1:
            table = matches[0]
            out = copy.deepcopy(proposal)
            out["table"] = table
            out["column"] = column
            out["target"] = table
            return out, "inferred_table_from_uc_columns"
        if len(matches) > 1:
            return None, "ambiguous_table_for_column"
        return None, "missing_table_for_column"

    out = copy.deepcopy(proposal)
    out["table"] = table
    out["column"] = column
    out["target"] = table
    return out, "already_concrete"


def normalize_column_proposals(
    proposals: list[dict[str, Any]],
    *,
    uc_columns: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize RCA column proposals into renderable table/column shapes."""
    output: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    for proposal in proposals or []:
        if _patch_type(proposal) not in _COLUMN_PATCH_TYPES:
            output.append(proposal)
            continue

        raw_column = _column_value(proposal)
        table = _table_value(proposal)
        if raw_column in (None, "", []):
            decisions.append(_decision(proposal, decision="dropped", reason="missing_column"))
            continue

        if isinstance(raw_column, list):
            columns = [str(c).strip() for c in raw_column if str(c).strip()]
            if not columns:
                decisions.append(_decision(proposal, decision="dropped", reason="missing_column"))
                continue
            if len(columns) > 1:
                expanded: list[dict[str, Any]] = []
                for idx, column in enumerate(columns, start=1):
                    child, reason = _normalise_one(
                        proposal,
                        column=column,
                        table=table,
                        uc_columns=uc_columns,
                    )
                    if child is None:
                        decisions.append(
                            _decision(proposal, decision="dropped", reason=reason)
                        )
                        continue
                    pid = _proposal_id(proposal)
                    child["proposal_id"] = f"{pid}#col{idx}" if pid else f"col{idx}"
                    child["source_proposal_id"] = pid
                    expanded.append(child)
                output.extend(expanded)
                decisions.append(
                    _decision(
                        proposal,
                        decision="expanded",
                        reason="multi_column_fanout",
                        output_count=len(expanded),
                    )
                )
                continue
            raw_column = columns[0]

        if not isinstance(raw_column, str):
            decisions.append(_decision(proposal, decision="dropped", reason="invalid_column_target"))
            continue

        child, reason = _normalise_one(
            proposal,
            column=raw_column.strip(),
            table=table,
            uc_columns=uc_columns,
        )
        if child is None:
            decisions.append(_decision(proposal, decision="dropped", reason=reason))
            continue
        output.append(child)
        if reason != "already_concrete":
            decisions.append(_decision(proposal, decision="normalized", reason=reason, output_count=1))

    return output, decisions
```

- [ ] **Step 4: Wire normalization into `harness.py`**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, replace the single line:

```python
        patches = proposals_to_patches(all_proposals)
```

with:

```python
        try:
            from genie_space_optimizer.optimization.proposal_shape import (
                normalize_column_proposals,
            )

            _uc_columns_for_shape = (
                metadata_snapshot.get("_uc_columns", [])
                if isinstance(metadata_snapshot, dict)
                else []
            )
            all_proposals, _shape_decisions = normalize_column_proposals(
                all_proposals,
                uc_columns=_uc_columns_for_shape,
            )
            if _shape_decisions:
                print(
                    _section(f"[{ag_id}] RCA COLUMN SHAPE NORMALIZATION", "-") + "\n"
                    + _kv("Decisions", len(_shape_decisions)) + "\n"
                    + "\n".join(
                        f"|  - {d['proposal_id']} ({d['patch_type']}): "
                        f"{d['decision']} reason={d['reason']} outputs={d['output_count']}"
                        for d in _shape_decisions[:12]
                    ) + "\n"
                    + _bar("-")
                )
        except Exception:
            logger.debug(
                "RCA column proposal normalization failed (non-fatal)",
                exc_info=True,
            )

        patches = proposals_to_patches(all_proposals)
```

- [ ] **Step 5: Run tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_proposal_shape.py -q
```

Expected:

```text
6 passed
```

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_shape.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_proposal_shape.py
git commit -m "fix(optimizer): normalize RCA column proposal shapes"
```

---

## Task 3: Gate Patches By Applyability Before The Causal Cap

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:11975-12033`
- Test: `packages/genie-space-optimizer/tests/unit/test_patch_applyability.py`

**Why:** The cap should not be allowed to select patches that are known to no-op. The applyability gate must run before `select_target_aware_causal_patch_cap(...)` and must also run when `len(patches) <= MAX_AG_PATCHES`.

- [ ] **Step 1: Add source-level harness test**

Append to `packages/genie-space-optimizer/tests/unit/test_patch_applyability.py`:

```python
def test_harness_filters_applyable_patches_before_patch_cap() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    filter_idx = source.index("filter_applyable_patches(")
    cap_idx = source.index("select_target_aware_causal_patch_cap(")
    apply_idx = source.index("apply_log = apply_patch_set(")

    assert filter_idx < cap_idx < apply_idx
    snippet = source[filter_idx - 600 : filter_idx + 1800]
    assert "_applyability_decisions" in snippet
    assert "PATCH APPLYABILITY GATE" in snippet
    assert "applyable=False" in snippet
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_applyability.py::test_harness_filters_applyable_patches_before_patch_cap -q
```

Expected:

```text
FAILED tests/unit/test_patch_applyability.py::test_harness_filters_applyable_patches_before_patch_cap
```

- [ ] **Step 3: Insert applyability gate in `harness.py`**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, insert this block after rewrite-split expansion and before the `if len(patches) > MAX_AG_PATCHES:` block:

```python
        try:
            from genie_space_optimizer.optimization.patch_applyability import (
                filter_applyable_patches,
            )

            _patches_before_applyability = list(patches)
            patches, _applyability_decisions = filter_applyable_patches(
                patches=_patches_before_applyability,
                metadata_snapshot=metadata_snapshot,
                space_id=space_id,
            )
            _non_applyable_decisions = [
                d for d in _applyability_decisions if not d.applyable
            ]
            if _non_applyable_decisions:
                print(
                    _section(f"[{ag_id}] PATCH APPLYABILITY GATE", "-") + "\n"
                    + _kv("Input patches", len(_patches_before_applyability)) + "\n"
                    + _kv("Applyable patches", len(patches)) + "\n"
                    + _kv("Dropped patches", len(_non_applyable_decisions)) + "\n"
                    + "\n".join(
                        f"|  - {d.expanded_patch_id or d.proposal_id} "
                        f"{d.patch_type} target={d.target or '(none)'} "
                        f"table={d.table or '(none)'} column={d.column or '(none)'} "
                        f"applyable={d.applyable} reason={d.reason}"
                        for d in _non_applyable_decisions[:12]
                    ) + "\n"
                    + _bar("-")
                )
                logger.warning(
                    "AG %s patch applyability gate dropped %d/%d patch(es)",
                    ag_id,
                    len(_non_applyable_decisions),
                    len(_patches_before_applyability),
                )
        except Exception:
            logger.debug(
                "Patch applyability gate failed (non-fatal)",
                exc_info=True,
            )
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_applyability.py -q
```

Expected:

```text
7 passed
```

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_patch_applyability.py
git commit -m "fix(optimizer): gate patch cap by dry-run applyability"
```

---

## Task 4: Preserve Direct Lever-5/Lever-6 Fixes For Behavior Failures

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py:232-327`
- Test: `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`

**Why:** In both logs, direct behavior fixes existed but the cap selected only Lever-1 metadata patches. For `missing_filter`, `wrong_filter_condition`, `wrong_aggregation`, and `wrong_measure`, a bundle of only metadata patches is too weak when applyable Lever-5/Lever-6 fixes exist.

- [ ] **Step 1: Add failing tests**

Append to `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`:

```python
def test_behavior_failure_cap_preserves_direct_lever6_patch() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P001#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_q1",
            "target_qids": ["q1"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P002#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_q1",
            "target_qids": ["q1"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P003#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_q1",
            "target_qids": ["q1"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P023#1",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "relevance_score": 0.9,
            "target_qids": ["q1"],
            "root_cause": "missing_filter",
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q1",),
        max_patches=3,
    )

    selected_ids = {p["proposal_id"] for p in selected}
    assert "P023#1" in selected_ids
    selected_reasons = {
        d["proposal_id"]: d["selection_reason"]
        for d in decisions
        if d["decision"] == "selected"
    }
    assert selected_reasons["P023#1"] == "behavior_direct_fix_reserved"


def test_non_behavior_failure_keeps_existing_causal_ranking() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P001#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_q1",
            "target_qids": ["q1"],
            "root_cause": "column_disambiguation",
        },
        {
            "proposal_id": "P006#1",
            "type": "add_instruction",
            "lever": 5,
            "relevance_score": 0.1,
            "target_qids": ["q1"],
            "root_cause": "column_disambiguation",
        },
    ]

    selected, _decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q1",),
        max_patches=1,
    )

    assert [p["proposal_id"] for p in selected] == ["P001#1"]
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_selection.py::test_behavior_failure_cap_preserves_direct_lever6_patch -q
```

Expected:

```text
FAILED tests/unit/test_patch_selection.py::test_behavior_failure_cap_preserves_direct_lever6_patch
```

- [ ] **Step 3: Implement behavior direct-fix reservation**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py`, add helpers above `select_target_aware_causal_patch_cap(...)`:

```python
_BEHAVIOR_ROOT_CAUSES = frozenset(
    {
        "missing_filter",
        "wrong_filter_condition",
        "wrong_aggregation",
        "wrong_measure",
    }
)


def _root_cause(patch: dict[str, Any]) -> str:
    raw = patch.get("root_cause") or patch.get("rca_kind") or ""
    return str(raw).strip().split(":", 1)[0]


def _is_direct_behavior_patch(patch: dict[str, Any]) -> bool:
    if _root_cause(patch) not in _BEHAVIOR_ROOT_CAUSES:
        return False
    if _lever(patch) in {5, 6}:
        return True
    patch_type = str(patch.get("type") or patch.get("patch_type") or "")
    return patch_type in {
        "add_instruction",
        "update_instruction_section",
        "add_sql_snippet_filter",
        "add_sql_snippet_measure",
        "add_sql_snippet_calculation",
    }
```

Then in `select_target_aware_causal_patch_cap(...)`, after `selected_ids: set[str] = set()` and before the `for target in target_set:` loop, add:

```python
    reserved_direct_fix_pids: set[str] = set()
    if max_patches > 0:
        direct_candidates = [
            (idx, patch)
            for idx, patch in enumerate(patches)
            if _is_direct_behavior_patch(patch)
        ]
        if direct_candidates:
            idx, patch = min(
                direct_candidates,
                key=lambda item: (
                    -_score(item[1], "relevance_score"),
                    -causal_attribution_tier(item[1]),
                    _risk_rank(item[1]),
                    -_score(item[1], "confidence"),
                    item[0],
                ),
            )
            selected.append(patch)
            pid = _proposal_id(patch, idx)
            selected_ids.add(pid)
            reserved_direct_fix_pids.add(pid)
```

Then change the decision construction at lines 315-320 so selected direct-fix rows get the explicit reason:

```python
        if selected_flag and pid in reserved_direct_fix_pids:
            selection_reason = "behavior_direct_fix_reserved"
        elif selected_flag:
            selection_reason = "target_coverage"
        else:
            selection_reason = "lower_causal_rank"
        decisions.append({
            "proposal_id": pid,
            "decision": "selected" if selected_flag else "dropped",
            "selection_reason": selection_reason,
            "rank": rank_by_pid.get(pid),
            "relevance_score": _score(patch, "relevance_score"),
            "lever": _lever(patch),
            **_identity_fields(patch, pid),
        })
```

- [ ] **Step 4: Run patch-selection tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_selection.py -q
```

Expected:

```text
... passed
```

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py packages/genie-space-optimizer/tests/unit/test_patch_selection.py
git commit -m "fix(optimizer): reserve direct fixes for behavior failures"
```

---

## Task 5: Treat No-Applied Bundles As Deterministic Rejections

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:12182-12190`
- Create: `packages/genie-space-optimizer/tests/unit/test_no_applied_recovery.py`

**Why:** A bundle that produces `applied=[]` after dry-run gating and capping is deterministic evidence that the selected IDs are dead-on-arrival. It must not be treated as a neutral skip that can replay the same AG for five iterations.

- [ ] **Step 1: Add source-level tests**

Create `packages/genie-space-optimizer/tests/unit/test_no_applied_recovery.py`:

```python
from __future__ import annotations


def test_harness_marks_no_applied_bundle_as_dead_on_arrival() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    skip_idx = source.index("_should_skip_eval_for_patch_bundle(")
    snippet = source[skip_idx - 800 : skip_idx + 2200]

    assert "deterministic_no_applied_patches" in snippet
    assert "_dead_on_arrival_patch_signatures" in source
    assert "_dead_on_arrival_ag_ids" in source
    assert "all_selected_patches_dropped_by_applier" in snippet
    assert "pending_action_groups = []" in snippet
    assert "pending_strategy = None" in snippet


def test_harness_blocks_retry_of_same_dead_patch_signature() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    assert "_selected_patch_signature = tuple(sorted(" in source
    assert "_selected_patch_signature in _dead_on_arrival_patch_signatures" in source
    assert "Skipping dead-on-arrival AG retry" in source
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_no_applied_recovery.py -q
```

Expected:

```text
FAILED tests/unit/test_no_applied_recovery.py::test_harness_marks_no_applied_bundle_as_dead_on_arrival
```

- [ ] **Step 3: Add dead-on-arrival tracking sets**

In `_run_lever_loop`, near the other per-run state variables before the adaptive loop, add:

```python
    _dead_on_arrival_patch_signatures: set[tuple[str, ...]] = set()
    _dead_on_arrival_ag_ids: set[str] = set()
```

- [ ] **Step 4: Block retry after patch cap selection**

After patch cap selection and before pre-AG snapshot capture, add:

```python
        _selected_patch_signature = tuple(sorted(
            str(p.get("expanded_patch_id") or p.get("id") or p.get("proposal_id") or "")
            for p in patches
            if (p.get("expanded_patch_id") or p.get("id") or p.get("proposal_id"))
        ))
        if _selected_patch_signature in _dead_on_arrival_patch_signatures:
            logger.warning(
                "Skipping dead-on-arrival AG retry for %s with patch signature %s",
                ag_id,
                _selected_patch_signature,
            )
            print(
                _section(f"[{ag_id}] DEAD-ON-ARRIVAL RETRY BLOCKED", "!") + "\n"
                + _kv("Patch signature", _selected_patch_signature) + "\n"
                + _kv("Reason", "same selected patch IDs already produced no applied patches") + "\n"
                + _bar("!")
            )
            pending_action_groups = []
            pending_strategy = None
            continue
```

- [ ] **Step 5: Convert no-applied skip into deterministic rejection**

Inside the `_apply_skip.skip` block immediately after `_should_skip_eval_for_patch_bundle(...)`, before `continue`, add:

```python
            if _apply_skip.reason_code == "no_applied_patches":
                _dead_on_arrival_ag_ids.add(str(ag_id))
                _dead_on_arrival_patch_signatures.add(_selected_patch_signature)
                logger.warning(
                    "AG %s deterministic_no_applied_patches: selected patch "
                    "signature=%s recovery_reason=all_selected_patches_dropped_by_applier",
                    ag_id,
                    _selected_patch_signature,
                )
                print(
                    _section(f"[{ag_id}] DETERMINISTIC REJECTION: NO APPLIED PATCHES", "!") + "\n"
                    + _kv("Reason", "all_selected_patches_dropped_by_applier") + "\n"
                    + _kv("Selected patch signature", _selected_patch_signature) + "\n"
                    + _kv("Action", "discard buffered AG and force strategist recovery") + "\n"
                    + _bar("!")
                )
                pending_action_groups = []
                pending_strategy = None
```

Keep the existing skip-eval print after this block so operators still see the old headline plus the new deterministic rejection details.

- [ ] **Step 6: Run tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_no_applied_recovery.py -q
```

Expected:

```text
2 passed
```

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_no_applied_recovery.py
git commit -m "fix(optimizer): reject no-applied patch bundles deterministically"
```

---

## Task 6: Print Applier Decision Counts On Skip Eval

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier_audit.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:12182-12210`
- Test: `packages/genie-space-optimizer/tests/unit/test_applier_audit.py`

**Why:** The reconciliation says selected IDs were not applied, but it does not explain whether they were missing a table, invalid column, queued high-risk, or no-op.

- [ ] **Step 1: Add failing tests**

Append to `packages/genie-space-optimizer/tests/unit/test_applier_audit.py`:

```python
def test_applier_decision_counts_groups_reasons() -> None:
    from genie_space_optimizer.optimization.applier_audit import (
        applier_decision_counts,
    )

    decisions = [
        {"decision": "dropped_validation", "reason": "missing_table"},
        {"decision": "dropped_validation", "reason": "missing_table"},
        {"decision": "dropped_no_op", "reason": "apply_action_returned_false"},
    ]

    assert applier_decision_counts(decisions) == {
        "dropped_validation:missing_table": 2,
        "dropped_no_op:apply_action_returned_false": 1,
    }


def test_harness_prints_applier_decisions_on_skip_eval() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    skip_idx = source.index("_should_skip_eval_for_patch_bundle(")
    snippet = source[skip_idx : skip_idx + 2400]
    assert "APPLIER DECISIONS" in snippet
    assert "applier_decision_counts(" in snippet
    assert "apply_log.get(\"applier_decisions\")" in snippet
```

- [ ] **Step 2: Implement `applier_decision_counts`**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier_audit.py`, append:

```python
def applier_decision_counts(decisions: list[dict]) -> dict[str, int]:
    """Return grouped decision/reason counts for operator diagnostics."""
    counts: dict[str, int] = {}
    for row in decisions or []:
        decision = str(row.get("decision") or "unknown")
        reason = str(row.get("reason") or "unknown")
        key = f"{decision}:{reason}"
        counts[key] = counts.get(key, 0) + 1
    return counts
```

- [ ] **Step 3: Print counts in skip-eval block**

Inside `harness.py`, in the `_apply_skip.skip` block, add:

```python
            try:
                from genie_space_optimizer.optimization.applier_audit import (
                    applier_decision_counts,
                )

                _applier_decisions = apply_log.get("applier_decisions") or []
                _decision_counts = applier_decision_counts(_applier_decisions)
                if _decision_counts:
                    print(
                        _section(f"[{ag_id}] APPLIER DECISIONS", "-") + "\n"
                        + "\n".join(
                            f"|  {key}: {value}"
                            for key, value in sorted(_decision_counts.items())
                        ) + "\n"
                        + _bar("-")
                    )
            except Exception:
                logger.debug("Failed to print applier decision counts", exc_info=True)
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_applier_audit.py -q
```

Expected:

```text
... passed
```

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier_audit.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_applier_audit.py
git commit -m "feat(optimizer): print applier decision counts on skipped eval"
```

---

## Task 7: Fix Cap Decision Accounting And Truncation Disclosure

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:12007-12033`
- Test: `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`

**Why:** Logs show `Original size: 26`, `Kept: 3`, and only 8 displayed dropped IDs. Operators interpreted this as missing cap decisions. If display is intentionally truncated, the log must say so and include counts.

- [ ] **Step 1: Add source-level test**

Append to `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`:

```python
def test_harness_patch_cap_log_discloses_dropped_count_and_truncation() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    cap_idx = source.index("PATCH CAP APPLIED (causal-first)")
    snippet = source[cap_idx - 800 : cap_idx + 1200]
    assert "Dropped count" in snippet
    assert "Dropped shown" in snippet
    assert "Dropped truncated" in snippet
```

- [ ] **Step 2: Update cap print block**

In `harness.py`, replace the printed cap block fields for dropped IDs with:

```python
                + _kv("Dropped count", len(_dropped_decisions)) + "\n"
                + _kv("Dropped shown", min(len(_dropped_decisions), 8)) + "\n"
                + _kv("Dropped truncated", len(_dropped_decisions) > 8) + "\n"
                + _kv(
                    "Dropped proposal_ids",
                    [d.get("proposal_id") for d in _dropped_decisions[:8]]
                    if _dropped_decisions else "(none)",
                ) + "\n"
```

- [ ] **Step 3: Run test**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_selection.py::test_harness_patch_cap_log_discloses_dropped_count_and_truncation -q
```

Expected:

```text
1 passed
```

- [ ] **Step 4: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_patch_selection.py
git commit -m "chore(optimizer): disclose patch-cap truncation counts"
```

---

## Task 8: Add Static Replay Fixtures For Both No-Op Runs

**Files:**

- Create: `packages/genie-space-optimizer/tests/unit/test_static_replay_applyability_contract.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/static_judge_replay.py` only if the existing helper needs one small pure entrypoint.

**Why:** The two observed logs should become permanent regression fixtures: one for malformed columns, one for missing tables. The replay must prove the cap can no longer choose an all-non-applyable bundle.

- [ ] **Step 1: Create replay tests**

Create `packages/genie-space-optimizer/tests/unit/test_static_replay_applyability_contract.py`:

```python
from __future__ import annotations

from genie_space_optimizer.optimization.patch_applyability import (
    filter_applyable_patches,
)
from genie_space_optimizer.optimization.patch_selection import (
    select_target_aware_causal_patch_cap,
)


def _snapshot() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.mv_7now_fact_sales",
                    "name": "mv_7now_fact_sales",
                    "column_configs": [{"column_name": "time_window", "description": []}],
                },
                {
                    "identifier": "main.sales.mv_7now_store_sales",
                    "name": "mv_7now_store_sales",
                    "column_configs": [{"column_name": "time_window", "description": []}],
                },
                {
                    "identifier": "main.sales.mv_esr_store_sales",
                    "name": "mv_esr_store_sales",
                    "column_configs": [
                        {"column_name": "apsd_sales_usd_py_day", "description": []},
                        {"column_name": "apsd_sales_usd_day", "description": []},
                        {"column_name": "is_finance_monthly_same_store", "description": []},
                    ],
                },
            ]
        },
        "instructions": {"text_instructions": [{"content": "PURPOSE:\n- test"}]},
    }


def test_7now_malformed_l1_bundle_cannot_displace_applyable_filter() -> None:
    patches = [
        {
            "proposal_id": "P005#1",
            "type": "update_column_description",
            "table": "main.sales.mv_7now_store_sales",
            "column": [],
            "structured_sections": {"description": "bad"},
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_gs_013",
            "target_qids": ["gs_013"],
            "root_cause": "wrong_aggregation",
        },
        {
            "proposal_id": "P006#1",
            "type": "update_column_description",
            "table": "main.sales.mv_7now_store_sales",
            "column": ["zone_combination", "7now_avg_txn_diff_day"],
            "structured_sections": {"description": "bad"},
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_gs_013",
            "target_qids": ["gs_013"],
            "root_cause": "wrong_aggregation",
        },
        {
            "proposal_id": "P013#1",
            "type": "update_instruction_section",
            "target": "QUERY RULES",
            "section_name": "QUERY RULES",
            "new_text": "- Use mv_7now_fact_sales.time_window = 'mtd'.",
            "lever": 5,
            "relevance_score": 0.8,
            "target_qids": ["gs_021"],
            "root_cause": "missing_filter",
        },
    ]
    applyable, decisions = filter_applyable_patches(
        patches=patches,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    selected, _cap_decisions = select_target_aware_causal_patch_cap(
        applyable,
        target_qids=("gs_013", "gs_021"),
        max_patches=3,
    )
    assert {d.reason for d in decisions if not d.applyable} == {
        "invalid_column_target"
    }
    assert [p["proposal_id"] for p in selected] == ["P013#1"]


def test_esr_missing_table_l1_bundle_cannot_displace_applyable_instruction() -> None:
    patches = [
        {
            "proposal_id": "P003#1",
            "type": "update_column_description",
            "column": "apsd_sales_usd_py_day",
            "structured_sections": {"description": "missing table"},
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_gs_002",
            "target_qids": ["gs_002"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P004#1",
            "type": "update_column_description",
            "column": "apsd_sales_usd_day",
            "structured_sections": {"description": "missing table"},
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_gs_002",
            "target_qids": ["gs_002"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P023#1",
            "type": "update_instruction_section",
            "target": "QUERY RULES",
            "section_name": "QUERY RULES",
            "new_text": "- APSD KPI questions must filter is_finance_monthly_same_store = 'Y'.",
            "lever": 5,
            "relevance_score": 0.9,
            "target_qids": ["gs_002"],
            "root_cause": "missing_filter",
        },
    ]
    applyable, decisions = filter_applyable_patches(
        patches=patches,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    selected, _cap_decisions = select_target_aware_causal_patch_cap(
        applyable,
        target_qids=("gs_002",),
        max_patches=3,
    )
    assert {d.reason for d in decisions if not d.applyable} == {"missing_table"}
    assert [p["proposal_id"] for p in selected] == ["P023#1"]
```

- [ ] **Step 2: Run replay tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_static_replay_applyability_contract.py -q
```

Expected:

```text
2 passed
```

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/test_static_replay_applyability_contract.py
git commit -m "test(optimizer): replay no-op applyability regressions"
```

---

## Task 9: Focused Regression Suite And Manual Validation

**Files:**

- No new source files.
- This task validates the complete applyability contract.

- [ ] **Step 1: Run focused unit tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_patch_applyability.py \
  tests/unit/test_proposal_shape.py \
  tests/unit/test_patch_selection.py \
  tests/unit/test_applier_audit.py \
  tests/unit/test_no_applied_recovery.py \
  tests/unit/test_static_replay_applyability_contract.py \
  -q
```

Expected:

```text
all selected tests pass
```

- [ ] **Step 2: Run existing optimizer invariant tests touched by this plan**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_quarantine_control_plane.py \
  tests/unit/test_static_judge_replay.py \
  tests/unit/test_proposal_grounding.py \
  -q
```

Expected:

```text
all selected tests pass
```

- [ ] **Step 3: Manual one-iteration validation on a previously failing space**

Run a single lever-loop attempt against either of the two failing spaces and save logs:

```bash
# Save standard output and standard error from the Databricks job task as:
/tmp/gso_applyability.stdout.log
/tmp/gso_applyability.stderr.log
```

Verify applyability gate appears:

```bash
grep -A15 "PATCH APPLYABILITY GATE" /tmp/gso_applyability.stdout.log
```

Expected:

```text
Input patches ...
Applyable patches ...
Dropped patches ...
```

Verify cap does not select non-applyable patches:

```bash
grep -A8 "PATCH CAP APPLIED" /tmp/gso_applyability.stdout.log
```

Expected:

```text
Selected proposal_ids: ...
Dropped count: ...
Dropped truncated: ...
```

Verify no repeated no-op AG loop:

```bash
grep -E "DETERMINISTIC REJECTION: NO APPLIED PATCHES|DEAD-ON-ARRIVAL RETRY BLOCKED" /tmp/gso_applyability.stdout.log
```

Expected if a no-op bundle remains:

```text
[AG...] DETERMINISTIC REJECTION: NO APPLIED PATCHES
```

The same AG and same selected patch signature must not appear five times.

Verify reconciliation is clean or has typed reasons:

```bash
grep -A8 "CAP-VS-APPLIED RECONCILIATION" /tmp/gso_applyability.stdout.log
grep -A8 "APPLIER DECISIONS" /tmp/gso_applyability.stdout.log
```

Expected:

```text
Selected but not applied: (none)
```

or, if still non-empty, an adjacent `APPLIER DECISIONS` block explaining exact reasons.

- [ ] **Step 4: Commit validation notes if the repo tracks run evidence**

If run-evidence notes are tracked in this repo, create a short markdown note in `packages/genie-space-optimizer/docs/` with the four grep outputs. If run evidence is not tracked, skip this step.

---

## Self-Review

**Spec coverage**

| Requirement from analysis | Task |
|---|---|
| Pre-cap applyability gate | Task 1 + Task 3 |
| Harden Lever-1 RCA proposal generation | Task 2 |
| Reject list/stringified/empty/missing table column targets | Task 2 |
| Cap cannot select only non-applyable patches | Task 3 + Task 8 |
| Avoid all-metadata bundle for filter/aggregation RCAs | Task 4 |
| Treat no-applied patches as deterministic rejection | Task 5 |
| Improve logs with applier decision counts and reasons | Task 6 |
| Fix misleading cap dropped-ID truncation | Task 7 |
| Static replay for both observed runs | Task 8 |
| Manual validation greps | Task 9 |

**Placeholder scan**

This plan intentionally uses concrete file paths, function names, test names, commands, and expected outputs. It does not use `TBD`, `TODO`, `fill in details`, or `similar to Task N`.

**Type consistency**

- `PatchApplyabilityDecision` is defined in Task 1 and referenced by Tasks 3 and 8.
- `filter_applyable_patches(...)` is defined in Task 1 and wired into `harness.py` in Task 3.
- `normalize_column_proposals(...)` is defined in Task 2 and wired before `proposals_to_patches(all_proposals)`.
- `behavior_direct_fix_reserved` is introduced in Task 4 and tested in `test_patch_selection.py`.
- `deterministic_no_applied_patches` and `all_selected_patches_dropped_by_applier` are introduced in Task 5 and used in validation greps in Task 9.

**Codebase grounding**

The plan is grounded in the current code paths:

- `proposal_grounding.select_patch_bundle(...)` currently filters relevance/alignment only.
- `applier.proposals_to_patches(...)` drops invalid column targets and requires `tbl_id and col_name`.
- `applier.render_patch(...)` creates `column_configs` actions with table and column fields.
- `applier._apply_action_to_config(...)` returns `False` when `_find_table_in_config(...)` misses.
- `harness._run_lever_loop(...)` currently caps before applying and now prints cap-vs-applied reconciliation, but does not yet act on no-applied bundles.

The implementation should fix the root cause without changing judge prompts, benchmark SQL, rollback verification, or arbiter behavior.
