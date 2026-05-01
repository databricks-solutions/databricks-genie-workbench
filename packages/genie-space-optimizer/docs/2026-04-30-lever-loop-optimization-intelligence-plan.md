# Lever Loop Optimization Intelligence Implementation Plan

> **Superseded by:** [`2026-04-30-lever-loop-rca-convergence-plan-v2.md`](./2026-04-30-lever-loop-rca-convergence-plan-v2.md). This document is a milestone record; convergence wiring lives in the v2 plan.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the RCA-driven lever loop choose the most causal direct fixes, learn from rejected SQL improvements, and avoid suppressing or quarantining still-fixable hard failures.

**Architecture:** This builds on the applyability contract by adding a second layer of optimization intelligence after patches are renderable: cluster-causal cap ranking, precise reflection retry signatures, SQL-shape delta memory, authoritative diagnostic AG replay, and conservative terminal/quarantine handling. The implementation keeps pure ranking, retry, SQL-delta, and guardrail logic in small modules with unit tests, then threads their decisions through `harness.py` only at orchestration boundaries.

**Tech Stack:** Python 3.11, pytest, Databricks Genie Space config dicts, MLflow evaluation rows, existing `genie_space_optimizer.optimization` modules.

---

## Why This Plan Exists

Recent lever-loop runs show the optimizer has moved past the structural no-op failure where selected patches could not apply. The remaining misses are optimization-intelligence failures:

- The cap can still pick applyable but off-causal L5/L6 patches while dropping the direct SQL-shape fix for the active hard cluster.
- Reflection retry suppression is keyed too broadly, so one rolled-back metadata patch can block later, more specific proposals on the same table.
- Rejected candidates are treated as binary failures even when their SQL moved closer to ground truth.
- Diagnostic action groups created for coverage gaps are advisory instead of authoritative.
- RCA cards and archetypes need to explicitly cover extra defensive filters, null-group preservation, and recent-window temporal deltas.
- `previous_sql` and `repeatability` are visible consistency diagnostics, but they should not vote as root-cause drivers.
- Pre-arbiter regression should block broad SQL degradation when no target was fixed.
- Quarantine should not hide a hard failure when the latest rejected candidate produced a concrete, narrow SQL delta.

This plan is intentionally generalizable. It does not special-case `gs_026`, `tkt_payment`, `PAYMENT_CURRENCY_CD`, or any one Genie Space. Those examples only motivate optimizer-wide invariants.

## File Structure

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py`
  - Owns cap ranking and reservation policy for cluster-causal direct L5/L6 behavior patches.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_asset_alignment.py`
  - Owns stricter asset and column alignment checks for L6 snippets and example SQLs.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_grounding.py`
  - Converts counterfactual passing-dependent scans from diagnostics into gates for risky L5/L6 behavior patches.
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/reflection_retry.py`
  - Owns precise retry signatures and retry-allowance decisions for previously rolled-back patches.
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_shape_delta.py`
  - Owns compact, leak-safe SQL-shape delta extraction from accepted baseline, rejected candidate, and ground truth SQL.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
  - Threads the new pure decisions into strategist memory, patch filtering, diagnostic AG queues, acceptance guardrails, and visible logs.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca.py`
  - Extends RCA detection and patch themes for extra equality filters, null guards, and recent-window temporal deltas.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/archetypes.py`
  - Adds a leak-safe `recent_window_days` archetype.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/cluster_driven_synthesis.py`
  - Emits typed synthesis decline reasons and routes recent-window RCA to the new archetype.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/judge_classes.py`
  - Downgrades `previous_sql` and `repeatability` to zero-weight meta judges.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py`
  - Adds pure pre-arbiter regression guardrail logic and baseline identity helpers.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_terminal.py`
  - Adds terminal status for unresolved hard failures with untried SQL deltas.
- Test: `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_proposal_asset_alignment.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_proposal_grounding.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_reflection_retry.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_sql_shape_delta.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_rca_extra_defensive_filters.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_judge_classes.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_control_plane.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_terminal_status_taxonomy.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_static_replay_optimization_intelligence.py`

## Task 1: Cluster-Causal L5/L6 Patch Selection Fidelity

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_asset_alignment.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_grounding.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_proposal_asset_alignment.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_proposal_grounding.py`

- [ ] **Step 1: Add failing cap-ranking tests for active-cluster direct behavior patches**

Append these tests to `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`:

```python
def test_active_cluster_direct_behavior_patch_beats_peripheral_rca_patch():
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "metadata-payment-amount",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 0.99,
            "rca_id": "rca-payment",
            "target_qids": ["gs_026"],
            "primary_cluster_id": "H002",
            "source_cluster_ids": ["H002"],
            "target_table": "cat.sch.tkt_payment",
            "column": "PAYMENT_AMT",
        },
        {
            "proposal_id": "snippet-document-base-fare",
            "type": "add_sql_snippet_expression",
            "lever": 6,
            "relevance_score": 1.0,
            "rca_id": "rca-document",
            "target_qids": ["gs_026"],
            "primary_cluster_id": "H999",
            "source_cluster_ids": ["H999"],
            "target_table": "cat.sch.tkt_document",
            "column": "BASE_FARE_AMT",
        },
        {
            "proposal_id": "direct-payment-filter",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "root_cause": "wrong_filter_condition",
            "relevance_score": 0.80,
            "rca_id": "rca-payment",
            "target_qids": ["gs_026"],
            "primary_cluster_id": "H002",
            "source_cluster_ids": ["H002"],
            "target_table": "cat.sch.tkt_payment",
            "column": "PAYMENT_CURRENCY_CD",
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=2,
        active_cluster_ids=("H002",),
    )

    assert [p["proposal_id"] for p in selected] == [
        "direct-payment-filter",
        "metadata-payment-amount",
    ]
    by_id = {d["proposal_id"]: d for d in decisions}
    assert by_id["direct-payment-filter"]["selection_reason"] == "active_cluster_direct_behavior_reserved"
    assert by_id["snippet-document-base-fare"]["decision"] == "dropped"
```

- [ ] **Step 2: Run the failing cap-ranking test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_patch_selection.py::test_active_cluster_direct_behavior_patch_beats_peripheral_rca_patch -q
```

Expected: FAIL with `TypeError: select_target_aware_causal_patch_cap() got an unexpected keyword argument 'active_cluster_ids'`.

- [ ] **Step 3: Implement active-cluster ranking and direct-behavior reservation**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py`, update the helper signatures and sort keys:

```python
def _cluster_ids(patch: dict[str, Any]) -> tuple[str, ...]:
    raw: list[Any] = []
    raw.extend(patch.get("source_cluster_ids") or [])
    raw.append(patch.get("primary_cluster_id"))
    raw.append(patch.get("cluster_id"))
    return tuple(dict.fromkeys(str(v) for v in raw if str(v)))


def _active_cluster_match_tier(
    patch: dict[str, Any],
    active_cluster_ids: tuple[str, ...],
) -> int:
    active = {str(cid) for cid in active_cluster_ids or () if str(cid)}
    if not active:
        return 0
    patch_clusters = set(_cluster_ids(patch))
    if patch.get("primary_cluster_id") and str(patch.get("primary_cluster_id")) in active:
        return 2
    if patch_clusters & active:
        return 1
    return 0
```

Change `select_causal_patch_cap(...)` to accept `active_cluster_ids: tuple[str, ...] = ()` and include `-_active_cluster_match_tier(patch, active_cluster_ids)` immediately before `-causal_attribution_tier(patch)` in the sort key.

Change `select_target_aware_causal_patch_cap(...)` to accept the same optional `active_cluster_ids` parameter. In the direct candidate reservation sort key, put active cluster match before relevance:

```python
key=lambda item: (
    -_active_cluster_match_tier(item[1], active_cluster_ids),
    -_score(item[1], "relevance_score"),
    -causal_attribution_tier(item[1]),
    _risk_rank(item[1]),
    -_score(item[1], "confidence"),
    item[0],
)
```

When the selected direct behavior patch has `_active_cluster_match_tier(...) > 0`, emit `selection_reason = "active_cluster_direct_behavior_reserved"` instead of `behavior_direct_fix_reserved`.

- [ ] **Step 4: Thread active cluster IDs from the harness into the cap**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, find the call to `select_target_aware_causal_patch_cap(...)` and pass the current AG source clusters:

```python
_active_cluster_ids_for_cap = tuple(
    str(cid) for cid in (ag.get("source_cluster_ids") or []) if str(cid)
)

selected_patches, cap_decisions = select_target_aware_causal_patch_cap(
    applyable_patches,
    target_qids=_target_qids,
    max_patches=MAX_AG_PATCHES,
    active_cluster_ids=_active_cluster_ids_for_cap,
)
```

If the local variable names differ because the applyability plan has been implemented with different names, preserve the same semantics: the list passed to the cap must already contain only applyable patches, and `active_cluster_ids` must come from the AG currently being executed.

- [ ] **Step 5: Add failing L6 asset-alignment test**

Append this test to `packages/genie-space-optimizer/tests/unit/test_proposal_asset_alignment.py`:

```python
def test_l6_snippet_must_align_with_cluster_asset_without_cross_asset_justification():
    from genie_space_optimizer.optimization.proposal_asset_alignment import (
        proposal_aligns_with_cluster,
    )

    cluster = {
        "cluster_id": "H002",
        "blame_assets": ["cat.sch.tkt_payment"],
        "reference_assets": [],
        "lineage_assets": ["cat.sch.tkt_payment"],
    }
    patch = {
        "type": "add_sql_snippet_expression",
        "lever": 6,
        "target_table": "cat.sch.tkt_document",
        "column": "BASE_FARE_AMT",
    }

    decision = proposal_aligns_with_cluster(patch, cluster)

    assert decision["aligned"] is False
    assert decision["reason"] == "asset_not_in_cluster_lineage"
```

- [ ] **Step 6: Run the failing asset-alignment test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_proposal_asset_alignment.py::test_l6_snippet_must_align_with_cluster_asset_without_cross_asset_justification -q
```

Expected: FAIL if current target extraction misses `target_table`/column-specific L6 targets, or PASS if Task 1 from the quarantine/applier-audit plan already covers it. If it passes, continue to Step 7 because the stricter gate still needs integration.

- [ ] **Step 7: Strengthen L6 alignment for snippets and examples**

In `proposal_asset_alignment.py`, add a stricter helper:

```python
_STRICT_ASSET_PATCH_TYPES = frozenset({
    "add_sql_snippet_filter",
    "add_sql_snippet_measure",
    "add_sql_snippet_expression",
    "add_sql_snippet_calculation",
    "add_example_sql",
})


def l5_l6_patch_requires_asset_alignment(patch: dict | None) -> bool:
    if not isinstance(patch, dict):
        return False
    ptype = str(patch.get("type") or patch.get("patch_type") or "")
    try:
        lever = int(patch.get("lever", 0) or 0)
    except (TypeError, ValueError):
        lever = 0
    return ptype in _STRICT_ASSET_PATCH_TYPES or lever in {5, 6}
```

Then update `proposal_aligns_with_cluster(...)` so missing proposal assets remain allowed for metadata patches, but not for strict L5/L6 SQL-shape patches when a cluster has lineage:

```python
    if not proposal_assets:
        if l5_l6_patch_requires_asset_alignment(patch) and cluster_assets:
            return {
                "aligned": False,
                "reason": "strict_patch_missing_target_asset",
                "proposal_assets": proposal_assets,
                "cluster_assets": cluster_assets,
            }
        return {
            "aligned": True,
            "reason": "no_lineage_constraint",
            "proposal_assets": proposal_assets,
            "cluster_assets": cluster_assets,
        }
```

- [ ] **Step 8: Add failing counterfactual gate test for high-blast-radius L6 patches**

Append this test to `packages/genie-space-optimizer/tests/unit/test_proposal_grounding.py`:

```python
def test_l6_counterfactual_blast_radius_gate_rejects_many_outside_dependents():
    from genie_space_optimizer.optimization.proposal_grounding import (
        patch_blast_radius_is_safe,
    )

    patch = {
        "type": "add_sql_snippet_expression",
        "lever": 6,
        "target_qids": ["gs_026"],
        "passing_dependents": ["gs_001", "gs_002", "gs_003", "gs_004"],
        "target_dependents": ["gs_026"],
    }

    decision = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("gs_026",),
        max_outside_target=0,
    )

    assert decision["safe"] is False
    assert decision["reason"] == "blast_radius_exceeds_threshold"
    assert decision["passing_dependents_outside_target"] == [
        "gs_001",
        "gs_002",
        "gs_003",
        "gs_004",
    ]
```

- [ ] **Step 9: Run the counterfactual gate test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_proposal_grounding.py::test_l6_counterfactual_blast_radius_gate_rejects_many_outside_dependents -q
```

Expected: PASS if the current helper already gates `passing_dependents`, otherwise FAIL with a mismatched reason or `safe=True`.

- [ ] **Step 10: Ensure the harness applies the blast-radius and asset gates before cap selection**

In `harness.py`, keep these gates in this order before `select_target_aware_causal_patch_cap(...)`:

```python
_grounded_cap_candidates: list[dict] = []
for _patch in applyable_patches:
    _blast = patch_blast_radius_is_safe(
        _patch,
        ag_target_qids=_target_qids,
        max_outside_target=0,
    )
    if not _blast.get("safe"):
        _emit_applier_or_grounding_decision(
            _patch,
            decision="dropped",
            reason_code=str(_blast.get("reason") or "blast_radius_rejected"),
            reason_detail=json.dumps(_blast, default=str)[:1000],
        )
        continue

    _cluster = _cluster_by_id_for_alignment.get(
        str((_patch.get("source_cluster_ids") or [""])[0])
    )
    _align = proposal_aligns_with_cluster(_patch, _cluster)
    if not _align.get("aligned"):
        _emit_applier_or_grounding_decision(
            _patch,
            decision="dropped",
            reason_code=str(_align.get("reason") or "asset_alignment_rejected"),
            reason_detail=json.dumps(_align, default=str)[:1000],
        )
        continue

    _grounded_cap_candidates.append(_patch)
```

Use the repository's existing audit-emission helper if it has a different name. Do not silently drop a patch; every pre-cap drop needs a decision row or visible log line.

- [ ] **Step 11: Run the selection and grounding tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest \
  tests/unit/test_patch_selection.py \
  tests/unit/test_proposal_asset_alignment.py \
  tests/unit/test_proposal_grounding.py -q
```

Expected: PASS.

- [ ] **Step 12: Commit Task 1**

```bash
git add \
  src/genie_space_optimizer/optimization/patch_selection.py \
  src/genie_space_optimizer/optimization/proposal_asset_alignment.py \
  src/genie_space_optimizer/optimization/proposal_grounding.py \
  src/genie_space_optimizer/optimization/harness.py \
  tests/unit/test_patch_selection.py \
  tests/unit/test_proposal_asset_alignment.py \
  tests/unit/test_proposal_grounding.py
git commit -m "fix: prioritize cluster-causal direct behavior patches"
```

## Task 2: Precise Reflection Retry Keys

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/reflection_retry.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_reflection_retry.py`

- [ ] **Step 1: Write failing tests for precise retry signatures**

Create `packages/genie-space-optimizer/tests/unit/test_reflection_retry.py`:

```python
from genie_space_optimizer.optimization.reflection_retry import (
    patch_retry_signature,
    retry_allowed_after_rollback,
)


def test_patch_retry_signature_includes_column_and_instruction_section():
    patch = {
        "type": "update_column_description",
        "table": "cat.sch.tkt_payment",
        "column": "PAYMENT_AMT",
        "instruction_section": "QUERY CONSTRUCTION",
    }

    assert patch_retry_signature(patch) == (
        "update_column_description",
        "cat.sch.tkt_payment",
        "PAYMENT_AMT",
        frozenset({"QUERY CONSTRUCTION"}),
    )


def test_retry_not_blocked_for_different_column_on_same_table():
    previous = {
        "type": "update_column_description",
        "target_table": "cat.sch.tkt_payment",
        "column": "PAYMENT_CURRENCY_CD",
    }
    current = {
        "type": "update_column_description",
        "target_table": "cat.sch.tkt_payment",
        "column": "PAYMENT_AMT",
    }

    decision = retry_allowed_after_rollback(
        current_patch=current,
        rolled_back_patches=[previous],
        rollback_cause="insufficient_gain",
    )

    assert decision.allowed is True
    assert decision.reason == "new_precise_patch_signature"


def test_retry_allowed_when_bundle_adds_direct_l6_behavior_patch():
    previous = {
        "type": "update_column_description",
        "target_table": "cat.sch.tkt_payment",
        "column": "PAYMENT_AMT",
    }
    current = {
        "type": "add_sql_snippet_filter",
        "lever": 6,
        "target_table": "cat.sch.tkt_payment",
        "column": "PAYMENT_CURRENCY_CD",
        "root_cause": "wrong_filter_condition",
    }

    decision = retry_allowed_after_rollback(
        current_patch=current,
        rolled_back_patches=[previous],
        rollback_cause="target_still_hard",
    )

    assert decision.allowed is True
    assert decision.reason == "adds_direct_behavior_shape"
```

- [ ] **Step 2: Run the failing reflection retry tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_reflection_retry.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.reflection_retry'`.

- [ ] **Step 3: Implement precise retry helpers**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/reflection_retry.py`:

```python
"""Precise retry signatures for reflection-as-validator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RetryDecision:
    allowed: bool
    reason: str


def _patch_type(patch: dict[str, Any]) -> str:
    return str(patch.get("type") or patch.get("patch_type") or "").strip()


def _target_table(patch: dict[str, Any]) -> str:
    return str(
        patch.get("target_table")
        or patch.get("table")
        or patch.get("target_object")
        or patch.get("target")
        or ""
    ).strip()


def _target_column(patch: dict[str, Any]) -> str:
    value = patch.get("column") or patch.get("target_column") or ""
    if isinstance(value, str):
        return value.strip()
    return ""


def _section_set(patch: dict[str, Any]) -> frozenset[str]:
    raw = (
        patch.get("structured_section_set")
        or patch.get("instruction_sections")
        or patch.get("instruction_section")
        or patch.get("section_name")
        or []
    )
    if isinstance(raw, str):
        raw = [raw]
    return frozenset(str(v).strip() for v in raw if str(v).strip())


def patch_retry_signature(patch: dict[str, Any]) -> tuple[str, str, str, frozenset[str]]:
    """Return a precise retry key for one patch shape."""
    return (_patch_type(patch), _target_table(patch), _target_column(patch), _section_set(patch))


_DIRECT_BEHAVIOR_TYPES = frozenset({
    "add_instruction",
    "update_instruction_section",
    "add_sql_snippet_filter",
    "add_sql_snippet_measure",
    "add_sql_snippet_expression",
    "add_example_sql",
})


def _is_direct_behavior_patch(patch: dict[str, Any]) -> bool:
    ptype = _patch_type(patch)
    try:
        lever = int(patch.get("lever", 0) or 0)
    except (TypeError, ValueError):
        lever = 0
    root = str(patch.get("root_cause") or patch.get("rca_kind") or "").strip()
    return ptype in _DIRECT_BEHAVIOR_TYPES and lever in {5, 6} and bool(root)


def retry_allowed_after_rollback(
    *,
    current_patch: dict[str, Any],
    rolled_back_patches: list[dict[str, Any]],
    rollback_cause: str,
) -> RetryDecision:
    """Decide whether reflection should allow a patch after a rollback."""
    current_sig = patch_retry_signature(current_patch)
    previous_sigs = {patch_retry_signature(p) for p in rolled_back_patches}
    if current_sig not in previous_sigs:
        if _is_direct_behavior_patch(current_patch):
            return RetryDecision(True, "adds_direct_behavior_shape")
        return RetryDecision(True, "new_precise_patch_signature")
    if rollback_cause in {"infra_schema_failure", "insufficient_gain", "target_still_hard"}:
        return RetryDecision(True, f"retry_allowed_for_{rollback_cause}")
    return RetryDecision(False, "same_harmful_patch_signature")
```

- [ ] **Step 4: Replace coarse `_patch_forbidden` construction in harness**

In `harness.py`, replace the tuple shape `set[tuple[str, str]]` in the reflection-as-validator block with `patch_retry_signature(...)`. Store rolled-back patch dictionaries on reflection entries when building them:

```python
from genie_space_optimizer.optimization.reflection_retry import (
    patch_retry_signature,
    retry_allowed_after_rollback,
)

_rolled_back_patches_by_signature: dict[tuple[str, str, str, frozenset[str]], list[dict]] = {}
for _rb in reflection_buffer:
    if _rb.get("accepted"):
        continue
    for _patch in _rb.get("patches") or _rb.get("selected_patches") or []:
        _rolled_back_patches_by_signature.setdefault(
            patch_retry_signature(_patch),
            [],
        ).append(_patch)
```

When evaluating a proposal, call `retry_allowed_after_rollback(...)` with the prior patches sharing that signature. Drop only when `allowed is False`. Keep the existing `escalation_justification` bypass, but make it a bypass of a precise key, not a table-wide ban.

- [ ] **Step 5: Record rollback cause separately from retry signatures**

In `_build_reflection_entry(...)`, add a stable field:

```python
"rollback_cause": (
    rollback_reason
    or rollback_class
    or "unknown"
),
```

When building rejection entries after acceptance gate failures, map reasons as follows:

```python
_rollback_cause = {
    "rejected_unbounded_collateral": "harmful_regression",
    "target_qids_not_improved": "target_still_hard",
    "no_post_arbiter_gain": "insufficient_gain",
    "missing_pre_rows": "infra_schema_failure",
}.get(str(_control_plane_decision.reason_code), "insufficient_gain")
```

- [ ] **Step 6: Run reflection tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_reflection_retry.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

```bash
git add \
  src/genie_space_optimizer/optimization/reflection_retry.py \
  src/genie_space_optimizer/optimization/harness.py \
  tests/unit/test_reflection_retry.py
git commit -m "fix: make reflection retry signatures precise"
```

## Task 3: SQL-Shape Delta Memory For Rejected Action Groups

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_shape_delta.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_sql_shape_delta.py`

- [ ] **Step 1: Write failing SQL-delta tests**

Create `packages/genie-space-optimizer/tests/unit/test_sql_shape_delta.py`:

```python
from genie_space_optimizer.optimization.sql_shape_delta import compute_sql_shape_delta


def test_sql_shape_delta_detects_removed_extra_filter_and_remaining_window_delta():
    accepted = """
    SELECT payment_method, SUM(PAYMENT_AMT)
    FROM cat.sch.tkt_payment
    WHERE PAYMENT_CURRENCY_CD = 'USD'
      AND transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), 29) AND CURRENT_DATE()
    GROUP BY payment_method
    """
    candidate = """
    SELECT payment_method, SUM(PAYMENT_AMT)
    FROM cat.sch.tkt_payment
    WHERE transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), 29) AND CURRENT_DATE()
    GROUP BY payment_method
    """
    ground_truth = """
    SELECT payment_method, SUM(PAYMENT_AMT)
    FROM cat.sch.tkt_payment
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY payment_method
    """

    delta = compute_sql_shape_delta(
        target_qid="gs_026",
        accepted_sql=accepted,
        candidate_sql=candidate,
        ground_truth_sql=ground_truth,
        accepted_row_count=4,
        candidate_row_count=7,
    )

    assert delta["target_qid"] == "gs_026"
    assert "removed_filter: PAYMENT_CURRENCY_CD = 'USD'" in delta["improved"]
    assert "row_count: 4 -> 7" in delta["improved"]
    assert "date_window: 29_vs_30" in delta["remaining"]
    assert "predicate_form: between_vs_gte" in delta["remaining"]
    assert delta["next_hint"] == (
        "teach recent_window_days archetype with DATE_SUB(CURRENT_DATE(), 30)"
    )
```

- [ ] **Step 2: Run the failing SQL-delta tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_sql_shape_delta.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.sql_shape_delta'`.

- [ ] **Step 3: Implement compact SQL-delta extraction**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_shape_delta.py`:

```python
"""Leak-safe SQL-shape deltas for rejected lever-loop candidates."""

from __future__ import annotations

import re
from typing import Any


_EQUALITY_FILTER_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=\s*'([^']*)'",
    re.IGNORECASE,
)
_DATE_SUB_RE = re.compile(
    r"DATE_SUB\s*\(\s*CURRENT_DATE\s*\(\s*\)\s*,\s*(\d+)\s*\)",
    re.IGNORECASE,
)


def _normalize_sql(sql: str) -> str:
    return " ".join(str(sql or "").replace("`", "").split())


def _equality_filters(sql: str) -> set[str]:
    norm = _normalize_sql(sql)
    return {f"{m.group(1)} = '{m.group(2)}'" for m in _EQUALITY_FILTER_RE.finditer(norm)}


def _date_sub_days(sql: str) -> set[int]:
    return {int(m.group(1)) for m in _DATE_SUB_RE.finditer(_normalize_sql(sql))}


def compute_sql_shape_delta(
    *,
    target_qid: str,
    accepted_sql: str,
    candidate_sql: str,
    ground_truth_sql: str,
    accepted_row_count: int | None = None,
    candidate_row_count: int | None = None,
) -> dict[str, Any]:
    """Return a compact summary of candidate movement toward ground truth."""
    accepted_norm = _normalize_sql(accepted_sql)
    candidate_norm = _normalize_sql(candidate_sql)
    ground_truth_norm = _normalize_sql(ground_truth_sql)

    improved: list[str] = []
    remaining: list[str] = []

    removed_filters = sorted(
        f for f in _equality_filters(accepted_norm)
        if f not in _equality_filters(candidate_norm)
        and f not in _equality_filters(ground_truth_norm)
    )
    improved.extend(f"removed_filter: {f}" for f in removed_filters)

    if accepted_row_count is not None and candidate_row_count is not None:
        if int(candidate_row_count) != int(accepted_row_count):
            improved.append(f"row_count: {int(accepted_row_count)} -> {int(candidate_row_count)}")

    candidate_days = _date_sub_days(candidate_norm)
    gt_days = _date_sub_days(ground_truth_norm)
    if candidate_days and gt_days and candidate_days != gt_days:
        c = sorted(candidate_days)[0]
        g = sorted(gt_days)[0]
        remaining.append(f"date_window: {c}_vs_{g}")

    has_between = " BETWEEN " in candidate_norm.upper()
    gt_uses_gte = " >= " in ground_truth_norm.upper()
    if has_between and gt_uses_gte:
        remaining.append("predicate_form: between_vs_gte")

    next_hint = ""
    if any(r.startswith("date_window:") for r in remaining):
        day = sorted(gt_days)[0] if gt_days else 30
        next_hint = (
            f"teach recent_window_days archetype with DATE_SUB(CURRENT_DATE(), {day})"
        )

    return {
        "target_qid": str(target_qid),
        "improved": improved,
        "remaining": remaining,
        "next_hint": next_hint,
    }
```

- [ ] **Step 4: Store SQL-shape deltas in rejected reflection entries**

In `harness.py`, after a candidate is rejected and before `_build_reflection_entry(...)` is appended, compute deltas for the AG target qids:

```python
from genie_space_optimizer.optimization.sql_shape_delta import compute_sql_shape_delta

_sql_shape_deltas: list[dict] = []
_baseline_by_qid = _rows_by_qid(_baseline_rows_for_control_plane)
_candidate_by_qid = _rows_by_qid(_after_rows)
for _qid in _target_qids:
    _pre_row = _baseline_by_qid.get(str(_qid)) or {}
    _post_row = _candidate_by_qid.get(str(_qid)) or {}
    _delta = compute_sql_shape_delta(
        target_qid=str(_qid),
        accepted_sql=_row_generated_sql(_pre_row),
        candidate_sql=_row_generated_sql(_post_row),
        ground_truth_sql=_row_expected_sql(_post_row) or _row_expected_sql(_pre_row),
        accepted_row_count=_row_result_count(_pre_row),
        candidate_row_count=_row_result_count(_post_row),
    )
    if _delta.get("improved") or _delta.get("remaining"):
        _sql_shape_deltas.append(_delta)
```

Use existing row-access helpers if they already expose generated SQL, expected SQL, and row counts. If row-count access does not exist, add a small private helper in `harness.py`:

```python
def _row_result_count(row: dict) -> int | None:
    for key in ("result_row_count", "row_count", "generated_row_count"):
        value = (row or {}).get(key)
        if isinstance(value, int):
            return value
    return None
```

Add `_sql_shape_deltas` to the reflection entry:

```python
reflection_entry["sql_shape_deltas"] = _sql_shape_deltas
```

- [ ] **Step 5: Feed SQL-shape deltas into strategist memory**

In the reflection formatter used by `_call_llm_for_adaptive_strategy(...)`, append a compact section:

```python
for _entry in reflection_buffer[-3:]:
    for _delta in _entry.get("sql_shape_deltas") or []:
        lines.append(
            "SQL shape delta for {qid}: improved={improved}; remaining={remaining}; next_hint={hint}".format(
                qid=_delta.get("target_qid"),
                improved=", ".join(_delta.get("improved") or []),
                remaining=", ".join(_delta.get("remaining") or []),
                hint=_delta.get("next_hint") or "",
            )
        )
```

Do not include raw benchmark questions. The `next_hint` must stay structural and use abstract archetype language.

- [ ] **Step 6: Run SQL-delta tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_sql_shape_delta.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Task 3**

```bash
git add \
  src/genie_space_optimizer/optimization/sql_shape_delta.py \
  src/genie_space_optimizer/optimization/harness.py \
  tests/unit/test_sql_shape_delta.py
git commit -m "feat: remember SQL-shape deltas from rejected AGs"
```

## Task 4: Authoritative Diagnostic AG Queue

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_static_replay_optimization_intelligence.py`

- [ ] **Step 1: Add source-level replay test for diagnostic AG priority**

Create `packages/genie-space-optimizer/tests/unit/test_static_replay_optimization_intelligence.py`:

```python
from pathlib import Path


def test_harness_has_authoritative_diagnostic_action_queue():
    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "diagnostic_action_queue" in source
    assert "USING DIAGNOSTIC AG FROM COVERAGE GAP" in source
    assert "SKIPPING DIAGNOSTIC AG BECAUSE CLUSTER RESOLVED" in source
    assert source.index("diagnostic_action_queue") < source.index("_call_llm_for_adaptive_strategy")
```

- [ ] **Step 2: Run the failing source-level replay test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_static_replay_optimization_intelligence.py::test_harness_has_authoritative_diagnostic_action_queue -q
```

Expected: FAIL because `diagnostic_action_queue` does not exist yet.

- [ ] **Step 3: Add a separate diagnostic queue in the lever-loop state**

In `harness.py`, near `pending_action_groups: list[dict] = []`, add:

```python
diagnostic_action_queue: list[dict] = []
```

When `uncovered_patchable_clusters(...)` returns clusters, append diagnostic AGs to this queue instead of only appending them to `action_groups`:

```python
for _c in _uncovered:
    diagnostic_action_queue.append(diagnostic_action_group_for_cluster(_c))
```

- [ ] **Step 4: Consume diagnostic AGs before buffered AGs and fresh strategist calls**

Before the existing buffered AG block, add:

```python
ag = None
_live_cluster_ids = {
    str(c.get("cluster_id") or "")
    for c in clusters + (soft_signal_clusters or [])
    if c.get("cluster_id")
}
while diagnostic_action_queue and ag is None:
    _candidate = diagnostic_action_queue.pop(0)
    _src_ids = {
        str(cid) for cid in (_candidate.get("source_cluster_ids") or []) if str(cid)
    }
    if _src_ids and not (_src_ids & _live_cluster_ids):
        print(
            _section("SKIPPING DIAGNOSTIC AG BECAUSE CLUSTER RESOLVED", "-")
            + "\n"
            + _kv("AG id", _candidate.get("id", "?"))
            + "\n"
            + _kv("Source clusters", sorted(_src_ids))
            + "\n"
            + _bar("-")
        )
        continue
    ag = _candidate
    print(
        _section("USING DIAGNOSTIC AG FROM COVERAGE GAP", "-")
        + "\n"
        + _kv("AG id", ag.get("id", "?"))
        + "\n"
        + _kv("Source clusters", sorted(_src_ids))
        + "\n"
        + _bar("-")
    )
```

Then change the existing `if ag is None:` fresh strategist branch to respect this preselected diagnostic AG.

- [ ] **Step 5: Run diagnostic queue source-level test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_static_replay_optimization_intelligence.py::test_harness_has_authoritative_diagnostic_action_queue -q
```

Expected: PASS.

- [ ] **Step 6: Commit Task 4**

```bash
git add \
  src/genie_space_optimizer/optimization/harness.py \
  tests/unit/test_static_replay_optimization_intelligence.py
git commit -m "fix: consume diagnostic AGs before fresh strategy"
```

## Task 5: RCA Coverage For Extra Defensive Filters And Recent Windows

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/archetypes.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/cluster_driven_synthesis.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_rca_extra_defensive_filters.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_feature_mining.py`

- [ ] **Step 1: Add failing RCA tests for extra equality filters and temporal deltas**

Create `packages/genie-space-optimizer/tests/unit/test_rca_extra_defensive_filters.py`:

```python
from genie_space_optimizer.optimization.rca import (
    RcaKind,
    rca_findings_from_eval_rows,
    rca_themes_from_findings,
)


def test_rca_detects_extra_equality_filter_absent_from_expected_sql():
    row = {
        "inputs/question_id": "gs_026",
        "expected_sql": "SELECT SUM(PAYMENT_AMT) FROM tkt_payment",
        "generated_sql": (
            "SELECT SUM(PAYMENT_AMT) FROM tkt_payment "
            "WHERE PAYMENT_CURRENCY_CD = 'USD'"
        ),
    }

    findings = rca_findings_from_eval_rows([row])

    assert any(f.rca_kind is RcaKind.EXTRA_DEFENSIVE_FILTER for f in findings)
    finding = next(f for f in findings if f.rca_kind is RcaKind.EXTRA_DEFENSIVE_FILTER)
    assert "PAYMENT_CURRENCY_CD = 'USD'" in finding.actual_objects


def test_rca_theme_for_extra_filter_teaches_null_group_preservation_and_amount_semantics():
    row = {
        "inputs/question_id": "gs_026",
        "expected_sql": "SELECT SUM(PAYMENT_AMT) FROM tkt_payment",
        "generated_sql": (
            "SELECT SUM(PAYMENT_AMT) FROM tkt_payment "
            "WHERE PAYMENT_CURRENCY_CD = 'USD' AND PAYMENT_METHOD IS NOT NULL"
        ),
    }
    findings = rca_findings_from_eval_rows([row])
    themes = rca_themes_from_findings(findings)
    patches = [patch for theme in themes for patch in theme.patches]
    intents = " ".join(str(p.get("intent") or p.get("instruction") or "") for p in patches)

    assert "do not add unrequested equality filters" in intents
    assert "preserve null groups" in intents
    assert "amount column already encodes the measure" in intents
```

- [ ] **Step 2: Run the failing RCA tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_rca_extra_defensive_filters.py -q
```

Expected: FAIL because extra equality filters are not yet promoted into the expected RCA object/theme shape.

- [ ] **Step 3: Extend RCA SQL-shape detection**

In `rca.py`, add a helper near the existing WHERE extraction helpers:

```python
_EQUALITY_FILTER_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=\s*'([^']*)'",
    re.IGNORECASE,
)


def _equality_filters(where_sql: str) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            f"{m.group(1)} = '{m.group(2)}'"
            for m in _EQUALITY_FILTER_RE.finditer(where_sql or "")
        )
    )
```

Inside `rca_findings_from_eval_rows(...)`, after `exp_where` and `gen_where` are computed, add:

```python
    extra_equality_filters = tuple(
        f for f in _equality_filters(gen_where)
        if f not in set(_equality_filters(exp_where))
    )
    if extra_equality_filters:
        kind = RcaKind.EXTRA_DEFENSIVE_FILTER
        findings.append(RcaFinding(
            rca_id=_mk_id(qid, kind),
            question_id=qid,
            rca_kind=kind,
            confidence=0.85,
            expected_objects=(),
            actual_objects=extra_equality_filters,
            evidence=(
                RcaEvidence(
                    source="sql_shape",
                    detail=(
                        "generated SQL adds equality filters absent from expected SQL: "
                        + ", ".join(extra_equality_filters)
                    ),
                    confidence=0.85,
                ),
            ),
            recommended_levers=recommended_levers_for_rca_kind(kind),
            patch_family="avoid_unrequested_defensive_filters",
            target_qids=(qid,),
        ))
```

- [ ] **Step 4: Expand extra defensive filter patch themes**

In `rca_themes_from_findings(...)`, replace the current `EXTRA_DEFENSIVE_FILTER` patch block with:

```python
        elif f.rca_kind is RcaKind.EXTRA_DEFENSIVE_FILTER:
            patches.append({
                **base,
                "type": "add_instruction",
                "target": "QUERY CONSTRUCTION",
                "instruction_section": "QUERY CONSTRUCTION",
                "lever": 5,
                "intent": (
                    "do not add unrequested equality filters or IS NOT NULL filters; "
                    "preserve null groups unless the user explicitly excludes them; "
                    "when an amount column already encodes the requested measure, do not "
                    "add a currency-code filter unless the question asks for that currency"
                ),
                "actual_objects": list(f.actual_objects),
            })
            patches.append(_example_synthesis_intent(
                base,
                f,
                root_cause="extra_defensive_filter",
            ))
```

- [ ] **Step 5: Add a recent-window archetype**

In `archetypes.py`, add `"extra_defensive_filter"` and `"time_window_logic_mismatch"` to the relevant root-cause sets, then add this archetype before `period_over_period`:

```python
    Archetype(
        name="recent_window_days",
        applicable_root_causes=frozenset({
            "temporal_filter_missing",
            "wrong_filter",
            "wrong_filter_condition",
            "time_window_logic_mismatch",
            "extra_defensive_filter",
        }),
        required_schema_traits=frozenset({"has_date", "has_numeric"}),
        prompt_template=(
            "Produce a recent-window aggregate query using a relative lower-bound "
            "predicate of the form date_col >= DATE_SUB(CURRENT_DATE(), N). "
            "Use an invented business question and schema-safe columns. Do not copy "
            "benchmark wording. Preserve null dimension groups unless the invented "
            "question explicitly asks to exclude them."
        ),
        output_shape={"requires_constructs": ["SELECT", "WHERE"]},
    ),
```

- [ ] **Step 6: Log typed cluster-driven synthesis decline reasons**

In `cluster_driven_synthesis.py`, replace bare `return None` decline paths with a small helper:

```python
def _decline(
    *,
    cluster_id: str,
    archetype: str = "",
    reason: str,
    gate_results: list | None = None,
) -> None:
    _log_summary(
        "cluster",
        cluster_id=cluster_id,
        archetype=archetype,
        outcome="skipped",
        skipped_reason=reason,
        gate_results=gate_results,
    )
```

Use these exact reason codes:

- `safety_cap`
- `iteration_budget`
- `leakage_firewall`
- `no_safe_archetype`
- `empty_model_output`
- `parser_failure`
- `proposal_gate_failure`
- `genie_agreement_failure`

For example, change the `validate_afs(...)` rejection path to:

```python
        _decline(cluster_id=cluster_id, reason=f"leakage_firewall:{exc}")
        return None
```

- [ ] **Step 7: Run RCA and archetype tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest \
  tests/unit/test_rca_extra_defensive_filters.py \
  tests/unit/test_feature_mining.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit Task 5**

```bash
git add \
  src/genie_space_optimizer/optimization/rca.py \
  src/genie_space_optimizer/optimization/archetypes.py \
  src/genie_space_optimizer/optimization/cluster_driven_synthesis.py \
  tests/unit/test_rca_extra_defensive_filters.py \
  tests/unit/test_feature_mining.py
git commit -m "feat: cover extra defensive filters and recent windows"
```

## Task 6: Downgrade Consistency Judges As Cluster Drivers

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/judge_classes.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_judge_classes.py`

- [ ] **Step 1: Add failing judge classification test**

Append this test to `packages/genie-space-optimizer/tests/unit/test_judge_classes.py`:

```python
def test_previous_sql_and_repeatability_are_meta_zero_weight():
    from genie_space_optimizer.optimization.judge_classes import (
        SignalClass,
        judge_signal_class,
        judge_weight_for_root_cause,
    )

    assert judge_signal_class("previous_sql") is SignalClass.META
    assert judge_signal_class("repeatability") is SignalClass.META
    assert judge_weight_for_root_cause("previous_sql") == 0.0
    assert judge_weight_for_root_cause("repeatability") == 0.0
```

- [ ] **Step 2: Run the failing judge classification test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_judge_classes.py::test_previous_sql_and_repeatability_are_meta_zero_weight -q
```

Expected: FAIL because both judges currently fall through default classification and weight.

- [ ] **Step 3: Add explicit meta classifications and zero weights**

In `judge_classes.py`, update `JUDGE_TO_SIGNAL_CLASS`:

```python
    "previous_sql":        SignalClass.META,
    "repeatability":       SignalClass.META,
```

Update `JUDGE_WEIGHT_FOR_ROOT_CAUSE`:

```python
    "previous_sql":        0.0,
    "repeatability":       0.0,
```

- [ ] **Step 4: Run judge tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_judge_classes.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 6**

```bash
git add \
  src/genie_space_optimizer/optimization/judge_classes.py \
  tests/unit/test_judge_classes.py
git commit -m "fix: prevent consistency judges from driving RCA clusters"
```

## Task 7: Pre-Arbiter Regression Guardrail And Baseline Diagnostics

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_control_plane.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_static_replay_optimization_intelligence.py`

- [ ] **Step 1: Add failing pure guardrail test**

Append this test to `packages/genie-space-optimizer/tests/unit/test_control_plane.py`:

```python
def test_pre_arbiter_regression_without_target_fix_rejects_candidate():
    from genie_space_optimizer.optimization.control_plane import (
        decide_pre_arbiter_regression_guardrail,
    )

    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=69.6,
        candidate_pre_arbiter_accuracy=60.9,
        target_fixed_qids=(),
        max_pre_arbiter_regression_pp=5.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "pre_arbiter_regression_without_target_fix"
    assert decision.delta_pp == -8.7
```

- [ ] **Step 2: Run failing guardrail test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_control_plane.py::test_pre_arbiter_regression_without_target_fix_rejects_candidate -q
```

Expected: FAIL with missing `decide_pre_arbiter_regression_guardrail`.

- [ ] **Step 3: Implement pure guardrail decision**

In `control_plane.py`, add:

```python
@dataclass(frozen=True)
class PreArbiterRegressionDecision:
    accepted: bool
    reason_code: str
    delta_pp: float


def decide_pre_arbiter_regression_guardrail(
    *,
    baseline_pre_arbiter_accuracy: float,
    candidate_pre_arbiter_accuracy: float,
    target_fixed_qids: tuple[str, ...],
    max_pre_arbiter_regression_pp: float = 5.0,
) -> PreArbiterRegressionDecision:
    delta = round(
        float(candidate_pre_arbiter_accuracy) - float(baseline_pre_arbiter_accuracy),
        1,
    )
    if target_fixed_qids:
        return PreArbiterRegressionDecision(True, "target_fixed", delta)
    if delta <= -abs(float(max_pre_arbiter_regression_pp)):
        return PreArbiterRegressionDecision(
            False,
            "pre_arbiter_regression_without_target_fix",
            delta,
        )
    return PreArbiterRegressionDecision(True, "within_pre_arbiter_regression_budget", delta)
```

- [ ] **Step 4: Enforce the guardrail in harness after control-plane target-fix computation**

In `harness.py`, after `_control_plane_decision` is available, add:

```python
from genie_space_optimizer.optimization.control_plane import (
    decide_pre_arbiter_regression_guardrail,
)

_max_pre_arbiter_regression_pp = 5.0
try:
    if isinstance(config, dict):
        _max_pre_arbiter_regression_pp = float(
            config.get("max_pre_arbiter_regression_pp", 5.0)
        )
except Exception:
    _max_pre_arbiter_regression_pp = 5.0

_pre_arbiter_guardrail = decide_pre_arbiter_regression_guardrail(
    baseline_pre_arbiter_accuracy=float(_best_pre_arbiter),
    candidate_pre_arbiter_accuracy=float(full_pre_arbiter_accuracy),
    target_fixed_qids=tuple(_control_plane_decision.target_fixed_qids),
    max_pre_arbiter_regression_pp=_max_pre_arbiter_regression_pp,
)
if not _pre_arbiter_guardrail.accepted:
    regressions.append({
        "judge": "pre_arbiter",
        "previous": float(_best_pre_arbiter),
        "current": float(full_pre_arbiter_accuracy),
        "drop": abs(_pre_arbiter_guardrail.delta_pp),
        "reason": _pre_arbiter_guardrail.reason_code,
    })
```

Also emit an audit row with `gate_name="pre_arbiter_regression_guardrail"` and the reason code.

- [ ] **Step 5: Make baseline warnings actionable in stdout**

In the visible control-plane block in `harness.py`, include:

```python
+ _kv("Baseline source for control plane", _baseline_source_for_control_plane) + "\n"
+ _kv("Pre row iteration id", _pre_iteration_id_for_control_plane or "(memory)") + "\n"
+ _kv("Post row iteration id", iteration_counter) + "\n"
```

If `_pre_iteration_id_for_control_plane` does not exist, set it when loading fallback rows:

```python
_pre_iteration_id_for_control_plane = "accepted_baseline_memory"
if _fallback_iter_for_control_plane:
    _pre_iteration_id_for_control_plane = str(
        _fallback_iter_for_control_plane.get("iteration") or "delta_latest_full_fallback"
    )
```

- [ ] **Step 6: Add source-level baseline diagnostic test**

Append this test to `test_static_replay_optimization_intelligence.py`:

```python
def test_harness_prints_control_plane_baseline_source_and_iteration_ids():
    from pathlib import Path

    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "Baseline source for control plane" in source
    assert "Pre row iteration id" in source
    assert "Post row iteration id" in source
```

- [ ] **Step 7: Run guardrail and diagnostic tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest \
  tests/unit/test_control_plane.py::test_pre_arbiter_regression_without_target_fix_rejects_candidate \
  tests/unit/test_static_replay_optimization_intelligence.py::test_harness_prints_control_plane_baseline_source_and_iteration_ids -q
```

Expected: PASS.

- [ ] **Step 8: Commit Task 7**

```bash
git add \
  src/genie_space_optimizer/optimization/control_plane.py \
  src/genie_space_optimizer/optimization/harness.py \
  tests/unit/test_control_plane.py \
  tests/unit/test_static_replay_optimization_intelligence.py
git commit -m "fix: gate broad pre-arbiter regressions"
```

## Task 8: Delay Quarantine When Concrete SQL Deltas Remain

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_terminal.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_terminal_status_taxonomy.py`

- [ ] **Step 1: Add failing terminal-status test**

Append this test to `packages/genie-space-optimizer/tests/unit/test_terminal_status_taxonomy.py`:

```python
def test_plateau_with_untried_sql_delta_gets_specific_terminal_status():
    from genie_space_optimizer.optimization.rca_terminal import (
        RcaTerminalStatus,
        resolve_terminal_on_plateau,
    )

    decision = resolve_terminal_on_plateau(
        quarantined_qids={"gs_026"},
        current_hard_qids={"gs_026"},
        regression_debt_qids=set(),
        sql_delta_qids={"gs_026"},
    )

    assert decision.status is RcaTerminalStatus.UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA
    assert decision.should_continue is True
```

- [ ] **Step 2: Run the failing terminal-status test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_terminal_status_taxonomy.py::test_plateau_with_untried_sql_delta_gets_specific_terminal_status -q
```

Expected: FAIL because `sql_delta_qids` and the terminal status do not exist yet.

- [ ] **Step 3: Extend terminal taxonomy**

In `rca_terminal.py`, add enum value:

```python
    UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA = "unresolved_hard_failure_with_untried_sql_delta"
```

Update `resolve_terminal_on_plateau(...)` signature:

```python
def resolve_terminal_on_plateau(
    *,
    quarantined_qids: set[str],
    current_hard_qids: set[str],
    regression_debt_qids: set[str],
    sql_delta_qids: set[str] | None = None,
) -> RcaTerminalDecision:
```

Add this block before the quarantined-and-hard terminal branch:

```python
    still_patchable = sorted(set(sql_delta_qids or set()) & set(current_hard_qids))
    if still_patchable:
        return RcaTerminalDecision(
            status=RcaTerminalStatus.UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA,
            should_continue=True,
            reason=(
                f"{len(still_patchable)} hard failure(s) have concrete SQL deltas "
                f"remaining: {still_patchable}"
            ),
        )
```

- [ ] **Step 4: Prevent hard quarantine when latest reflection has a concrete SQL delta**

In `harness.py`, before adding qids to persistent quarantine, compute:

```python
_qid_has_untried_sql_delta = {
    str(delta.get("target_qid"))
    for entry in reflection_buffer[-3:]
    for delta in (entry.get("sql_shape_deltas") or [])
    if delta.get("remaining") and delta.get("next_hint")
}
```

When `_newly_quarantined` is computed, remove these qids:

```python
_delayed_quarantine_qids = _newly_quarantined & _qid_has_untried_sql_delta
if _delayed_quarantine_qids:
    logger.warning(
        "Delaying hard quarantine for qids with concrete SQL deltas: %s",
        sorted(_delayed_quarantine_qids),
    )
    _newly_quarantined -= _delayed_quarantine_qids
```

Add a visible line:

```python
print(
    _section("QUARANTINE DELAYED FOR SQL-SHAPE DELTA", "-")
    + "\n"
    + _kv("Questions", sorted(_delayed_quarantine_qids))
    + "\n"
    + _bar("-")
)
```

- [ ] **Step 5: Pass SQL-delta qids into plateau resolution**

In the `resolve_terminal_on_plateau(...)` call site, pass:

```python
sql_delta_qids=_qid_has_untried_sql_delta,
```

- [ ] **Step 6: Run terminal taxonomy tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_terminal_status_taxonomy.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Task 8**

```bash
git add \
  src/genie_space_optimizer/optimization/rca_terminal.py \
  src/genie_space_optimizer/optimization/harness.py \
  tests/unit/test_terminal_status_taxonomy.py
git commit -m "fix: delay quarantine for unresolved SQL deltas"
```

## Task 9: Gate-Baseline And GT-Correction Count Cleanup

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_static_replay_optimization_intelligence.py`

- [ ] **Step 1: Add source-level test for GT correction unique-qid count**

Append this test to `test_static_replay_optimization_intelligence.py`:

```python
def test_harness_uses_qid_values_for_gt_correction_candidate_count():
    from pathlib import Path

    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "_gt_correction_candidate_qids" in source
    assert "GT correction candidates" in source
    assert "len(_gt_correction_candidate_qids)" in source
```

- [ ] **Step 2: Run failing GT count test**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_static_replay_optimization_intelligence.py::test_harness_uses_qid_values_for_gt_correction_candidate_count -q
```

Expected: FAIL if the current visible log computes unique question count from a wrong field.

- [ ] **Step 3: Fix GT correction candidate qid extraction**

In `harness.py`, near the GT correction candidate log, compute qids using the same helper used elsewhere:

```python
_gt_correction_candidate_qids = {
    str(
        row.get("inputs.question_id")
        or row.get("inputs/question_id")
        or row.get("question_id")
        or (row.get("inputs") or {}).get("question_id")
        or ""
    )
    for row in _gt_correction_candidate_rows
}
_gt_correction_candidate_qids.discard("")
print(
    f"GT correction candidates: {len(_gt_correction_candidate_rows)} row(s) "
    f"across {len(_gt_correction_candidate_qids)} unique question(s)"
)
```

- [ ] **Step 4: Add static replay test for accepted baseline after rejected candidate**

Append this test to `test_static_replay_optimization_intelligence.py`:

```python
def test_gate_baseline_contract_documents_accepted_rows_after_rejection():
    from pathlib import Path

    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "accepted_baseline_rows_for_control_plane" in source
    assert "candidate row we just persisted cannot serve as its own baseline" in source
    assert "Baseline source for control plane" in source
```

- [ ] **Step 5: Run static replay cleanup tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_static_replay_optimization_intelligence.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit Task 9**

```bash
git add \
  src/genie_space_optimizer/optimization/harness.py \
  tests/unit/test_static_replay_optimization_intelligence.py
git commit -m "fix: clarify gate baseline and GT correction diagnostics"
```

## Task 10: Focused Regression Suite And Manual Validation

**Files:**
- Modify: `packages/genie-space-optimizer/docs/2026-04-30-lever-loop-optimization-intelligence-plan.md`
- Test: all tests listed below

- [ ] **Step 1: Run focused optimizer-intelligence tests**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest \
  tests/unit/test_patch_selection.py \
  tests/unit/test_proposal_asset_alignment.py \
  tests/unit/test_proposal_grounding.py \
  tests/unit/test_reflection_retry.py \
  tests/unit/test_sql_shape_delta.py \
  tests/unit/test_rca_extra_defensive_filters.py \
  tests/unit/test_judge_classes.py \
  tests/unit/test_control_plane.py \
  tests/unit/test_terminal_status_taxonomy.py \
  tests/unit/test_static_replay_optimization_intelligence.py -q
```

Expected: PASS.

- [ ] **Step 2: Run the broader unit suite for optimizer control-plane paths**

Run:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit -q
```

Expected: PASS.

- [ ] **Step 3: Run one manual lever-loop validation against a hard recent-window/filter failure**

Use the same command shape used for recent manual lever-loop validations in this repository. Record the run id, Genie Space id, and cluster log path in the implementation PR.

Expected visible log evidence:

```text
USING DIAGNOSTIC AG FROM COVERAGE GAP
Baseline source for control plane: accepted_baseline_memory
pre_arbiter_regression_guardrail
SQL shape delta for <qid>
selection_reason=active_cluster_direct_behavior_reserved
```

Expected behavioral evidence:

- No selected patch is dropped by the applier after cap selection.
- Direct L5/L6 behavior patches tied to the active hard cluster survive the cap when present.
- Off-causal L6 snippets without cross-asset justification are rejected before the cap.
- A rejected candidate with a concrete target SQL improvement writes a `sql_shape_delta` into reflection memory.
- A still-fixable hard qid with an untried SQL delta is not hard-quarantined.

- [ ] **Step 4: Commit validation notes if docs were updated**

If validation adds notes to this plan or a follow-up doc, commit them:

```bash
git add docs/2026-04-30-lever-loop-optimization-intelligence-plan.md
git commit -m "docs: record optimization intelligence validation"
```

## Implementation Order

1. Cluster-causal L5/L6 cap ranking and stricter L6 asset alignment.
2. Counterfactual scan enforcement for high-blast-radius L6 snippets and example SQLs.
3. Precise reflection retry keys.
4. SQL-shape delta memory for rejected AGs.
5. Diagnostic AG queue made authoritative.
6. RCA card support for extra defensive filters and temporal-window deltas.
7. Suppress `previous_sql` and `repeatability` from cluster dominance.
8. Pre-arbiter regression hard guardrail.
9. Quarantine delay when concrete SQL delta remains.
10. GT correction count and gate-baseline diagnostics cleanup.

## Self-Review

**Spec coverage:** All ten items from the updated holistic plan are represented. Task 1 covers cap fidelity, L5/L6 slot reservation, counterfactual gating, and L6 asset alignment. Task 2 covers precise retry keys and rollback causes. Task 3 covers SQL-shape delta memory. Task 4 covers authoritative diagnostic AG consumption. Task 5 covers extra defensive filters, null-group preservation, amount semantics, recent-window archetypes, and synthesis decline reasons. Task 6 covers consistency judge downgrade. Task 7 covers pre-arbiter guardrails and baseline diagnostics. Task 8 covers quarantine delay and terminal status. Task 9 covers GT correction count and gate-baseline cleanup.

**Placeholder scan:** No task relies on unspecified behavior. Every code-changing task includes concrete test code, implementation shape, commands, and expected outcomes.

**Type consistency:** New symbols are consistently named across tests and implementation steps: `active_cluster_ids`, `patch_retry_signature`, `retry_allowed_after_rollback`, `compute_sql_shape_delta`, `diagnostic_action_queue`, `decide_pre_arbiter_regression_guardrail`, and `UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA`.
