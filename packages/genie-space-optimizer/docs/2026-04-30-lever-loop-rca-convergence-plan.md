# Lever Loop RCA Convergence Implementation Plan

> **Superseded by:** [`2026-04-30-lever-loop-rca-convergence-plan-v2.md`](./2026-04-30-lever-loop-rca-convergence-plan-v2.md). This document is a milestone record; convergence wiring lives in the v2 plan.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the gap between landed lever-loop modules (asset alignment, precise reflection retry, SQL-shape deltas, terminal-status taxonomy, causal-first patch cap, per-question regression telemetry) and the harness orchestration that today still calls them with stale or missing inputs, and remove the architectural choke points that block convergence on multi-cluster benchmarks (single-AG strategist slice, pre-arbiter baseline mismatch, plateau resolved against stale full_result).

**Architecture:** All fixes flow through the harness orchestration layer (`harness.py`) and a single architectural change in the strategist (`optimizer.py`). The shared modules (`patch_selection`, `proposal_asset_alignment`, `reflection_retry`, `sql_shape_delta`, `rca_terminal`, `per_question_regression`, `control_plane`) already export the right primitives — this plan wires them up correctly, fixes the baseline mismatch that produces false `passing_to_hard_regressed` verdicts, and enables multi-AG iterations so a five-cluster benchmark gets five attempts instead of one. Every wiring task is paired with a unit/static-replay test that pins observable harness behavior, so regressions are caught at the boundary closest to the bug.

**Tech Stack:** Python 3.10+, `pytest` (`uv run pytest`), Spark Connect (mocked in unit tests), Delta tables `genie_eval_iterations` / `genie_eval_question_regressions` (only touched through existing helpers). No new dependencies.

---

## Background: Symptoms vs. Existing Code Boundaries

The most recent lever-loop run (`7now_delivery_analytics_space`) showed:

1. The cap selected `update_description` patches over the only direct-behavior `add_sql_snippet_filter` for Q017's hard cluster.
2. Q009 was flagged `passing_to_hard_regressed`, the AG was rolled back, and the loop quarantined a question that was actually still passing under the post-enrichment baseline.
3. The strategist produced one Action Group despite five hard clusters being live; with `MAX_ITERATIONS=5`, four clusters never received an attempt.
4. The loop terminated `plateau_no_open_failures` while `gs_001`, `gs_009`, `gs_017`, `gs_021`, and `gs_026` were still hard.
5. `AG_COVERAGE_H001` was instruction-only and gate-dropped before it could fix `Q026`'s shape regression.
6. The per-question regression Delta table received rows without `source_cluster_ids`, `source_proposal_ids`, or `applied_patch_ids` — the attribution columns are NULL on disk.

The shared modules already encode the right behavior:

* `patch_selection.select_target_aware_causal_patch_cap` accepts `active_cluster_ids` and reserves a direct-behavior patch first; `harness.py:12402` does not pass it.
* `proposal_asset_alignment.proposal_aligns_with_cluster` exists; the harness never invokes it for L5/L6 patches before the cap.
* `reflection_retry.patch_retry_signature` and `retry_allowed_after_rollback` exist; the harness still uses a coarse `(patch_type, target)` set built from `do_not_retry` strings (`harness.py:11414`).
* `sql_shape_delta.compute_sql_shape_delta` exists; the harness never stores its output on the rejected reflection entry.
* `rca_terminal.resolve_terminal_on_plateau` accepts `sql_delta_qids` and returns `UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA`; `harness.py:9979` calls it without `sql_delta_qids` and computes `current_hard_qids` from `full_result.get("rows")`, which can be stale across the iteration boundary.
* `per_question_regression.build_question_regression_rows` accepts `cluster_ids_by_qid`, `proposal_ids_by_qid`, `applied_patch_ids`; `harness.py:8897` passes none of them.
* `control_plane.load_latest_full_iteration` honors `eval_scope='full'` only; clustering and the post-enrichment guardrail use `load_latest_state_iteration` (which also accepts `'enrichment'`). The pre-arbiter regression guardrail today seeds its baseline from the wrong loader, which is the smoking gun behind the Q009 false regression.
* `optimizer.py:9642` slices the strategist output to one AG (`action_groups[:1]`).

This plan turns each bullet into a wiring fix with a failing test first. Each task is independent enough to be reviewed and merged on its own, but the tasks compose: by the end, the harness uses the active cluster, requires asset alignment for SQL-shape patches, learns from rejected SQL deltas, retries patches at the precise (type/table/column/section) signature, classifies plateaus against the latest hard inventory, scales iteration budget by cluster count, and writes complete attribution into `genie_eval_question_regressions`.

---

## File Structure

| Path | Responsibility | Status |
|------|----------------|--------|
| `src/genie_space_optimizer/optimization/harness.py` | Orchestration; all wiring fixes land here. | modify |
| `src/genie_space_optimizer/optimization/optimizer.py` | Strategist call; remove single-AG slice. | modify |
| `src/genie_space_optimizer/optimization/control_plane.py` | Add baseline-source helper used by harness; existing helpers untouched. | modify |
| `src/genie_space_optimizer/optimization/sql_shape_delta.py` | Already exports `compute_sql_shape_delta`. | unchanged |
| `src/genie_space_optimizer/optimization/reflection_retry.py` | Already exports `patch_retry_signature`, `retry_allowed_after_rollback`. | unchanged |
| `src/genie_space_optimizer/optimization/rca_terminal.py` | Already exports plateau resolver with `sql_delta_qids`. | unchanged |
| `src/genie_space_optimizer/optimization/per_question_regression.py` | Already accepts attribution kwargs. | unchanged |
| `src/genie_space_optimizer/optimization/proposal_asset_alignment.py` | Already exports `proposal_aligns_with_cluster`. | unchanged |
| `src/genie_space_optimizer/common/config.py` | Add `MAX_ITERATIONS_PER_CLUSTER` knob. | modify |
| `tests/unit/test_harness_patch_cap_active_cluster.py` | New — pin active-cluster wiring. | create |
| `tests/unit/test_harness_asset_alignment_gate.py` | New — pin L5/L6 asset alignment wiring. | create |
| `tests/unit/test_harness_reflection_retry_wiring.py` | New — pin precise-retry wiring. | create |
| `tests/unit/test_harness_sql_delta_memory.py` | New — pin SQL-shape delta storage on rejected entries. | create |
| `tests/unit/test_harness_plateau_resolution.py` | New — pin plateau resolver inputs. | create |
| `tests/unit/test_harness_question_regression_attribution.py` | New — pin attribution kwargs. | create |
| `tests/unit/test_harness_pre_arbiter_baseline_source.py` | New — pin control-plane baseline loader. | create |
| `tests/unit/test_optimizer_multi_ag_strategist.py` | New — pin multi-AG strategist output. | create |
| `tests/unit/test_max_iterations_per_cluster.py` | New — pin iteration-budget scaling. | create |

---

## Task 1: Wire `active_cluster_ids` into the Causal-First Patch Cap

**Why:** `patch_selection.select_target_aware_causal_patch_cap` reserves the highest-tier direct-behavior patch *for the active cluster* first, then fills target QIDs, then fills with the global causal ranking. The harness already computes the active AG's source cluster IDs but does not pass them, so the cap defaults to `active_cluster_ids=()` and reserves whichever direct-behavior patch sorts first across all proposals — typically a `update_description` from another cluster. This is the single line that explains why the cap dropped Q017's `add_sql_snippet_filter` in the most recent run.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:12388-12410`
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_patch_cap_active_cluster.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_harness_patch_cap_active_cluster.py
"""Pin that the harness passes active_cluster_ids to the causal patch cap."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_harness_patch_cap_call_passes_active_cluster_ids() -> None:
    """The patch cap call site must forward active_cluster_ids from the AG.

    Regression guard for the run where the cap dropped Q017's direct-behavior
    add_sql_snippet_filter because active_cluster_ids defaulted to ().
    """
    src = inspect.getsource(harness.run_lever_loop_for_run)
    assert "select_target_aware_causal_patch_cap(" in src, (
        "Patch cap call site moved; update this guard."
    )
    cap_block = src.split("select_target_aware_causal_patch_cap(", 1)[1].split(")", 1)[0]
    assert "active_cluster_ids=" in cap_block, (
        "harness must pass active_cluster_ids to select_target_aware_causal_patch_cap; "
        "without it the cap reserves direct-behavior patches from arbitrary clusters."
    )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_patch_cap_active_cluster.py -v
```

Expected: FAIL with `AssertionError: harness must pass active_cluster_ids ...`.

- [ ] **Step 3: Wire active_cluster_ids in the harness call**

Replace the cap call block at `harness.py:12388-12406` with:

```python
        if len(patches) > MAX_AG_PATCHES:
            from genie_space_optimizer.optimization.patch_selection import (
                select_target_aware_causal_patch_cap,
            )
            from genie_space_optimizer.optimization.control_plane import (
                target_qids_from_action_group as _target_qids_for_patch_cap,
            )

            _patch_cap_target_qids = tuple(
                locals().get("_blast_target_qids")
                or _target_qids_for_patch_cap(ag, strategy.get("_source_clusters", []))
            )
            _active_cluster_ids_for_cap = tuple(
                str(cid).strip()
                for cid in (ag.get("source_cluster_ids") or [])
                if str(cid).strip()
            )

            _before_cap = list(patches)
            patches, _patch_cap_decisions = select_target_aware_causal_patch_cap(
                _before_cap,
                target_qids=_patch_cap_target_qids,
                max_patches=MAX_AG_PATCHES,
                active_cluster_ids=_active_cluster_ids_for_cap,
            )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_patch_cap_active_cluster.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_harness_patch_cap_active_cluster.py
git commit -m "fix(lever-loop): forward active_cluster_ids from AG into causal patch cap"
```

---

## Task 2: Gate L5/L6 Patches by Asset Alignment Before the Cap

**Why:** `proposal_asset_alignment.l5_l6_patch_requires_asset_alignment` returns `True` for `add_sql_snippet_filter`, `add_sql_snippet_measure`, `add_sql_snippet_expression`, `add_sql_snippet_calculation`, `add_example_sql`, and any L5/L6 patch. `proposal_aligns_with_cluster` returns the alignment decision. Without invoking these, the harness lets the strategist's stray cross-asset proposals (a Q017 SQL filter targeting Q026's table, for example) survive into the cap and inflate the patch count. The cap then drops a real causal patch as a tie-breaker.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:12340-12390` (proposal grounding section, just before the cap)
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_asset_alignment_gate.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_harness_asset_alignment_gate.py
"""Pin that L5/L6 SQL-shape patches are dropped if they don't align with the cluster's lineage."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_harness_invokes_proposal_aligns_with_cluster_for_l5_l6() -> None:
    """Harness must call proposal_aligns_with_cluster for L5/L6 SQL-shape patches.

    Regression guard: cross-asset SQL filters (filter on table A targeting cluster
    rooted in table B) silently survived the cap and dropped the real causal patch.
    """
    src = inspect.getsource(harness.run_lever_loop_for_run)
    assert "proposal_aligns_with_cluster" in src, (
        "harness must invoke proposal_aligns_with_cluster for L5/L6 patches"
    )
    assert "l5_l6_patch_requires_asset_alignment" in src, (
        "harness must guard the alignment call with l5_l6_patch_requires_asset_alignment"
    )


def test_harness_drops_misaligned_l5_l6_before_cap() -> None:
    """The grounding step must record dropped misaligned patches in the audit trail."""
    src = inspect.getsource(harness.run_lever_loop_for_run)
    assert "asset_alignment_dropped" in src, (
        "harness must emit an asset_alignment_dropped audit reason for visibility"
    )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_asset_alignment_gate.py -v
```

Expected: FAIL on both assertions.

- [ ] **Step 3: Add the alignment gate immediately before the patch-cap block**

Insert the following block at `harness.py` directly before the `if len(patches) > MAX_AG_PATCHES:` block (right after `patches = list(patches_after_grounding)` or the equivalent point where `patches` already holds the post-grounding list):

```python
        # Asset-alignment gate for L5/L6 SQL-shape patches.
        # The cluster's lineage assets are the only legal write targets unless
        # the patch carries an explicit cross_asset_justification. Misaligned
        # SQL-shape patches inflate the cap input and crowd out causal patches.
        from genie_space_optimizer.optimization.proposal_asset_alignment import (
            l5_l6_patch_requires_asset_alignment,
            proposal_aligns_with_cluster,
        )

        _ag_source_cluster_ids = {
            str(cid).strip()
            for cid in (ag.get("source_cluster_ids") or [])
            if str(cid).strip()
        }
        _source_clusters_by_id = {
            str(c.get("cluster_id") or "").strip(): c
            for c in (strategy.get("_source_clusters") or [])
            if str(c.get("cluster_id") or "").strip()
        }
        _aligned_patches: list[dict] = []
        _alignment_drops: list[dict] = []
        for _p in patches:
            if not l5_l6_patch_requires_asset_alignment(_p):
                _aligned_patches.append(_p)
                continue
            _matched_cluster = next(
                (
                    _source_clusters_by_id[c]
                    for c in _ag_source_cluster_ids
                    if c in _source_clusters_by_id
                ),
                None,
            )
            _decision = proposal_aligns_with_cluster(_p, _matched_cluster)
            if _decision.get("aligned"):
                _aligned_patches.append(_p)
                continue
            _alignment_drops.append(
                {
                    "proposal_id": str(_p.get("proposal_id") or _p.get("id") or ""),
                    "patch_type": str(_p.get("type") or _p.get("patch_type") or ""),
                    "reason": _decision.get("reason"),
                    "proposal_assets": list(_decision.get("proposal_assets") or ()),
                    "cluster_assets": list(_decision.get("cluster_assets") or ()),
                }
            )
        if _alignment_drops:
            logger.info(
                "[%s] asset_alignment_dropped: %d patch(es); reasons=%s",
                ag_id,
                len(_alignment_drops),
                [d["reason"] for d in _alignment_drops],
            )
            for _drop in _alignment_drops:
                _audit_emit(
                    stage_letter="G",
                    gate_name="proposal_asset_alignment",
                    decision="reject",
                    reason_code="asset_alignment_dropped",
                    affected_qids=[],
                    metrics=_drop,
                )
        patches = _aligned_patches
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_asset_alignment_gate.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_harness_asset_alignment_gate.py
git commit -m "fix(lever-loop): gate L5/L6 SQL-shape patches by cluster asset alignment before cap"
```

---

## Task 3: Replace Coarse `_patch_forbidden` with Precise Retry Signature

**Why:** Today the harness keys reflection's "this rolled back, do not retry" set on the string `f"{patch_type} on {target}"` parsed out of `do_not_retry` strings (`harness.py:11414`). Two patches that touch *different columns of the same table* collide on `(update_column_description, table)` and the second one is dropped as a duplicate. `reflection_retry.patch_retry_signature` returns `(patch_type, table, column, section_set)` and `retry_allowed_after_rollback` understands that infra/insufficient-gain/target-still-hard rollbacks should not block retries. The harness must use these.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:11400-11550`
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_reflection_retry_wiring.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_harness_reflection_retry_wiring.py
"""Pin that the harness uses precise retry signatures, not (type, target) tuples."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_harness_uses_patch_retry_signature() -> None:
    """harness must use the precise per-patch retry signature."""
    src = inspect.getsource(harness.run_lever_loop_for_run)
    assert "patch_retry_signature" in src, (
        "harness must import and call patch_retry_signature; "
        "the (patch_type, target) string parser is too coarse and silently "
        "blocks legitimate per-column patches."
    )


def test_harness_uses_retry_allowed_after_rollback() -> None:
    """harness must consult retry_allowed_after_rollback to honor rollback cause."""
    src = inspect.getsource(harness.run_lever_loop_for_run)
    assert "retry_allowed_after_rollback" in src, (
        "harness must call retry_allowed_after_rollback; otherwise infra/insufficient-gain "
        "rollbacks block retries even when the precise signature is new."
    )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_reflection_retry_wiring.py -v
```

Expected: FAIL on both assertions.

- [ ] **Step 3: Replace the `_patch_forbidden` block**

Replace the block at `harness.py:11414-11550` (everything from `_patch_forbidden: set[tuple[str, str]] = set()` up to and including the existing `if _key in _patch_forbidden:` filter loop) with:

```python
        # Precise per-patch reflection guard. We compare each candidate proposal
        # against the rolled-back history using a signature that includes
        # (patch_type, target_table, target_column, instruction_section_set), so
        # different columns on the same table no longer collide. Rollback cause
        # is honored: infra failures / insufficient-gain / target-still-hard do
        # not block re-trying the same shape.
        from genie_space_optimizer.optimization.reflection_retry import (
            patch_retry_signature,
            retry_allowed_after_rollback,
        )
        from genie_space_optimizer.optimization.rollback_class import (
            RollbackClass as _RC,
        )

        _rolled_back_history: list[dict] = []
        _rollback_cause_by_sig: dict[tuple, str] = {}
        for _rb in reflection_buffer:
            if _rb.get("accepted"):
                continue
            _cause = (
                "infra_schema_failure"
                if _rb.get("rollback_class") == _RC.INFRA_FAILURE.value
                else "content_regression"
                if _rb.get("rollback_class") == _RC.CONTENT_REGRESSION.value
                else str(_rb.get("rollback_cause") or "content_regression")
            )
            for _entry in _rb.get("rolled_back_patches", []) or []:
                if not isinstance(_entry, dict):
                    continue
                _rolled_back_history.append(_entry)
                _rollback_cause_by_sig.setdefault(
                    patch_retry_signature(_entry), _cause,
                )

        _kept: list[dict] = []
        _dropped: list[tuple[str, str, str]] = []
        for _p in all_proposals:
            _decision = retry_allowed_after_rollback(
                current_patch=_p,
                rolled_back_patches=_rolled_back_history,
                rollback_cause=_rollback_cause_by_sig.get(
                    patch_retry_signature(_p), "content_regression",
                ),
            )
            if _decision.allowed:
                _kept.append(_p)
                continue
            _dropped.append(
                (
                    str(_p.get("type") or _p.get("patch_type") or ""),
                    str(
                        _p.get("target") or _p.get("target_object")
                        or _p.get("target_table") or _p.get("table") or "?"
                    ),
                    _decision.reason,
                )
            )

        logger.info(
            "[%s] reflection_retry: kept=%d dropped=%d rollback_history=%d",
            ag_id, len(_kept), len(_dropped), len(_rolled_back_history),
        )
        for _ptype, _target, _reason in _dropped:
            _audit_emit(
                stage_letter="R",
                gate_name="reflection_retry",
                decision="reject",
                reason_code=_reason,
                affected_qids=[],
                metrics={"patch_type": _ptype, "target": _target},
            )
        all_proposals = _kept
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_reflection_retry_wiring.py -v
```

Expected: PASS.

- [ ] **Step 5: Run full reflection-related test suite to confirm no regression**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_reflection_retry.py tests/unit/test_harness_reflection_retry_wiring.py -v
```

Expected: PASS for all tests.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_harness_reflection_retry_wiring.py
git commit -m "fix(lever-loop): replace coarse forbidden tuple with patch_retry_signature in reflection guard"
```

---

## Task 4: Persist SQL-Shape Deltas on Rejected Reflection Entries

**Why:** When an AG is rolled back because the candidate post-arbiter accuracy drops or a target qid stays hard, the candidate SQL is the most concrete piece of evidence we have about what the strategist *almost* learned. `compute_sql_shape_delta` summarizes the delta between the accepted SQL and the candidate SQL with respect to the ground-truth SQL, including a `next_hint` string the strategist can read on the next turn. Today nothing calls it, so the strategist sees only the coarse rollback verdict and re-proposes the same shape.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` near the rollback bookkeeping (search for `reflection_buffer.append` after a rollback verdict).
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_sql_delta_memory.py`

- [ ] **Step 1: Find the rollback bookkeeping site**

```bash
cd packages/genie-space-optimizer
rg -n "reflection_buffer.append|sql_shape_delta" src/genie_space_optimizer/optimization/harness.py
```

Expected: Identify a single site where the harness builds the rejected reflection entry with keys including `accepted: False`, `rollback_class`, and the per-target evaluation rows. This is the entry to enrich.

- [ ] **Step 2: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_harness_sql_delta_memory.py
"""Pin that rejected reflection entries carry per-target SQL-shape deltas."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_harness_imports_compute_sql_shape_delta() -> None:
    """harness must import compute_sql_shape_delta to populate reflection memory."""
    src = inspect.getsource(harness)
    assert "compute_sql_shape_delta" in src, (
        "harness must call compute_sql_shape_delta when a candidate AG is rejected; "
        "without it the strategist re-proposes the same shape."
    )


def test_harness_records_sql_shape_deltas_on_rejected_entry() -> None:
    """The rejected reflection entry must include a sql_shape_deltas list."""
    src = inspect.getsource(harness.run_lever_loop_for_run)
    assert "sql_shape_deltas" in src, (
        "rejected reflection entries must store sql_shape_deltas keyed by qid"
    )
```

- [ ] **Step 3: Run the test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_sql_delta_memory.py -v
```

Expected: FAIL on both assertions.

- [ ] **Step 4: Add the SQL-delta computation at the rollback site**

Insert the following block immediately before any `reflection_buffer.append({...})` call that records a rejected (non-accepted) AG. If multiple sites exist, factor a small helper and apply it at each one. Replace the placeholder names below with the actual variables in scope at the chosen site (`accepted_rows`, `candidate_rows`, `ground_truth_by_qid`).

```python
        from genie_space_optimizer.optimization.sql_shape_delta import (
            compute_sql_shape_delta,
        )

        def _row_qid(row: dict) -> str:
            return str(
                row.get("inputs.question_id")
                or row.get("inputs/question_id")
                or row.get("question_id")
                or (row.get("inputs") or {}).get("question_id", "")
            )

        def _row_sql(row: dict) -> str:
            return str(
                row.get("outputs.genie_sql")
                or row.get("outputs/genie_sql")
                or row.get("genie_sql")
                or (row.get("outputs") or {}).get("genie_sql", "")
                or ""
            )

        def _row_count(row: dict) -> int | None:
            for k in ("genie_row_count", "outputs.genie_row_count", "outputs/genie_row_count"):
                v = row.get(k)
                if isinstance(v, (int, float)):
                    return int(v)
            v = (row.get("outputs") or {}).get("genie_row_count")
            return int(v) if isinstance(v, (int, float)) else None

        _accepted_by_qid = {
            _row_qid(r): r for r in (accepted_rows or []) if _row_qid(r)
        }
        _candidate_by_qid = {
            _row_qid(r): r for r in (candidate_rows or []) if _row_qid(r)
        }
        _sql_deltas: list[dict] = []
        for _qid, _cand_row in _candidate_by_qid.items():
            _gt_sql = str(ground_truth_by_qid.get(_qid, "")) if ground_truth_by_qid else ""
            if not _gt_sql:
                continue
            _acc_row = _accepted_by_qid.get(_qid, {})
            _delta = compute_sql_shape_delta(
                target_qid=_qid,
                accepted_sql=_row_sql(_acc_row),
                candidate_sql=_row_sql(_cand_row),
                ground_truth_sql=_gt_sql,
                accepted_row_count=_row_count(_acc_row),
                candidate_row_count=_row_count(_cand_row),
            )
            if _delta["improved"] or _delta["remaining"]:
                _sql_deltas.append(_delta)
```

Then change the rejected reflection entry construction so the dict includes `"sql_shape_deltas": _sql_deltas`.

If `accepted_rows`, `candidate_rows`, or `ground_truth_by_qid` are not yet in scope at the rollback site, plumb them through from the surrounding scope: `accepted_rows` is `_accepted_baseline_rows_for_control_plane`, `candidate_rows` is `full_result_1.get("rows", [])`, and `ground_truth_by_qid` is the existing `_ground_truth_by_qid` mapping the harness already builds for the arbiter.

- [ ] **Step 5: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_sql_delta_memory.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_harness_sql_delta_memory.py
git commit -m "feat(lever-loop): record sql_shape_deltas on rejected AGs for strategist memory"
```

---

## Task 5: Plateau Resolver — Use Latest Hard Inventory and `sql_delta_qids`

**Why:** `harness.py:9970` builds `current_hard_qids` from `full_result.get("rows", [])`. After a rejected AG, `full_result` still points at the *previous* accepted iteration's rows, so a hard failure that just stayed hard does not appear in `current_hard_qids`. The plateau resolver then sees an empty intersection and returns `PLATEAU_NO_OPEN_FAILURES`. We also do not pass `sql_delta_qids`, so the new `UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA` branch never fires.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:9962-9995`
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_plateau_resolution.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_harness_plateau_resolution.py
"""Pin the plateau resolver inputs in the harness."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_plateau_resolution_uses_latest_state_iteration() -> None:
    """The plateau resolver block must read the latest state iteration, not full_result."""
    src = inspect.getsource(harness.run_lever_loop_for_run)
    plateau_block = src.split("resolve_terminal_on_plateau", 1)[1].split("break", 1)[0]
    assert "load_latest_state_iteration" in plateau_block, (
        "plateau resolution must use load_latest_state_iteration so the hard inventory "
        "is the current state, not the last accepted full_result"
    )


def test_plateau_resolution_passes_sql_delta_qids() -> None:
    """The plateau resolver must receive sql_delta_qids to enable the SQL-delta branch."""
    src = inspect.getsource(harness.run_lever_loop_for_run)
    plateau_block = src.split("resolve_terminal_on_plateau", 1)[1].split("break", 1)[0]
    assert "sql_delta_qids" in plateau_block, (
        "harness must pass sql_delta_qids to resolve_terminal_on_plateau"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_plateau_resolution.py -v
```

Expected: FAIL on both assertions.

- [ ] **Step 3: Replace the plateau resolver block**

Replace `harness.py:9952-9994` with:

```python
            from genie_space_optimizer.optimization.rca_terminal import (
                resolve_terminal_on_plateau,
            )
            from genie_space_optimizer.optimization.control_plane import (
                hard_failure_qids as _hard_failure_qids_for_plateau,
            )
            from genie_space_optimizer.optimization.state import (
                load_latest_state_iteration,
            )

            _state_iter = load_latest_state_iteration(
                spark, run_id, catalog, schema,
            ) or {}
            _plateau_rows: list[dict] = []
            try:
                _plateau_rows = list(_state_iter.get("rows") or [])
            except Exception:
                _plateau_rows = []
            _current_hard_qids = set(
                _hard_failure_qids_for_plateau(_plateau_rows)
            )
            _regression_debt_qids = set(
                _correction_state.get("regression_debt_qids", set()) or set()
            )
            _quarantined_qids = set(
                _correction_state.get("quarantined_qids", set()) or set()
            )
            _sql_delta_qids: set[str] = set()
            for _rb in reflection_buffer:
                for _delta in _rb.get("sql_shape_deltas", []) or []:
                    _qid = str(_delta.get("target_qid") or "")
                    if _qid and (_delta.get("remaining") or _delta.get("improved")):
                        _sql_delta_qids.add(_qid)

            _resolved = resolve_terminal_on_plateau(
                quarantined_qids=_quarantined_qids,
                current_hard_qids=_current_hard_qids,
                regression_debt_qids=_regression_debt_qids,
                sql_delta_qids=_sql_delta_qids,
            )
            logger.info(
                "Plateau terminal resolved at iteration %d: status=%s reason=%s "
                "(hard=%d quarantined=%d debt=%d sql_delta=%d)",
                _iter_num, _resolved.status.value, _resolved.reason,
                len(_current_hard_qids), len(_quarantined_qids),
                len(_regression_debt_qids), len(_sql_delta_qids),
            )
            if _resolved.should_continue:
                logger.info(
                    "Plateau suppressed because RCA terminal status is %s",
                    _resolved.status.value,
                )
                continue
            print(
                _section("LEVER LOOP — TERMINATION: plateau", "!") + "\n"
                + _kv("Reason", _resolved.reason) + "\n"
                + _kv("RCA terminal status", _resolved.status.value) + "\n"
                + _kv("Iteration", _iteration_label(_iter_num)) + "\n"
                + _bar("!")
            )
            break
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_plateau_resolution.py tests/unit/test_terminal_status_taxonomy.py -v
```

Expected: PASS for both files.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_harness_plateau_resolution.py
git commit -m "fix(lever-loop): plateau resolver uses latest state iteration and sql_delta_qids"
```

---

## Task 6: Wire Per-Question Regression Attribution

**Why:** `genie_eval_question_regressions` is the queryable audit table for "this qid flipped at iteration N because of AG X". Today its `source_cluster_ids_json`, `source_proposal_ids_json`, and `applied_patch_ids_json` columns are NULL because the harness call site at `harness.py:8897` omits all three kwargs.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:8895-8908`
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_question_regression_attribution.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_harness_question_regression_attribution.py
"""Pin that build_question_regression_rows receives full attribution kwargs."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_build_question_regression_rows_call_passes_attribution() -> None:
    """harness must pass cluster_ids_by_qid, proposal_ids_by_qid, applied_patch_ids."""
    src = inspect.getsource(harness.run_lever_loop_for_run)
    call_block = src.split("build_question_regression_rows(", 1)[1].split(")", 1)[0]
    for kwarg in ("cluster_ids_by_qid=", "proposal_ids_by_qid=", "applied_patch_ids="):
        assert kwarg in call_block, (
            f"build_question_regression_rows call missing {kwarg}; "
            "the question_regressions table will have NULL attribution columns"
        )
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_question_regression_attribution.py -v
```

Expected: FAIL on the first assertion.

- [ ] **Step 3: Build the attribution dictionaries before the call and pass them**

Replace `harness.py:8895-8908` with:

```python
    # Persist non-hold_pass transitions and emit per-qid audit rows.
    try:
        _cluster_ids_by_qid: dict[str, list[str]] = {}
        for _c in (strategy.get("_source_clusters") or []):
            _cid = str(_c.get("cluster_id") or "").strip()
            if not _cid:
                continue
            for _q in _c.get("question_ids") or []:
                _cluster_ids_by_qid.setdefault(str(_q), []).append(_cid)
        _proposal_ids_by_qid: dict[str, list[str]] = {}
        for _p in (all_proposals or []):
            _pid = str(_p.get("proposal_id") or _p.get("id") or "").strip()
            if not _pid:
                continue
            for _q in _p.get("target_qids") or []:
                _proposal_ids_by_qid.setdefault(str(_q), []).append(_pid)
        _applied_patch_ids = [
            str(_p.get("proposal_id") or _p.get("id") or "")
            for _p in (applied_patches or [])
            if (_p.get("proposal_id") or _p.get("id"))
        ]
        _t4_rows = build_question_regression_rows(
            run_id=run_id,
            iteration=iteration_counter,
            ag_id=ag_id,
            verdict=_t4_verdict,
            suppressed_qids=_suppressed_qids,
            cluster_ids_by_qid=_cluster_ids_by_qid,
            proposal_ids_by_qid=_proposal_ids_by_qid,
            applied_patch_ids=_applied_patch_ids,
        )
        if _t4_rows:
            from genie_space_optimizer.optimization.state import (
                write_question_regressions,
            )
            write_question_regressions(spark, _t4_rows, catalog=catalog, schema=schema)
        for _row in _t4_rows:
            _audit_emit(
                stage_letter="M",
                gate_name="per_question_regression",
                decision=(
                    "fail" if _row["transition"] == "pass_to_fail" and not _row["suppressed"]
                    else "pass"
                ),
                reason_code=_row["transition"],
                affected_qids=[_row["question_id"]],
                metrics={
                    "transition": _row["transition"],
                    "was_passing": _row["was_passing"],
                    "is_passing": _row["is_passing"],
                    "suppressed": _row["suppressed"],
                    "source_cluster_ids": _row["source_cluster_ids"],
                    "source_proposal_ids": _row["source_proposal_ids"],
                    "applied_patch_ids": _row["applied_patch_ids"],
                },
            )
    except Exception:
        logger.debug("Failed to persist per-question regression rows", exc_info=True)
```

If `applied_patches` is not in scope at this point, replace it with the variable that holds the AG's applied patch list (search the surrounding function for `applier_decisions`/`applied`); if no such variable exists, derive it from `[d for d in applier_decisions if d.get("decision") == "applied"]`.

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_question_regression_attribution.py \
                tests/unit/test_per_question_regression_attribution.py -v
```

Expected: PASS for both files.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_harness_question_regression_attribution.py
git commit -m "fix(lever-loop): pass cluster/proposal/patch attribution into question_regression rows"
```

---

## Task 7: Pre-Arbiter Regression Guardrail Uses the Same Baseline as Clustering

**Why:** The Q009 false `passing_to_hard_regressed` was caused by a baseline mismatch. Clustering and the patch-cap target list use `load_latest_state_iteration` (which accepts both `'full'` and `'enrichment'` scopes); the pre-arbiter regression guardrail builds its `pre_arbiter_baseline` from `_accepted_baseline_rows_for_control_plane`, which is initialized at `harness.py:9258` from `load_latest_full_iteration` (only `'full'` scope). On cold start with post-enrichment iter 0, the control-plane baseline still points at the *pre-enrichment* row, while clustering points at the post-enrichment row. The candidate's pre-arbiter accuracy for Q009 then looks like a regression even though it stayed correct under the actual current baseline.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:9252-9265` and the seeding logic at the start of the loop where `_accepted_baseline_rows_for_control_plane` is later refreshed.
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py` — add a `select_control_plane_baseline_rows` helper.
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_pre_arbiter_baseline_source.py`

- [ ] **Step 1: Write the failing tests**

```python
# packages/genie-space-optimizer/tests/unit/test_harness_pre_arbiter_baseline_source.py
"""Pin that the control-plane baseline matches the clustering baseline."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness, control_plane


def test_select_control_plane_baseline_rows_exists() -> None:
    """control_plane must export select_control_plane_baseline_rows."""
    assert hasattr(control_plane, "select_control_plane_baseline_rows"), (
        "control_plane must export select_control_plane_baseline_rows so the harness "
        "can fetch a baseline that matches the clustering scope"
    )


def test_harness_seeds_control_plane_baseline_from_state_iteration() -> None:
    """harness must seed the control-plane baseline using the same loader as clustering."""
    src = inspect.getsource(harness.run_lever_loop_for_run)
    assert "select_control_plane_baseline_rows" in src, (
        "harness must call select_control_plane_baseline_rows to seed the control-plane baseline"
    )


def test_select_control_plane_baseline_rows_prefers_latest_state_iteration() -> None:
    """Returns the latest state-iteration rows when present, with eval_scope tagged."""
    state_iter = {
        "iteration": 0,
        "eval_scope": "enrichment",
        "rows": [{"inputs.question_id": "gs_009", "feedback/result_correctness/value": "yes"}],
    }
    full_iter = {
        "iteration": 0,
        "eval_scope": "full",
        "rows": [{"inputs.question_id": "gs_009", "feedback/result_correctness/value": "no"}],
    }
    rows, scope = control_plane.select_control_plane_baseline_rows(
        latest_state_iteration=state_iter,
        latest_full_iteration=full_iter,
    )
    assert rows == state_iter["rows"]
    assert scope == "enrichment"


def test_select_control_plane_baseline_rows_falls_back_to_full() -> None:
    """Falls back to the latest full iteration when no state iteration row exists."""
    full_iter = {"iteration": 1, "eval_scope": "full", "rows": [{"q": 1}]}
    rows, scope = control_plane.select_control_plane_baseline_rows(
        latest_state_iteration=None,
        latest_full_iteration=full_iter,
    )
    assert rows == full_iter["rows"]
    assert scope == "full"


def test_select_control_plane_baseline_rows_handles_empty_inputs() -> None:
    """Both inputs missing returns empty rows and unknown scope."""
    rows, scope = control_plane.select_control_plane_baseline_rows(
        latest_state_iteration=None, latest_full_iteration=None,
    )
    assert rows == []
    assert scope == "unknown"
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_pre_arbiter_baseline_source.py -v
```

Expected: FAIL on `test_select_control_plane_baseline_rows_exists` first (the helper does not exist yet).

- [ ] **Step 3: Add the helper to `control_plane.py`**

Append to `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py`:

```python
def select_control_plane_baseline_rows(
    *,
    latest_state_iteration: dict | None,
    latest_full_iteration: dict | None,
) -> tuple[list[dict], str]:
    """Pick the baseline rows the pre-arbiter regression guardrail compares against.

    Clustering reads ``load_latest_state_iteration`` so it sees post-enrichment
    rows even when the lever loop has not produced a ``full`` iteration yet.
    The control-plane guardrail must use the same source so a candidate is not
    marked as a regression against a stale ``pre-enrichment`` baseline.

    Returns ``(rows, eval_scope)`` where ``eval_scope`` is one of
    ``"full"``, ``"enrichment"``, or ``"unknown"``.
    """
    state = latest_state_iteration or {}
    state_rows = list(state.get("rows") or [])
    if state_rows:
        return state_rows, str(state.get("eval_scope") or "full")
    full = latest_full_iteration or {}
    full_rows = list(full.get("rows") or [])
    if full_rows:
        return full_rows, str(full.get("eval_scope") or "full")
    return [], "unknown"
```

- [ ] **Step 4: Re-seed `_accepted_baseline_rows_for_control_plane` from the helper**

Replace the block at `harness.py:9256-9262` with:

```python
    _accepted_baseline_rows_for_control_plane: list[dict] = []
    _accepted_baseline_eval_scope: str = "unknown"
    try:
        from genie_space_optimizer.optimization.control_plane import (
            select_control_plane_baseline_rows,
        )
        _state_iter_baseline = load_latest_state_iteration(
            spark, run_id, catalog, schema,
        )
        _full_iter_baseline = load_latest_full_iteration(
            spark, run_id, catalog, schema,
        )
        _accepted_baseline_rows_for_control_plane, _accepted_baseline_eval_scope = (
            select_control_plane_baseline_rows(
                latest_state_iteration=_state_iter_baseline,
                latest_full_iteration=_full_iter_baseline,
            )
        )
        logger.info(
            "Control-plane baseline seeded: rows=%d eval_scope=%s",
            len(_accepted_baseline_rows_for_control_plane),
            _accepted_baseline_eval_scope,
        )
    except Exception:
        logger.warning("Failed to initialize accepted baseline rows", exc_info=True)
```

The existing acceptance path that updates `_accepted_baseline_rows_for_control_plane` after a successful AG remains unchanged — once the loop accepts a `full` iteration, that one becomes authoritative as before.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_pre_arbiter_baseline_source.py tests/unit/test_control_plane.py -v
```

Expected: PASS for both files.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py \
        packages/genie-space-optimizer/tests/unit/test_harness_pre_arbiter_baseline_source.py
git commit -m "fix(lever-loop): pre-arbiter guardrail seeds baseline from latest state iteration"
```

---

## Task 8: Allow the Strategist to Emit Multiple Action Groups Per Iteration

**Why:** `optimizer.py:9642` returns `action_groups[:1]`, so a five-cluster benchmark produces one AG per iteration. Combined with `MAX_ITERATIONS=5`, the loop attempts at most one cluster per iteration. The slice was added when the strategist's downstream cost was unbounded; today the patch cap, applier, and pre-arbiter guardrail bound the per-iteration cost regardless of AG count.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:9620-9645`
- Test: `packages/genie-space-optimizer/tests/unit/test_optimizer_multi_ag_strategist.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_optimizer_multi_ag_strategist.py
"""Pin that the strategist preserves multiple action groups."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import optimizer


def test_call_llm_for_adaptive_strategy_does_not_slice_to_first_ag() -> None:
    """The strategist return must not collapse multi-AG output to the first AG."""
    src = inspect.getsource(optimizer._call_llm_for_adaptive_strategy)
    assert "action_groups[:1]" not in src, (
        "remove action_groups[:1] slice; the loop must see all proposed AGs so "
        "multi-cluster benchmarks get one attempt per cluster"
    )
    assert "action_groups[:MAX_ACTION_GROUPS_PER_STRATEGY]" in src or "action_groups," in src, (
        "strategist must return the full list (optionally bounded by a constant), not [:1]"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_optimizer_multi_ag_strategist.py -v
```

Expected: FAIL on the first assertion.

- [ ] **Step 3: Add the bound and remove the slice**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py`. At the top of the file (next to other module-level constants), add:

```python
MAX_ACTION_GROUPS_PER_STRATEGY = 5
```

At the return statement around line 9642, replace:

```python
    return {
        "action_groups": action_groups[:1],
        "global_instruction_rewrite": global_rewrite,
        "rationale": rationale,
    }
```

with:

```python
    return {
        "action_groups": action_groups[:MAX_ACTION_GROUPS_PER_STRATEGY],
        "global_instruction_rewrite": global_rewrite,
        "rationale": rationale,
    }
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_optimizer_multi_ag_strategist.py -v
```

Expected: PASS.

- [ ] **Step 5: Run the broader optimizer test suite to confirm no regression**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_optimizer.py tests/unit/test_optimizer_strategist.py \
              tests/unit/test_static_replay_optimization_intelligence.py -v
```

Expected: PASS for all.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py \
        packages/genie-space-optimizer/tests/unit/test_optimizer_multi_ag_strategist.py
git commit -m "feat(strategist): emit up to MAX_ACTION_GROUPS_PER_STRATEGY AGs per iteration"
```

---

## Task 9: Scale `MAX_ITERATIONS` by Hard Cluster Count

**Why:** Even with multi-AG strategist output, the harness pops one AG per outer iteration (the `while diagnostic_action_queue` loop in `harness.py:10695` and the AG dispatch that follows). On a five-cluster benchmark with `MAX_ITERATIONS=5`, the loop converges fast only when the first iteration's AG fixes everything. We add a per-run iteration budget that scales with the *initial* hard cluster count, capped to a safety ceiling.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:9172` (caller) and the iteration-budget computation site at the start of the lever loop.
- Test: `packages/genie-space-optimizer/tests/unit/test_max_iterations_per_cluster.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_max_iterations_per_cluster.py
"""Pin that the iteration budget scales with the initial hard cluster count."""

from __future__ import annotations

from genie_space_optimizer.common.config import (
    MAX_ITERATIONS,
    MAX_ITERATIONS_PER_CLUSTER,
    MAX_ITERATIONS_HARD_CEILING,
)
from genie_space_optimizer.optimization.harness import (
    compute_iteration_budget,
)


def test_constants_present() -> None:
    """The two new knobs must be importable so callers can override them."""
    assert isinstance(MAX_ITERATIONS_PER_CLUSTER, int)
    assert isinstance(MAX_ITERATIONS_HARD_CEILING, int)
    assert MAX_ITERATIONS_PER_CLUSTER >= 1
    assert MAX_ITERATIONS_HARD_CEILING >= MAX_ITERATIONS


def test_compute_iteration_budget_scales_with_hard_clusters() -> None:
    """Five hard clusters must yield budget >= 5 iterations (one attempt per cluster)."""
    assert compute_iteration_budget(
        hard_cluster_count=5,
        requested_max_iterations=MAX_ITERATIONS,
    ) >= 5


def test_compute_iteration_budget_respects_explicit_request() -> None:
    """An explicit caller request larger than the scaled value still wins."""
    assert compute_iteration_budget(
        hard_cluster_count=2,
        requested_max_iterations=20,
    ) == 20


def test_compute_iteration_budget_respects_hard_ceiling() -> None:
    """Scaled budget never exceeds the hard ceiling regardless of cluster count."""
    assert (
        compute_iteration_budget(
            hard_cluster_count=1000,
            requested_max_iterations=MAX_ITERATIONS,
        )
        == MAX_ITERATIONS_HARD_CEILING
    )


def test_compute_iteration_budget_floor_is_max_iterations() -> None:
    """Zero hard clusters still gets MAX_ITERATIONS so triage can run."""
    assert (
        compute_iteration_budget(
            hard_cluster_count=0,
            requested_max_iterations=MAX_ITERATIONS,
        )
        == MAX_ITERATIONS
    )
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_max_iterations_per_cluster.py -v
```

Expected: FAIL with `ImportError: cannot import name 'MAX_ITERATIONS_PER_CLUSTER'` (and `compute_iteration_budget`).

- [ ] **Step 3: Add the constants to `config.py`**

Append to `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py`:

```python
MAX_ITERATIONS_PER_CLUSTER = 1
MAX_ITERATIONS_HARD_CEILING = 15
```

- [ ] **Step 4: Add the budget helper to `harness.py`**

Add the following near the top of `harness.py` (next to the other top-level helpers, before `run_lever_loop_for_run`):

```python
def compute_iteration_budget(
    *,
    hard_cluster_count: int,
    requested_max_iterations: int,
) -> int:
    """Return the iteration budget, scaled by the initial hard cluster count.

    Each hard cluster gets at least ``MAX_ITERATIONS_PER_CLUSTER`` attempts,
    floored at ``MAX_ITERATIONS`` and capped at ``MAX_ITERATIONS_HARD_CEILING``.
    An explicit caller request always wins when it is greater than the scaled
    value, so manual overrides still work.
    """
    from genie_space_optimizer.common.config import (
        MAX_ITERATIONS as _MAX_ITERATIONS,
        MAX_ITERATIONS_HARD_CEILING as _CEILING,
        MAX_ITERATIONS_PER_CLUSTER as _PER_CLUSTER,
    )
    floor = int(requested_max_iterations or _MAX_ITERATIONS)
    scaled = max(floor, int(hard_cluster_count or 0) * int(_PER_CLUSTER))
    return min(scaled, int(_CEILING))
```

- [ ] **Step 5: Use the helper at the start of the lever loop**

Find the existing `max_iterations = max_iterations or MAX_ITERATIONS` site (around `harness.py:13691`) and replace it with the call below; also locate the first `clusters` computation in the loop and use its `len(...)` as `hard_cluster_count`:

```python
    max_iterations = compute_iteration_budget(
        hard_cluster_count=len(clusters or []),
        requested_max_iterations=max_iterations or MAX_ITERATIONS,
    )
    logger.info(
        "Iteration budget set to %d (hard_clusters=%d, requested=%s)",
        max_iterations, len(clusters or []),
        (max_iterations if max_iterations else MAX_ITERATIONS),
    )
```

If `clusters` is not yet bound at that line (because clustering happens later), defer the budget computation to immediately after the first clustering call and before the iteration loop starts.

- [ ] **Step 6: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_max_iterations_per_cluster.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_max_iterations_per_cluster.py
git commit -m "feat(lever-loop): scale max_iterations by initial hard cluster count"
```

---

## Task 10: Strategist Memo Cache Includes SQL-Delta Fingerprint

**Why:** Once Tasks 4 and 5 land, rejected AGs carry `sql_shape_deltas`. The strategist's memoization key (`_strategist_memo_key`) hashes only cluster signatures + space revision. After a rollback, the key is unchanged, so the cache returns the same strategy and the loop spins. We add the SQL-delta fingerprint to the key so the strategist re-runs once the reflection memory has new evidence.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` — `_strategist_memo_key` definition (search the function for that name).
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_strategist_memo_includes_sql_deltas.py`

- [ ] **Step 1: Find `_strategist_memo_key` and review its current signature**

```bash
cd packages/genie-space-optimizer
rg -n "def _strategist_memo_key" src/genie_space_optimizer/optimization/harness.py
```

Expected: One match. Read the signature so the new parameter is appended cleanly.

- [ ] **Step 2: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_harness_strategist_memo_includes_sql_deltas.py
"""Pin that the strategist memo cache invalidates when SQL deltas change."""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import _strategist_memo_key


def test_memo_key_changes_when_sql_deltas_change() -> None:
    """Two memo keys with different SQL-delta fingerprints must not collide."""
    clusters = [{"cluster_id": "H001", "question_ids": ["gs_017"]}]
    snapshot = {"revision": "rev1"}
    deltas_a = [{"target_qid": "gs_017", "improved": [], "remaining": ["date_window: 7_vs_30"]}]
    deltas_b = [{"target_qid": "gs_017", "improved": ["removed_filter: foo='bar'"], "remaining": []}]
    key_a = _strategist_memo_key(clusters, snapshot, sql_shape_deltas=deltas_a)
    key_b = _strategist_memo_key(clusters, snapshot, sql_shape_deltas=deltas_b)
    assert key_a != key_b, (
        "_strategist_memo_key must include a fingerprint of sql_shape_deltas so "
        "post-rollback evidence forces a strategist re-run"
    )


def test_memo_key_stable_when_no_sql_deltas() -> None:
    """No deltas → same key on repeated calls (regression guard for default arg)."""
    clusters = [{"cluster_id": "H001", "question_ids": ["gs_017"]}]
    snapshot = {"revision": "rev1"}
    key_a = _strategist_memo_key(clusters, snapshot, sql_shape_deltas=[])
    key_b = _strategist_memo_key(clusters, snapshot, sql_shape_deltas=[])
    assert key_a == key_b
```

- [ ] **Step 3: Run the test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_strategist_memo_includes_sql_deltas.py -v
```

Expected: FAIL with `TypeError: _strategist_memo_key() got an unexpected keyword argument 'sql_shape_deltas'`.

- [ ] **Step 4: Add the kwarg to `_strategist_memo_key`**

Update `_strategist_memo_key` so its signature is:

```python
def _strategist_memo_key(
    clusters: list[dict],
    metadata_snapshot: dict,
    *,
    sql_shape_deltas: list[dict] | None = None,
) -> str:
    cluster_sig = tuple(
        (str(c.get("cluster_id") or ""), tuple(sorted(str(q) for q in c.get("question_ids") or [])))
        for c in clusters or []
    )
    snapshot_rev = str((metadata_snapshot or {}).get("revision") or "")
    delta_sig = tuple(
        (
            str(d.get("target_qid") or ""),
            tuple(sorted(str(x) for x in d.get("improved") or [])),
            tuple(sorted(str(x) for x in d.get("remaining") or [])),
        )
        for d in (sql_shape_deltas or [])
    )
    return repr((cluster_sig, snapshot_rev, delta_sig))
```

If `_strategist_memo_key` already returns `repr((...))`, keep that scheme — only add the third tuple component.

- [ ] **Step 5: Update the call site to pass the deltas**

At `harness.py:10722` replace:

```python
                _memo_key = _strategist_memo_key(
                    list(_strategy_hard_clusters), metadata_snapshot,
                )
```

with:

```python
                _memo_sql_deltas = [
                    _delta
                    for _rb in reflection_buffer
                    for _delta in (_rb.get("sql_shape_deltas") or [])
                ]
                _memo_key = _strategist_memo_key(
                    list(_strategy_hard_clusters), metadata_snapshot,
                    sql_shape_deltas=_memo_sql_deltas,
                )
```

- [ ] **Step 6: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_harness_strategist_memo_includes_sql_deltas.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_harness_strategist_memo_includes_sql_deltas.py
git commit -m "fix(lever-loop): memo key includes sql_shape_deltas so rollback evidence re-runs strategist"
```

---

## Task 11: Diagnostic AG Templates Carry SQL-Shape Patches for Shape Root Causes

**Why:** `diagnostic_action_group_for_cluster` (`control_plane.py:241`) builds an AG with empty `lever_directives`. When the cluster's root cause is a SQL-shape category — `plural_top_n_collapse`, `temporal_window_mismatch`, `extra_defensive_filter`, `wrong_aggregation`, `missing_filter` — the L5 structural gate downstream rejects the AG because no SQL-shape directive is present. The diagnostic AG then never produces a real candidate, and the cluster stays uncovered.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py:241-258`
- Test: `packages/genie-space-optimizer/tests/unit/test_diagnostic_action_group_shape_template.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_diagnostic_action_group_shape_template.py
"""Pin that diagnostic AGs carry SQL-shape directives for shape root causes."""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    diagnostic_action_group_for_cluster,
)


def test_diagnostic_ag_for_temporal_window_includes_l5_directive() -> None:
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_026"],
        "root_cause": "temporal_window_mismatch",
        "asi_counterfactual_fixes": ["use DATE_SUB(CURRENT_DATE(), 30) instead of last_30_days"],
    }
    ag = diagnostic_action_group_for_cluster(cluster)
    assert ag["lever_directives"], "shape root causes must seed lever_directives"
    assert "L5" in ag["lever_directives"], "temporal window mismatches need an L5 directive"


def test_diagnostic_ag_for_missing_filter_includes_l5_directive() -> None:
    cluster = {
        "cluster_id": "H002",
        "question_ids": ["gs_017"],
        "root_cause": "missing_filter",
        "asi_counterfactual_fixes": ["add is_finance_monthly_same_store='Y' filter"],
    }
    ag = diagnostic_action_group_for_cluster(cluster)
    assert "L5" in ag["lever_directives"]


def test_diagnostic_ag_for_unknown_root_cause_keeps_legacy_shape() -> None:
    cluster = {
        "cluster_id": "H003",
        "question_ids": ["gs_001"],
        "root_cause": "unknown",
        "asi_counterfactual_fixes": [],
    }
    ag = diagnostic_action_group_for_cluster(cluster)
    assert ag["lever_directives"] == {}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_diagnostic_action_group_shape_template.py -v
```

Expected: FAIL on the first two assertions.

- [ ] **Step 3: Update `diagnostic_action_group_for_cluster`**

Replace `control_plane.py:241-258` with:

```python
_SHAPE_ROOT_CAUSES = frozenset({
    "plural_top_n_collapse",
    "temporal_window_mismatch",
    "extra_defensive_filter",
    "wrong_aggregation",
    "missing_filter",
})


def diagnostic_action_group_for_cluster(cluster: dict) -> dict:
    """Build a deterministic AG when the strategist omits a hard cluster.

    For shape root causes we seed a lever-5 directive that asks the proposer
    to emit a concrete SQL-shape patch. Without this, the L5 structural gate
    rejects the AG and the cluster stays uncovered.
    """
    cid = str(cluster.get("cluster_id") or "H_UNKNOWN")
    qids = [str(q) for q in cluster.get("question_ids", []) or [] if str(q)]
    root = str(cluster.get("root_cause") or cluster.get("asi_failure_type") or "unknown")
    fixes = [str(f) for f in cluster.get("asi_counterfactual_fixes", []) or [] if str(f)]
    fix_text = (
        fixes[0] if fixes
        else "Use the cluster RCA evidence to produce a targeted metadata change."
    )
    lever_directives: dict[str, dict] = {}
    if root in _SHAPE_ROOT_CAUSES:
        lever_directives["L5"] = {
            "kind": "sql_shape",
            "root_cause": root,
            "guidance": fix_text,
            "target_qids": qids,
        }
    return {
        "id": f"AG_COVERAGE_{cid}",
        "root_cause_summary": f"{root}: {fix_text}",
        "affected_questions": qids,
        "source_cluster_ids": [cid],
        "coverage_reason": "strategist_omitted_patchable_hard_cluster",
        "lever_directives": lever_directives,
    }
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_diagnostic_action_group_shape_template.py -v
```

Expected: PASS for all three tests.

- [ ] **Step 5: Run downstream tests that exercise diagnostic AGs**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_control_plane.py \
              tests/unit/test_static_replay_optimization_intelligence.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py \
        packages/genie-space-optimizer/tests/unit/test_diagnostic_action_group_shape_template.py
git commit -m "fix(lever-loop): diagnostic AGs seed L5 directive for SQL-shape root causes"
```

---

## Task 12: Static Replay — End-to-End Convergence Guard

**Why:** With Tasks 1–11 landed, a synthetic five-cluster benchmark must converge in ≤ `compute_iteration_budget(5, MAX_ITERATIONS)` iterations under the static replay harness. This single end-to-end test is the integration checkpoint that prevents future regressions where one fix accidentally breaks another.

**Files:**
- Modify: `packages/genie-space-optimizer/tests/unit/test_static_replay_optimization_intelligence.py` (or add a new sibling file).
- Create (if missing): fixtures under `tests/unit/fixtures/static_replay/multi_cluster_convergence/`.

- [ ] **Step 1: Inspect the existing static replay harness**

```bash
cd packages/genie-space-optimizer
rg -n "static_replay|run_static_replay" tests/unit/test_static_replay_optimization_intelligence.py | head -40
```

Expected: One or two helpers (e.g. `run_static_replay(...)`) plus a fixture path for prior runs.

- [ ] **Step 2: Add the failing convergence test**

Append the following to `tests/unit/test_static_replay_optimization_intelligence.py`. Adjust `run_static_replay` to whatever signature already exists; the assertions below are what matters.

```python
def test_multi_cluster_benchmark_converges_within_scaled_budget(tmp_path) -> None:
    """A 5-cluster benchmark with one direct-behavior fix per cluster must converge.

    Wires the static replay through every fix landed in this plan: active-cluster
    cap, asset alignment, precise reflection retry, sql_shape_delta memory, plateau
    resolver with sql_delta_qids, baseline source alignment, multi-AG strategist,
    and scaled iteration budget. Failure of this test means one of the upstream
    tasks is wired but observable behavior still drops a cluster.
    """
    from genie_space_optimizer.optimization.harness import compute_iteration_budget
    from genie_space_optimizer.common.config import MAX_ITERATIONS

    fixture_dir = (
        Path(__file__).parent / "fixtures" / "static_replay"
        / "multi_cluster_convergence"
    )
    result = run_static_replay(fixture_dir=fixture_dir, tmp_path=tmp_path)

    expected_budget = compute_iteration_budget(
        hard_cluster_count=5,
        requested_max_iterations=MAX_ITERATIONS,
    )
    assert result.iterations_run <= expected_budget, (
        f"loop ran {result.iterations_run} iterations; expected ≤ {expected_budget}"
    )
    assert result.terminal_status == "converged_post_arbiter_100", (
        f"loop did not converge cleanly; terminal_status={result.terminal_status}"
    )
    assert not result.unresolved_hard_qids, (
        f"unresolved hard qids: {result.unresolved_hard_qids}"
    )
```

- [ ] **Step 3: Build the fixture**

Create `tests/unit/fixtures/static_replay/multi_cluster_convergence/` with the same on-disk shape used by other static replay fixtures in this directory (clusters JSON, baseline rows JSON, accepted patches JSON, etc.). Use the most recent passing static replay fixture as a template — copy it, then mutate to:

* Five hard clusters: `H001..H005`, each with one qid.
* Each cluster's RCA points at a different table.column.
* Each cluster has a single available `add_sql_snippet_filter` proposal that, if applied, flips its qid from hard-fail to pass.
* Ground-truth SQL for each qid is shape-equivalent to the proposal's predicted SQL after the patch is applied.

If `run_static_replay` already exists with a similar signature, reuse it; otherwise extend it minimally (no new helpers, just additional fixture loading).

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_static_replay_optimization_intelligence.py -v -k convergence
```

Expected: PASS.

- [ ] **Step 5: Run the full unit suite to confirm no regressions**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/ -q
```

Expected: All tests pass. Investigate any newly red test before committing.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/tests/unit/test_static_replay_optimization_intelligence.py \
        packages/genie-space-optimizer/tests/unit/fixtures/static_replay/multi_cluster_convergence/
git commit -m "test(lever-loop): static replay convergence guard for 5-cluster benchmark"
```

---

## Task 13: Stdout Enrichment — Per-Question Journey Ledger

**Why:** When the loop fails to converge, operators today have to splice multiple log sections together to see what happened to a single question. The data is already in the audit emitter (`_audit_emit`); we just need to project it into a single section at the end of each iteration.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` — add `_print_question_journey_ledger` near the existing `_section`/`_kv` helpers and call it once per iteration after `write_question_regressions`.
- Test: `packages/genie-space-optimizer/tests/unit/test_question_journey_ledger.py`

- [ ] **Step 1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_question_journey_ledger.py
"""Pin the per-question journey ledger output."""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _build_question_journey_ledger,
)


def test_journey_ledger_groups_by_qid() -> None:
    rows = [
        {
            "question_id": "gs_017",
            "transition": "fail_to_pass",
            "was_passing": False,
            "is_passing": True,
            "source_cluster_ids": ["H002"],
            "source_proposal_ids": ["P016"],
            "applied_patch_ids": ["P016"],
            "suppressed": False,
        },
        {
            "question_id": "gs_026",
            "transition": "hold_fail",
            "was_passing": False,
            "is_passing": False,
            "source_cluster_ids": ["H001"],
            "source_proposal_ids": [],
            "applied_patch_ids": [],
            "suppressed": False,
        },
    ]
    ledger = _build_question_journey_ledger(rows=rows, iteration=2, ag_id="AG1")
    assert "QUESTION JOURNEY LEDGER" in ledger
    assert "gs_017" in ledger and "fail_to_pass" in ledger
    assert "gs_026" in ledger and "hold_fail" in ledger
    assert "H001" in ledger and "H002" in ledger


def test_journey_ledger_is_empty_when_no_rows() -> None:
    ledger = _build_question_journey_ledger(rows=[], iteration=1, ag_id="AG1")
    assert ledger == ""
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_question_journey_ledger.py -v
```

Expected: FAIL with `ImportError: cannot import name '_build_question_journey_ledger'`.

- [ ] **Step 3: Add the helper to `harness.py`**

Add immediately after the existing `_section` / `_kv` / `_bar` helpers in `harness.py`:

```python
def _build_question_journey_ledger(
    *,
    rows: list[dict],
    iteration: int,
    ag_id: str,
) -> str:
    """Render a compact per-question journey table for the iteration."""
    if not rows:
        return ""
    lines = [
        _section(f"QUESTION JOURNEY LEDGER (iter {iteration} ag {ag_id})", "-"),
        f"|  {'qid':<14}{'transition':<18}{'clusters':<14}{'proposals':<18}{'applied'}",
    ]
    for row in rows:
        qid = str(row.get("question_id", ""))[:13]
        trans = str(row.get("transition", ""))[:17]
        clusters = ",".join(str(c) for c in (row.get("source_cluster_ids") or []))[:13]
        proposals = ",".join(str(p) for p in (row.get("source_proposal_ids") or []))[:17]
        applied = ",".join(str(p) for p in (row.get("applied_patch_ids") or []))
        lines.append(f"|  {qid:<14}{trans:<18}{clusters:<14}{proposals:<18}{applied}")
    lines.append(_bar("-"))
    return "\n".join(lines)
```

- [ ] **Step 4: Call the helper at the end of the per-iteration regression block**

At `harness.py` immediately after `write_question_regressions(spark, _t4_rows, catalog=catalog, schema=schema)` (the call updated in Task 6), add:

```python
        _ledger = _build_question_journey_ledger(
            rows=_t4_rows, iteration=iteration_counter, ag_id=ag_id,
        )
        if _ledger:
            print(_ledger)
```

- [ ] **Step 5: Run the test to verify it passes**

```bash
cd packages/genie-space-optimizer
uv run pytest tests/unit/test_question_journey_ledger.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_question_journey_ledger.py
git commit -m "feat(lever-loop): stdout question journey ledger per iteration"
```

---

## Task 14: Documentation — Cross-Reference the New Plan from Adjacent Plans

**Why:** Three earlier plans cover overlapping ground (`2026-04-30-lever-loop-applyability-contract-plan.md`, `2026-04-30-lever-loop-optimization-intelligence-plan.md`, `2026-04-30-lever-loop-snapshot-attribution-schema-plan.md`). When this convergence plan lands, future readers need a single arrow telling them "this is the current source of truth on lever-loop convergence as of 2026-04-30."

**Files:**
- Modify: `packages/genie-space-optimizer/docs/2026-04-30-lever-loop-applyability-contract-plan.md`
- Modify: `packages/genie-space-optimizer/docs/2026-04-30-lever-loop-optimization-intelligence-plan.md`
- Modify: `packages/genie-space-optimizer/docs/2026-04-30-lever-loop-snapshot-attribution-schema-plan.md`

- [ ] **Step 1: Add a "Superseded by" banner to each prior plan**

At the top of each of the three files, immediately after the H1 title, insert:

```markdown
> **Superseded by:** [`2026-04-30-lever-loop-rca-convergence-plan.md`](./2026-04-30-lever-loop-rca-convergence-plan.md). This document records the prior milestone; convergence wiring lives in the convergence plan.
```

- [ ] **Step 2: Verify each banner renders**

```bash
cd packages/genie-space-optimizer
head -n 5 docs/2026-04-30-lever-loop-applyability-contract-plan.md
head -n 5 docs/2026-04-30-lever-loop-optimization-intelligence-plan.md
head -n 5 docs/2026-04-30-lever-loop-snapshot-attribution-schema-plan.md
```

Expected: Each file shows the H1 followed by the new "Superseded by" banner.

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/docs/2026-04-30-lever-loop-applyability-contract-plan.md \
        packages/genie-space-optimizer/docs/2026-04-30-lever-loop-optimization-intelligence-plan.md \
        packages/genie-space-optimizer/docs/2026-04-30-lever-loop-snapshot-attribution-schema-plan.md
git commit -m "docs(lever-loop): point earlier plans to the convergence plan"
```

---

## Self-Review

**1. Spec coverage** — Each symptom from the most recent run is addressed:

| Symptom | Task |
|---|---|
| Cap dropped Q017's direct-behavior filter | Task 1 (active_cluster_ids), Task 2 (asset alignment) |
| Q009 false `passing_to_hard_regressed` | Task 7 (baseline source alignment) |
| Strategist returned 1 AG for 5 clusters | Task 8 (remove `[:1]` slice), Task 9 (scale iterations) |
| `plateau_no_open_failures` while hard failures remained | Task 5 (latest state iteration + sql_delta_qids), Task 4 (sql_shape_deltas) |
| `AG_COVERAGE_H001` instruction-only and gate-dropped | Task 11 (shape templates for diagnostic AGs) |
| `genie_eval_question_regressions` attribution columns NULL | Task 6 (attribution kwargs) |
| Coarse reflection retry blocks legitimate per-column patches | Task 3 (precise retry signature) |
| Strategist memo cache returns same strategy after rollback | Task 10 (sql_delta fingerprint) |
| Operators cannot trace one qid's journey across the iteration | Task 13 (journey ledger) |
| End-to-end regression guard | Task 12 (static replay convergence) |
| Earlier plans contradict the new wiring | Task 14 (superseded banners) |

**2. Placeholder scan** — No "TBD", "TODO", "implement later". Every code step shows the actual code or the exact replacement region. The only place I refer to in-scope variables (`accepted_rows`, `candidate_rows`, `ground_truth_by_qid`, `applied_patches`) is in Task 4/Task 6, where I name the fallback variable to use if the local name differs (`_accepted_baseline_rows_for_control_plane`, `full_result_1.get("rows", [])`, `_ground_truth_by_qid`, `[d for d in applier_decisions if d.get("decision") == "applied"]`).

**3. Type consistency** — `compute_iteration_budget` is defined in Task 9 and consumed by name in Task 12. `_strategist_memo_key` is updated with the kwarg in Task 10 and referenced by the same name in the call-site replacement. `select_control_plane_baseline_rows` is added in Task 7 and imported by the same name. `_build_question_journey_ledger` is added in Task 13 and tested under that name.

---

## Execution Handoff

Plan complete and saved to `packages/genie-space-optimizer/docs/2026-04-30-lever-loop-rca-convergence-plan.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
