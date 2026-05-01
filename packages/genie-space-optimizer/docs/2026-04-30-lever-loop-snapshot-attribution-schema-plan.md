# Lever Loop Snapshot Attribution And Canonical Schema Implementation Plan

> **Superseded by:** [`2026-04-30-lever-loop-rca-convergence-plan-v2.md`](./2026-04-30-lever-loop-rca-convergence-plan-v2.md). This document is a milestone record; convergence wiring lives in the v2 plan.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the lever loop recoverable and causally faithful by fixing parsed Genie config rollback verification, making real rollback mismatches terminal, ensuring RCA-attributed patches survive selection, preventing quarantine from hiding unresolved hard failures, hardening benchmark SQL hygiene, and documenting the canonical optimizer schema.

**Architecture:** Treat the parsed `serialized_space` shape as the rollback contract: expected pre-AG state and live post-rollback state must both flow through the same exportable Genie config normalization before digesting or diffing. Treat the live Genie Space configuration snapshot as in-memory control-plane state first and Delta persistence second. Treat proposal selection as a typed causality problem: explicit RCA + target-QID attribution outranks broad AG-level fallback metadata edits when relevance ties.

**Tech Stack:** Python 3.11, pytest, existing `genie_space_optimizer.optimization` modules, Databricks SDK `WorkspaceClient`, no new runtime dependencies.

---

## Why This Plan Exists

The latest cluster stdout/stderr changed the diagnosis:

- `config_snapshot` was missing from `genie_opt_runs`, and backfill failed with `DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT`. The latest stderr also makes the ownership split explicit: the warning text now reads *"This may fail on serverless if the runtime identity lacks Genie Space 'Can Edit' permission. The app backend should capture the snapshot at trigger time."* Lazy fetch from inside the lever loop is a degraded fallback, not the contract; the run-level baseline snapshot must be captured at run-trigger time, before the lever loop ever starts.
- Rollback verification compared a parsed pre-AG `serialized_space` snapshot to the full live Genie API response and returned `live_config_differs_from_pre_snapshot` even when rollback may have restored the effective Genie config. The latest stderr shows this happen four times in one run: AG1, AG3, AG4, and AG5 all printed `verify_rollback_restored returned not verified ... Halting further AGs`, yet AG2 ran after AG1, AG3 ran after AG2, AG4 ran after AG3, and AG5 ran after AG4. Today's "halt" is a log line, not a control-flow change. The fix in Task 2 is to make `failed_rollback_verification` a terminal `RuntimeError` for the entire run.
- The proposal inventory contained RCA-attributed patches, but the patch cap selected broad non-RCA proposals first. The cap decision rows also did not carry enough identity fields (`parent_proposal_id`, `expanded_patch_id`, `rca_id`, `target_qids`, `causal_attribution_tier`) to reconcile the inventory with the cap log, which made post-mortem triage hard. The latest stderr also exposes an internal-consistency bug in the drop log itself: `AG AG4 patch cap (causal-first): kept 3 of 15. Dropped proposal_ids=['P001#2', 'P002#1', 'P001#2', 'P001#3', 'P001#4', 'P002#3', 'P002#4', 'P003#1']` — `P001#2` is listed twice in a single drop list. Either patch expansion produces duplicate `expanded_patch_id`s or the drop log doesn't deduplicate. Either way, the cap log is unreconcilable with the proposal inventory until both are deduped by stable identity.
- Quarantine soft-skipped unresolved hard QIDs after rollback verification failed.
- The control-plane acceptance gate rejected an action group that delivered a +5pp net gain and fixed its declared causal target, because a single passing-to-hard regression breached `max_new_passing_to_hard_regressions` (which defaults to `0`) instead of being absorbed into bounded regression debt under the operator-configured `max_new_hard_regressions=1`. Concretely: AG4 fixed `gs_001` (declared target), regressed `gs_021` from passing to hard, accuracy went +5pp, and the gate emitted `rejected_unbounded_collateral` with `regression_debt_qids=(none)` instead of `accepted_with_regression_debt` with `regression_debt_qids=("gs_021",)`.
- Benchmark SQL validation exposed deterministic hygiene bugs: trailing semicolons inside subquery wrappers and generated SQL with filters not present in the question.
- The GT repair LLM-call path is brittle to empty / non-JSON model output. The latest stderr shows `_extract_json` raising `JSONDecodeError: Expecting value: line 1 column 1 (char 0)` from inside `_attempt_gt_repair(...)` when the LLM returned an empty response. That should be a typed soft failure ("no repair available", continue), not a stack trace.
- The strategist keeps under-covering hard clusters: iter_01 missed `H004`, iter_02 missed `H003` and `H004`, iter_03 missed `H001`. The harness already appends diagnostic AGs as a fallback, but it does not log *why* the strategist missed those clusters, so the next debugger has no signal to choose between "no RCA card produced", "token budget truncation", "schema validation rejected the strategist response", or "selection bug". This plan adds diagnostics — not behavior changes — for that gap.
- The codebase still has too many overlapping names for "what went wrong": `failure_type`, `root_cause`, `primary_kind`, `DiffKind`, RCA defect/theme labels.

This plan is a follow-on to:

- `packages/genie-space-optimizer/docs/2026-04-29-lever-loop-control-spine-static-replay-plan.md`
- `packages/genie-space-optimizer/docs/2026-04-29-unified-rca-prompt-alignment-plan.md`

It does not duplicate accepted-baseline row wiring, regression debt, or the existing static replay harness. It extends them for the newly observed failure modes.

## Final Log-Driven Execution Order

Implement these tasks in this order. Do not start prompt-alignment work until these control-plane foundations are stable:

1. Task 1: parsed-to-parsed rollback snapshot contract with exportable config normalization and first-diff diagnostics.
2. Task 2: in-memory pre-AG snapshot capture plus terminal rollback-verification failure.
3. Task 3: narrow Delta concurrent-write retry for run status persistence.
4. Task 4 and Task 5: causal attribution tier, metadata backfill, and cap-decision identity fields so RCA patches survive the cap and cap rows can be reconciled against the proposal inventory.
5. Task 6: target-fixed gate instrumentation and quarantine guard for any untrusted state.
6. Task 7: regression-debt accounting fix so a single bounded passing-to-hard regression cannot reject a net-positive AG that fixed its declared causal target.
7. Task 8: benchmark SQL hygiene.
8. Task 9: static replay for the observed patch-selection, `target_fixed_qids`, and AG4 regression-debt shapes.
9. Task 10: canonical schema document.
10. Tasks 11-12: focused tests and one-iteration live validation.

---

## File Structure

- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/snapshot_contract.py`
  - Captures a pre-AG live Genie Space snapshot.
  - Computes stable snapshot digests.
  - Compares expected parsed Genie config to live `fetch_space_config(...)._parsed_space` after `strip_non_exportable_fields(...)` and `sort_genie_config(...)`.
  - Emits `expected_digest`, `live_digest`, and a `first_diff_path` / `first_diff_expected` / `first_diff_live` diagnostic when content differs.

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
  - At the start of `_run_lever_loop(...)`, log a structured `RUN-LEVEL CONFIG SNAPSHOT MISSING` warning whenever the run row lacks a `config_snapshot`, and perform a one-shot bounded API fallback so the trigger-time-capture contract is audible in stderr.
  - Capture a fresh pre-AG snapshot immediately before `apply_patch_set(...)`.
  - Fail the AG before patch application if the snapshot cannot be captured.
  - Use the in-memory pre-AG snapshot for rollback and `verify_rollback_restored(...)`.
  - Make corrected rollback verification failure terminal for the run by raising `FailedRollbackVerification`, instead of printing "Halting further AGs" and continuing into the next AG.
  - Log a gate disagreement diagnostic when `post_hard < pre_hard` but `target_fixed_qids` is empty.
  - Log a structured strategist-coverage-gap diagnostic whenever the harness appends diagnostic AGs because the strategist did not cover one or more patchable hard clusters.
  - Suppress quarantine state mutation whenever state is untrusted.
  - Backfill AG-level and cluster-level causal metadata onto broad strategist proposals before patch selection.

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py`
  - Retry `genie_opt_runs` updates that fail with Delta concurrent-write conflicts.
  - Keep retries narrow: only retry recognized concurrent-write conflict classes.

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py`
  - Add a causal attribution tier to cap sorting.
  - Prefer explicit `rca_id` + target QIDs over AG-level fallback QIDs when relevance ties.
  - Emit selection decisions with `causal_attribution_tier`, `parent_proposal_id`, `expanded_patch_id`, `rca_id`, `target_qids`, `_grounding_target_qids`, `lever`, and `patch_type` so cap log rows can be reconciled against the proposal inventory and against the strategist's RCA cards.

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py`
  - Treat `max_new_passing_to_hard_regressions` as derived from `max_new_hard_regressions` by default so a single bounded passing-to-hard regression cannot reject a net-positive AG that fixed its declared causal target.
  - Keep `max_new_passing_to_hard_regressions` as an explicit override knob for callers that want stricter passing-to-hard policy than overall hard-regression policy.

- Modify: `packages/genie-space-optimizer/tests/unit/test_control_plane.py`
  - Replay the exact AG4 control-plane decision shape from the latest cluster log.
  - Assert `accepted_with_regression_debt` with `regression_debt_qids=("gs_021",)` when a single passing-to-hard regression accompanies a target-fix and a positive net gain under `max_new_hard_regressions=1`.

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/static_judge_replay.py`
  - Return attribution tiers in replay output.
  - Add an observed-log replay case that proves RCA-attributed patches survive a three-patch cap.

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/evaluation.py`
  - Strip trailing statement semicolons before wrapping SQL in `SELECT * FROM (...) _gvse_sample LIMIT n`.
  - Make `_extract_json(...)` return a typed sentinel for empty / whitespace-only / non-JSON LLM responses instead of raising `JSONDecodeError`. Callers that want to keep raising can opt in via an explicit flag.

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/benchmarks.py`
  - Add a deterministic same-store filter alignment guard before the LLM alignment check.

- Create: `packages/genie-space-optimizer/docs/canonical-schema.md`
  - Canonical vocabulary table.
  - Typed data-flow contract.
  - Determinism declaration.
  - Review/freeze policy.

- Create: `packages/genie-space-optimizer/tests/unit/test_snapshot_contract.py`
  - Pure snapshot digest and live-compare tests.

- Modify: `packages/genie-space-optimizer/tests/unit/test_preflight_substeps.py`
  - Narrow test for retrying `config_snapshot` updates on Delta concurrent append.

- Modify: `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`
  - Tests for attribution-tier sorting and target-aware cap behavior.

- Create: `packages/genie-space-optimizer/tests/unit/test_patch_causal_backfill.py`
  - Source-level or pure helper tests for AG/cluster causal metadata backfill.

- Modify: `packages/genie-space-optimizer/tests/unit/test_static_judge_replay.py`
  - Observed-log replay covering RCA patch selection, accepted-with-debt behavior, and the exact AG1 `target_fixed_qids` delta.

- Create: `packages/genie-space-optimizer/tests/unit/test_benchmark_sql_hygiene.py`
  - Tests for trailing-semicolon stripping and same-store filter alignment.

- Create: `packages/genie-space-optimizer/tests/unit/test_evaluation_extract_json.py`
  - Tests that `_extract_json(...)` returns a typed sentinel for empty / whitespace-only / non-JSON / empty-fenced-block content and still parses valid JSON, with an explicit `strict` opt-in for callers that want the old raise-on-error behavior.

---

## Task 1: Add A Pre-AG Snapshot Contract

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/snapshot_contract.py`
- Create: `packages/genie-space-optimizer/tests/unit/test_snapshot_contract.py`

- [ ] **Step 1: Write the failing snapshot contract tests**

Create `packages/genie-space-optimizer/tests/unit/test_snapshot_contract.py`:

```python
from __future__ import annotations


def test_snapshot_digest_ignores_runtime_private_keys() -> None:
    from genie_space_optimizer.optimization.snapshot_contract import snapshot_digest

    left = {
        "data_sources": {"tables": [{"name": "sales"}]},
        "_uc_columns": [{"name": "runtime-only"}],
    }
    right = {
        "data_sources": {"tables": [{"name": "sales"}]},
        "_different_runtime_key": "ignored",
    }

    assert snapshot_digest(left) == snapshot_digest(right)


def test_compare_live_to_expected_uses_live_parsed_space_not_full_api_response(monkeypatch) -> None:
    from genie_space_optimizer.optimization import snapshot_contract

    parsed = {
        "instructions": {"text_instructions": [{"content": "before"}]},
        "data_sources": {"tables": [{"name": "sales"}]},
    }
    monkeypatch.setattr(
        snapshot_contract,
        "fetch_space_config",
        lambda _w, _space_id: {
            "id": "space_1",
            "title": "Runtime metadata must not participate in rollback compare",
            "serialized_space": "{\"instructions\": {}}",
            "_parsed_space": dict(parsed),
            "_uc_columns": [{"runtime": "ignored"}],
        },
    )

    result = snapshot_contract.compare_live_to_expected_snapshot(
        w=object(),
        space_id="space_1",
        expected_snapshot=dict(parsed),
    )

    assert result["verified"] is True
    assert result["reason"] == "matched_pre_snapshot"
    assert result["expected_digest"] == result["live_digest"]


def test_compare_live_to_expected_reports_first_meaningful_diff(monkeypatch) -> None:
    from genie_space_optimizer.optimization import snapshot_contract

    monkeypatch.setattr(
        snapshot_contract,
        "fetch_space_config",
        lambda _w, _space_id: {
            "_parsed_space": {
                "instructions": {
                    "text_instructions": [{"content": "after rollback"}],
                },
            },
        },
    )

    result = snapshot_contract.compare_live_to_expected_snapshot(
        w=object(),
        space_id="space_1",
        expected_snapshot={
            "instructions": {
                "text_instructions": [{"content": "before rollback"}],
            },
        },
    )

    assert result["verified"] is False
    assert result["reason"] == "live_config_differs_from_pre_snapshot"
    assert result["expected_digest"] != result["live_digest"]
    assert result["first_diff_path"] == "instructions.text_instructions[0].content"
    assert result["first_diff_expected"] == "before rollback"
    assert result["first_diff_live"] == "after rollback"


def test_capture_pre_ag_snapshot_returns_snapshot_and_digest(monkeypatch) -> None:
    from genie_space_optimizer.optimization import snapshot_contract

    monkeypatch.setattr(
        snapshot_contract,
        "fetch_space_config",
        lambda _w, _space_id: {
            "serialized_space": "{\"instructions\": {}}",
            "_parsed_space": {
                "instructions": {"text_instructions": [{"content": "before"}]},
            },
        },
    )

    captured = snapshot_contract.capture_pre_ag_snapshot(
        w=object(),
        space_id="space_1",
        ag_id="AG1",
    )

    assert captured["captured"] is True
    assert captured["ag_id"] == "AG1"
    assert captured["snapshot"]["instructions"]["text_instructions"][0]["content"] == "before"
    assert len(captured["digest"]) == 64
```

- [ ] **Step 2: Run the tests and verify they fail**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_snapshot_contract.py -q
```

Expected before implementation:

```text
FAILED tests/unit/test_snapshot_contract.py::test_snapshot_digest_ignores_runtime_private_keys
```

- [ ] **Step 3: Implement the snapshot contract helper**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/snapshot_contract.py`:

```python
"""Pre-action-group Genie Space snapshot contract.

The live optimizer uses this module to make rollback verification depend on
the in-process pre-AG snapshot instead of a Delta row that may be stale or
missing after a concurrent-write conflict.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.genie_client import (
    fetch_space_config,
    sort_genie_config,
    strip_non_exportable_fields,
)


_HIGH_SIGNAL_PATHS: tuple[tuple[str, ...], ...] = (
    ("data_sources", "tables"),
    ("data_sources", "metric_views"),
    ("instructions", "text_instructions"),
    ("instructions", "sql_snippets"),
    ("instructions", "example_question_sqls"),
)


def _parsed_space(config: dict[str, Any] | None) -> dict[str, Any]:
    """Return the parsed Genie `serialized_space` shape from a fetch response."""
    if not isinstance(config, dict):
        return {}
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return parsed
    serialized = config.get("serialized_space")
    if isinstance(serialized, dict):
        return serialized
    return config


def canonical_snapshot(value: Any) -> Any:
    """Return a stable compare shape for Genie Space snapshots.

    The rollback contract is the parsed Genie config, not the full API response.
    Normalize through the same exportable/sorted helpers used before PATCH so
    fields like `_uc_columns`, `_data_profile`, and `uc_comment` do not create
    false rollback mismatches.
    """
    if isinstance(value, dict):
        try:
            value = sort_genie_config(strip_non_exportable_fields(value))
        except Exception:
            value = dict(value)
    if isinstance(value, dict):
        return {
            str(k): canonical_snapshot(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
            if not str(k).startswith("_")
        }
    if isinstance(value, list):
        return [canonical_snapshot(item) for item in value]
    return value


def snapshot_digest(snapshot: dict[str, Any] | None) -> str:
    payload = json.dumps(
        canonical_snapshot(snapshot or {}),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _get_path(value: Any, path: tuple[str, ...]) -> Any:
    current = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def high_signal_projection(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    normalized = canonical_snapshot(snapshot or {})
    return {
        ".".join(path): _get_path(normalized, path)
        for path in _HIGH_SIGNAL_PATHS
    }


def _first_diff(expected: Any, live: Any, path: str = "") -> dict[str, Any] | None:
    if expected == live:
        return None
    if isinstance(expected, dict) and isinstance(live, dict):
        keys = sorted(set(expected) | set(live), key=str)
        for key in keys:
            child = _first_diff(
                expected.get(key),
                live.get(key),
                f"{path}.{key}" if path else str(key),
            )
            if child is not None:
                return child
    if isinstance(expected, list) and isinstance(live, list):
        for idx in range(max(len(expected), len(live))):
            expected_item = expected[idx] if idx < len(expected) else None
            live_item = live[idx] if idx < len(live) else None
            child = _first_diff(expected_item, live_item, f"{path}[{idx}]")
            if child is not None:
                return child
    return {
        "first_diff_path": path or "$",
        "first_diff_expected": expected,
        "first_diff_live": live,
    }


def capture_pre_ag_snapshot(
    *,
    w: WorkspaceClient | Any,
    space_id: str,
    ag_id: str,
) -> dict[str, Any]:
    try:
        snapshot = fetch_space_config(w, space_id)
    except Exception as exc:
        return {
            "captured": False,
            "ag_id": ag_id,
            "reason": "fetch_failed",
            "error": str(exc)[:500],
            "snapshot": {},
            "digest": "",
        }
    parsed = _parsed_space(snapshot)
    return {
        "captured": True,
        "ag_id": ag_id,
        "reason": "captured",
        "snapshot": parsed,
        "digest": snapshot_digest(parsed),
    }


def compare_live_to_expected_snapshot(
    *,
    w: WorkspaceClient | Any | None,
    space_id: str,
    expected_snapshot: dict[str, Any],
) -> dict[str, Any]:
    if w is None:
        return {"verified": True, "reason": "no_workspace_client"}
    try:
        live = fetch_space_config(w, space_id)
    except Exception as exc:
        return {
            "verified": False,
            "reason": "fetch_failed",
            "error": str(exc)[:500],
        }

    expected_norm = canonical_snapshot(expected_snapshot or {})
    live_norm = canonical_snapshot(_parsed_space(live))
    expected_digest = snapshot_digest(expected_norm)
    live_digest = snapshot_digest(live_norm)
    if expected_digest == live_digest:
        return {
            "verified": True,
            "reason": "matched_pre_snapshot",
            "expected_digest": expected_digest,
            "live_digest": live_digest,
        }

    expected_signal = high_signal_projection(expected_norm)
    live_signal = high_signal_projection(live_norm)
    diff = _first_diff(expected_norm, live_norm) or {}
    if expected_signal == live_signal:
        return {
            "verified": True,
            "reason": "matched_high_signal_config",
            "expected_digest": expected_digest,
            "live_digest": live_digest,
            **diff,
        }
    return {
        "verified": False,
        "reason": "live_config_differs_from_pre_snapshot",
        "expected_digest": expected_digest,
        "live_digest": live_digest,
        **diff,
    }
```

- [ ] **Step 4: Run the tests and verify they pass**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_snapshot_contract.py -q
```

Expected:

```text
4 passed
```

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/snapshot_contract.py packages/genie-space-optimizer/tests/unit/test_snapshot_contract.py
git commit -m "fix(optimizer): add pre-ag snapshot contract"
```

---

## Task 2: Use The In-Memory Snapshot And Make Failed Verification Terminal

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_snapshot_contract.py`

- [ ] **Step 1: Add a source-level harness test for pre-AG capture**

Append to `packages/genie-space-optimizer/tests/unit/test_snapshot_contract.py`:

```python
def test_harness_captures_pre_ag_snapshot_before_apply() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)

    capture_idx = source.index("capture_pre_ag_snapshot(")
    apply_idx = source.index("        apply_log = apply_patch_set(")
    rollback_idx = source.index("rollback(apply_log, w, space_id,")

    assert capture_idx < apply_idx
    assert "metadata_snapshot = _pre_ag_snapshot_capture[\"snapshot\"]" in source
    assert "expected_snapshot=metadata_snapshot" in source
    assert rollback_idx > apply_idx


def test_failed_rollback_verification_is_terminal() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness)

    assert "class FailedRollbackVerification(RuntimeError):" in source
    assert 'convergence_reason="failed_rollback_verification"' in source
    assert "raise FailedRollbackVerification(" in source
    assert "_kv(\"First diff\", _restore_decision.get(\"first_diff_path\", \"(none)\"))" in source
```

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_snapshot_contract.py::test_harness_captures_pre_ag_snapshot_before_apply \
  tests/unit/test_snapshot_contract.py::test_failed_rollback_verification_is_terminal \
  -q
```

Expected before implementation:

```text
FAILED tests/unit/test_snapshot_contract.py::test_harness_captures_pre_ag_snapshot_before_apply
FAILED tests/unit/test_snapshot_contract.py::test_failed_rollback_verification_is_terminal
```

- [ ] **Step 3: Replace rollback verification internals with the snapshot contract helper**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py`, replace the body of `verify_rollback_restored(...)` with:

```python
def verify_rollback_restored(
    *,
    w: WorkspaceClient | None,
    space_id: str,
    expected_snapshot: dict,
) -> dict:
    """Fetch the live space and confirm rollback restored the pre-AG config."""
    from genie_space_optimizer.optimization.snapshot_contract import (
        compare_live_to_expected_snapshot,
    )

    return compare_live_to_expected_snapshot(
        w=w,
        space_id=space_id,
        expected_snapshot=expected_snapshot,
    )
```

Leave `_canonical_for_rollback_compare(...)` in place for one release only if other callers import it; otherwise remove it in the same commit after a repository-wide search confirms no references.

- [ ] **Step 4: Capture the live snapshot before patch application**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, immediately before:

```python
        apply_log = apply_patch_set(
            w, space_id, patches, metadata_snapshot, apply_mode=apply_mode,
        )
```

insert:

```python
        from genie_space_optimizer.optimization.snapshot_contract import (
            capture_pre_ag_snapshot,
        )

        _pre_ag_snapshot_capture = capture_pre_ag_snapshot(
            w=w,
            space_id=space_id,
            ag_id=ag_id,
        )
        if not _pre_ag_snapshot_capture.get("captured"):
            reason = _pre_ag_snapshot_capture.get("reason", "pre_ag_snapshot_failed")
            logger.error(
                "AG %s: could not capture pre-AG snapshot before apply "
                "(reason=%s). Skipping patch application.",
                ag_id,
                reason,
            )
            print(
                _section(f"[{ag_id}] SKIP APPLY: PRE-AG SNAPSHOT FAILED", "!") + "\n"
                + _kv("Reason", reason) + "\n"
                + _bar("!")
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter,
                ag_id=ag_id,
                accepted=False,
                levers=[int(lk) for lk in lever_keys],
                target_objects=[],
                prev_scores=best_scores,
                new_scores=best_scores,
                rollback_reason="pre_ag_snapshot_failed",
                patches=patches,
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids or set(),
                reflection_text=(
                    "Skipped patch application because the pre-AG live "
                    "Genie Space snapshot could not be captured."
                ),
                refinement_mode="diagnostic",
                **_ag_identity_kwargs,
            ))
            pending_action_groups = []
            pending_strategy = None
            continue

        metadata_snapshot = _pre_ag_snapshot_capture["snapshot"]
        logger.info(
            "pre-AG snapshot captured for AG %s digest=%s",
            ag_id,
            _pre_ag_snapshot_capture.get("digest", ""),
        )
```

This makes `metadata_snapshot` the actual parsed live pre-AG state passed to `apply_patch_set(...)`, `rollback(...)`, and `verify_rollback_restored(...)`.

- [ ] **Step 5: Add terminal rollback-verification failure**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, near `OptimizationResult`, add:

```python
class FailedRollbackVerification(RuntimeError):
    """Raised after rollback leaves live Genie config in an untrusted state."""
```

In the existing `if not _restore_decision.get("verified", True):` block, replace the current "halting further AGs" behavior:

```python
                    pending_action_groups = []
                    pending_strategy = None
```

with:

```python
                    update_run_status(
                        spark,
                        run_id,
                        catalog,
                        schema,
                        status="FAILED",
                        convergence_reason="failed_rollback_verification",
                    )
                    raise FailedRollbackVerification(
                        json.dumps(_restore_decision, default=str)[:1000]
                    )
```

This is intentionally terminal. After the corrected parsed-config verifier, a mismatch means the loop cannot safely learn, quarantine, or ask the strategist for another AG against the live space.

- [ ] **Step 6: Add rollback-verification digest and first-diff logging**

In the existing `if not _restore_decision.get("verified", True):` block in `harness.py`, extend the print block with:

```python
                        + _kv("Expected digest", _restore_decision.get("expected_digest", "(none)")) + "\n"
                        + _kv("Live digest", _restore_decision.get("live_digest", "(none)")) + "\n"
                        + _kv("First diff", _restore_decision.get("first_diff_path", "(none)")) + "\n"
```

- [ ] **Step 7: Surface the trigger-time baseline-snapshot ownership contract**

The run-level `config_snapshot` belongs to the app backend, not the lever loop. It must be written into the `genie_opt_runs` row at run-trigger time, before the lever loop starts. Lazy fetch from inside the loop is a degraded fallback that fails on serverless when the runtime identity lacks Genie Space `Can Edit` permission. The latest stderr shows the warning text already says so: `No config snapshot found in run row for ... — fetching from API. This may fail on serverless if the runtime identity lacks Genie Space 'Can Edit' permission. The app backend should capture the snapshot at trigger time.` The harness today silently falls back to API fetch and prints that message at info level on stdout. We need an explicit warning, an unmistakable log marker, and a one-shot bounded fallback (not an unbounded retry loop).

Add a failing source-level test first. Append to `packages/genie-space-optimizer/tests/unit/test_snapshot_contract.py`:

```python
def test_harness_warns_when_run_level_config_snapshot_is_missing() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    assert "RUN-LEVEL CONFIG SNAPSHOT MISSING" in source
    assert "should have been captured at trigger time" in source
    assert "capture_pre_ag_snapshot(" in source
```

Run it and verify it fails:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_snapshot_contract.py::test_harness_warns_when_run_level_config_snapshot_is_missing \
  -q
```

Expected before implementation:

```text
FAILED tests/unit/test_snapshot_contract.py::test_harness_warns_when_run_level_config_snapshot_is_missing
```

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, near the top of `_run_lever_loop(...)` immediately after the run row is loaded (and before the first iteration begins), add:

```python
    if not (run_row or {}).get("config_snapshot"):
        logger.warning(
            "RUN-LEVEL CONFIG SNAPSHOT MISSING for run_id=%s. The run-level "
            "config snapshot should have been captured at trigger time by the app "
            "backend before the lever loop started. Falling back to a one-shot "
            "API fetch; this will fail on serverless if the runtime identity "
            "lacks Genie Space 'Can Edit' permission.",
            run_id,
        )
        try:
            _fallback_baseline = capture_pre_ag_snapshot(
                w=w, space_id=space_id, ag_id="run_baseline"
            )
        except Exception:
            logger.exception(
                "Run-level baseline snapshot fallback fetch failed; downstream "
                "rollback verification will fail terminally."
            )
            _fallback_baseline = None
```

This change does not alter pre-AG snapshot capture (Step 3 keeps owning that). It only makes the run-level baseline contract auditable in stderr and adds a typed fallback that does not silently retry.

Re-run the focused snapshot tests to confirm everything passes:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_snapshot_contract.py -q
```

Expected:

```text
7 passed
```

- [ ] **Step 8: Run focused tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_snapshot_contract.py -q
```

Expected:

```text
7 passed
```

- [ ] **Step 9: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/applier.py packages/genie-space-optimizer/tests/unit/test_snapshot_contract.py
git commit -m "fix(optimizer): verify rollback against pre-ag live snapshot"
```

---

## Task 3: Retry Delta Concurrent Writes For Run Status Updates

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_preflight_substeps.py`

- [ ] **Step 1: Add the failing retry test**

Append to `packages/genie-space-optimizer/tests/unit/test_preflight_substeps.py`:

```python
def test_update_run_status_retries_delta_concurrent_append(monkeypatch) -> None:
    from genie_space_optimizer.optimization import state

    calls: list[dict] = []

    class ConcurrentAppendLike(Exception):
        pass

    def fake_update_row(_spark, _catalog, _schema, _table, _keys, updates):
        calls.append(updates)
        if len(calls) == 1:
            raise ConcurrentAppendLike(
                "[DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT] Transaction conflict detected"
            )

    monkeypatch.setattr(state, "update_row", fake_update_row)
    monkeypatch.setattr(state.time, "sleep", lambda _seconds: None)

    state.update_run_status(
        spark=object(),
        run_id="run_1",
        catalog="cat",
        schema="sch",
        config_snapshot={"serialized_space": {"name": "snapshot"}},
    )

    assert len(calls) == 2
    assert "config_snapshot" in calls[1]
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_preflight_substeps.py::test_update_run_status_retries_delta_concurrent_append -q
```

Expected before implementation:

```text
FAILED tests/unit/test_preflight_substeps.py::test_update_run_status_retries_delta_concurrent_append
```

- [ ] **Step 3: Add narrow retry logic**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py`, add `import time` near the existing imports if absent, then add:

```python
def _is_delta_concurrent_write_conflict(exc: BaseException) -> bool:
    text = str(exc)
    return (
        "DELTA_CONCURRENT_APPEND" in text
        or "ConcurrentAppendException" in text
        or "Transaction conflict detected" in text
    )


def _update_row_with_delta_retry(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    keys: dict[str, Any],
    updates: dict[str, Any],
    *,
    attempts: int = 3,
) -> None:
    last_exc: BaseException | None = None
    for attempt in range(attempts):
        try:
            update_row(spark, catalog, schema, table, keys, updates)
            return
        except Exception as exc:
            if not _is_delta_concurrent_write_conflict(exc) or attempt == attempts - 1:
                raise
            last_exc = exc
            time.sleep(0.25 * (attempt + 1))
    if last_exc is not None:
        raise last_exc
```

- [ ] **Step 4: Use retry helper in `update_run_status`**

Replace:

```python
    update_row(spark, catalog, schema, TABLE_RUNS, {"run_id": run_id}, updates)
```

with:

```python
    _update_row_with_delta_retry(
        spark,
        catalog,
        schema,
        TABLE_RUNS,
        {"run_id": run_id},
        updates,
    )
```

- [ ] **Step 5: Run the focused test**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_preflight_substeps.py::test_update_run_status_retries_delta_concurrent_append -q
```

Expected:

```text
1 passed
```

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state.py packages/genie-space-optimizer/tests/unit/test_preflight_substeps.py
git commit -m "fix(optimizer): retry run status writes on delta conflicts"
```

---

## Task 4: Preserve RCA-Attributed Patches In The Patch Cap

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`

- [ ] **Step 1: Add a failing attribution-tier cap test**

Append to `packages/genie-space-optimizer/tests/unit/test_patch_selection.py`:

```python
def test_target_aware_cap_prefers_explicit_rca_attribution_over_broad_ag_ties() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P002_broad_location",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "risk_level": "low",
            "target_qids": ["q007", "q005", "q002", "q009"],
            "source_cluster_ids": ["H001", "H003", "H005", "H006"],
        },
        {
            "proposal_id": "P008_rca_sales_day",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "risk_level": "low",
            "target_qids": ["q007"],
            "rca_id": "rca_q007_measure_swap",
        },
        {
            "proposal_id": "P047_filter",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "relevance_score": 1.0,
            "risk_level": "low",
            "_grounding_target_qids": ["q007"],
            "rca_id": "rca_q007_filter_logic_mismatch",
        },
        {
            "proposal_id": "P045_rewrite_instruction",
            "type": "rewrite_instruction",
            "lever": 5,
            "relevance_score": 1.0,
            "risk_level": "high",
            "target_qids": ["q007", "q005", "q002", "q009"],
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q007", "q005", "q002", "q009"),
        max_patches=3,
    )

    selected_ids = [p["proposal_id"] for p in selected]
    assert "P008_rca_sales_day" in selected_ids
    assert "P047_filter" in selected_ids
    assert selected_ids.index("P008_rca_sales_day") < selected_ids.index("P002_broad_location")
    assert {
        d["proposal_id"]: d["causal_attribution_tier"] for d in decisions
    }["P008_rca_sales_day"] > {
        d["proposal_id"]: d["causal_attribution_tier"] for d in decisions
    }["P002_broad_location"]
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_selection.py::test_target_aware_cap_prefers_explicit_rca_attribution_over_broad_ag_ties -q
```

Expected before implementation:

```text
FAILED tests/unit/test_patch_selection.py::test_target_aware_cap_prefers_explicit_rca_attribution_over_broad_ag_ties
```

- [ ] **Step 3: Add attribution tier helpers**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py`, add:

```python
def _has_target_qids(patch: dict[str, Any]) -> bool:
    return bool(_target_qids(patch))


def causal_attribution_tier(patch: dict[str, Any]) -> int:
    """Return how specifically this patch is tied to a causal failure.

    3 = explicit RCA/theme plus target QIDs
    2 = target QIDs or grounding target QIDs
    1 = source cluster/action group only
    0 = no causal attribution
    """
    has_rca = bool(str(patch.get("rca_id") or "").strip())
    has_qids = _has_target_qids(patch)
    has_cluster = bool(patch.get("source_cluster_ids") or patch.get("primary_cluster_id"))
    has_ag = bool(patch.get("action_group_id") or patch.get("ag_id"))
    if has_rca and has_qids:
        return 3
    if has_qids:
        return 2
    if has_cluster or has_ag:
        return 1
    return 0
```

- [ ] **Step 4: Use attribution tier in global cap sorting**

In `select_causal_patch_cap(...)`, update `sort_key(...)` to include attribution immediately after relevance:

```python
            return (
                -relevance,
                -causal_attribution_tier(patch),
                -diversity_bonus,
                _risk_rank(patch),
                -_score(patch, "confidence"),
                -_score(patch, "net_impact"),
                idx,
            )
```

Add `"causal_attribution_tier": causal_attribution_tier(patch)` to every decision row produced in this function.

- [ ] **Step 5: Use attribution tier in target-aware candidate sorting**

In `select_target_aware_causal_patch_cap(...)`, update the target-candidate sort key:

```python
            key=lambda item: (
                -_score(item[1], "relevance_score"),
                -causal_attribution_tier(item[1]),
                _risk_rank(item[1]),
                -_score(item[1], "confidence"),
                item[0],
            ),
```

Add `"causal_attribution_tier": causal_attribution_tier(patch)` to every decision row produced in this function.

- [ ] **Step 5a: Emit reconciliation identity fields on every cap-decision row**

The latest cluster log shows the proposal inventory (`PROPOSAL INVENTORY`) and the patch-cap log (`PATCH CAP APPLIED`) using different identifiers (e.g. `P001 [AG4]` in one block and `P001#1`, `P001#5`, `P002#2` in the other) for the same physical patch. This makes triage hard. In both `select_causal_patch_cap(...)` and `select_target_aware_causal_patch_cap(...)`, every decision row must include the following identity fields, derived from the input patch dict:

```python
            decision = {
                "proposal_id": patch.get("proposal_id"),
                "parent_proposal_id": patch.get("parent_proposal_id") or patch.get("proposal_id"),
                "expanded_patch_id": patch.get("expanded_patch_id") or patch.get("proposal_id"),
                "lever": patch.get("lever"),
                "patch_type": patch.get("type"),
                "rca_id": patch.get("rca_id"),
                "target_qids": list(patch.get("target_qids") or []),
                "_grounding_target_qids": list(patch.get("_grounding_target_qids") or []),
                "causal_attribution_tier": causal_attribution_tier(patch),
                # ... existing decision fields (kept, dropped, reason, relevance, risk, etc.)
            }
```

`parent_proposal_id` is the strategist proposal id before any per-target/per-cluster expansion. `expanded_patch_id` is the per-row id used inside the cap (e.g. `P001#5`). When neither is present they fall back to `proposal_id` so the row is always self-describing.

Append this assertion to `test_target_aware_cap_prefers_explicit_rca_attribution_over_broad_ag_ties` to lock the contract:

```python
    by_id = {d["proposal_id"]: d for d in decisions}
    rca_decision = by_id["P008_rca_sales_day"]
    assert rca_decision["rca_id"] == "rca_q007_measure_swap"
    assert rca_decision["target_qids"] == ["q007"]
    assert rca_decision["lever"] == 1
    assert rca_decision["patch_type"] == "update_column_description"
    assert rca_decision["parent_proposal_id"] == "P008_rca_sales_day"
    assert rca_decision["expanded_patch_id"] == "P008_rca_sales_day"
    assert rca_decision["causal_attribution_tier"] == 3
```

- [ ] **Step 5b: Deduplicate cap selected and dropped lists by stable identity**

The latest cluster log shows `AG AG4 patch cap (causal-first): kept 3 of 15. Dropped proposal_ids=['P001#2', 'P002#1', 'P001#2', 'P001#3', 'P001#4', 'P002#3', 'P002#4', 'P003#1']`. `P001#2` appears twice in a single drop list. Either patch expansion produces duplicate `expanded_patch_id`s, or the drop log doesn't deduplicate. Either way, the cap log can't be reconciled against the proposal inventory.

Add this contract to both `select_causal_patch_cap(...)` and `select_target_aware_causal_patch_cap(...)`. Define a small helper at module scope in `patch_selection.py`:

```python
def _stable_identity(patch: dict[str, Any]) -> str:
    return str(
        patch.get("expanded_patch_id")
        or patch.get("proposal_id")
        or id(patch)
    )


def _deduplicate_decisions(decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return decisions with at most one entry per stable identity.

    The first occurrence wins. Later occurrences are dropped silently because
    they would otherwise lie about cap reconciliation downstream.
    """
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for decision in decisions:
        identity = str(
            decision.get("expanded_patch_id")
            or decision.get("proposal_id")
            or ""
        )
        if not identity:
            deduped.append(decision)
            continue
        if identity in seen:
            continue
        seen.add(identity)
        deduped.append(decision)
    return deduped
```

In both cap functions, immediately before returning, route both the kept and dropped decision lists through `_deduplicate_decisions(...)`. Apply the same dedup to the input patches before sorting so duplicate `expanded_patch_id`s never enter the cap, using the same `_stable_identity(...)` helper. Update the cap log emitter (the line that prints `Dropped proposal_ids=[...]`) to consume the deduped list so the on-stderr representation matches the in-memory state.

Add this failing test to `tests/unit/test_patch_selection.py`:

```python
def test_target_aware_cap_dedupes_selected_and_dropped_by_expanded_patch_id() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P001",
            "expanded_patch_id": "P001#2",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "risk_level": "low",
            "target_qids": ["q007"],
        },
        {
            "proposal_id": "P001",
            "expanded_patch_id": "P001#2",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "risk_level": "low",
            "target_qids": ["q007"],
        },
        {
            "proposal_id": "P002",
            "expanded_patch_id": "P002#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 0.9,
            "risk_level": "low",
            "target_qids": ["q007"],
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q007",),
        max_patches=1,
    )

    selected_identities = [
        d.get("expanded_patch_id") or d.get("proposal_id") for d in selected
    ]
    decision_identities = [
        d.get("expanded_patch_id") or d.get("proposal_id") for d in decisions
    ]

    assert selected_identities.count("P001#2") <= 1
    assert decision_identities.count("P001#2") <= 1
```

Run the test before implementing dedup and verify it fails:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_patch_selection.py::test_target_aware_cap_dedupes_selected_and_dropped_by_expanded_patch_id \
  -q
```

Expected before implementation:

```text
FAILED tests/unit/test_patch_selection.py::test_target_aware_cap_dedupes_selected_and_dropped_by_expanded_patch_id
```

- [ ] **Step 6: Run patch selection tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_selection.py -q
```

Expected:

```text
7 passed
```

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/patch_selection.py packages/genie-space-optimizer/tests/unit/test_patch_selection.py
git commit -m "fix(optimizer): prefer rca attributed patches in cap and dedupe by identity"
```

---

## Task 5: Backfill Causal Metadata Onto Strategist Proposals

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Create: `packages/genie-space-optimizer/tests/unit/test_patch_causal_backfill.py`

- [ ] **Step 1: Write the failing causal backfill tests**

Create `packages/genie-space-optimizer/tests/unit/test_patch_causal_backfill.py`:

```python
from __future__ import annotations


def test_backfill_patch_causal_metadata_uses_affected_questions() -> None:
    from genie_space_optimizer.optimization.harness import _backfill_patch_causal_metadata

    patches = [
        {"proposal_id": "P1", "type": "update_column_description", "lever": 1},
    ]
    ag = {
        "id": "AG2",
        "primary_cluster_id": "H003",
        "source_cluster_ids": ["H003"],
        "affected_questions": ["q007", "q009"],
    }

    enriched = _backfill_patch_causal_metadata(
        patches=patches,
        action_group=ag,
        source_clusters=[],
    )

    assert enriched[0]["action_group_id"] == "AG2"
    assert enriched[0]["primary_cluster_id"] == "H003"
    assert enriched[0]["source_cluster_ids"] == ["H003"]
    assert enriched[0]["target_qids"] == ["q007", "q009"]
    assert enriched[0]["_grounding_target_qids"] == ["q007", "q009"]


def test_backfill_patch_causal_metadata_preserves_explicit_rca_targets() -> None:
    from genie_space_optimizer.optimization.harness import _backfill_patch_causal_metadata

    patches = [
        {
            "proposal_id": "P_rca",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "rca_id": "rca_q007_filter",
            "target_qids": ["q007"],
        },
    ]
    ag = {
        "id": "AG2",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["q007", "q009"],
    }

    enriched = _backfill_patch_causal_metadata(
        patches=patches,
        action_group=ag,
        source_clusters=[],
    )

    assert enriched[0]["rca_id"] == "rca_q007_filter"
    assert enriched[0]["target_qids"] == ["q007"]
    assert enriched[0]["_grounding_target_qids"] == ["q007"]
```

- [ ] **Step 2: Run the tests and verify they fail**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_causal_backfill.py -q
```

Expected before implementation:

```text
FAILED tests/unit/test_patch_causal_backfill.py::test_backfill_patch_causal_metadata_uses_affected_questions
```

- [ ] **Step 3: Add the pure backfill helper**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, near `_attach_rca_theme_attribution(...)`, add:

```python
def _qid_values(raw: object) -> list[str]:
    values: list[str] = []
    for item in raw or []:
        if isinstance(item, dict):
            value = item.get("question_id") or item.get("id")
        else:
            value = item
        if value:
            values.append(str(value))
    return list(dict.fromkeys(values))


def _cluster_qids_for_ids(
    source_clusters: list[dict],
    cluster_ids: list[str],
) -> list[str]:
    wanted = {str(cid) for cid in cluster_ids or [] if str(cid)}
    qids: list[str] = []
    for cluster in source_clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        if cid in wanted:
            qids.extend(_qid_values(cluster.get("question_ids") or []))
    return list(dict.fromkeys(qids))


def _backfill_patch_causal_metadata(
    *,
    patches: list[dict],
    action_group: dict,
    source_clusters: list[dict],
) -> list[dict]:
    """Attach AG/cluster causal metadata to broad strategist proposals.

    Explicit RCA metadata always wins. This helper fills only missing
    `target_qids` / `_grounding_target_qids` / source-cluster fields so the
    patch cap can distinguish broad AG proposals from precise RCA proposals.
    """
    ag_id = str(
        action_group.get("id")
        or action_group.get("action_group_id")
        or action_group.get("ag_id")
        or ""
    )
    source_cluster_ids = [
        str(cid) for cid in (action_group.get("source_cluster_ids") or []) if str(cid)
    ]
    primary_cluster_id = str(action_group.get("primary_cluster_id") or "")
    ag_qids = _qid_values(action_group.get("affected_questions") or [])
    if not ag_qids:
        ag_qids = _cluster_qids_for_ids(source_clusters, source_cluster_ids)

    enriched: list[dict] = []
    for patch in patches or []:
        item = dict(patch)
        if ag_id and not item.get("action_group_id"):
            item["action_group_id"] = ag_id
        if primary_cluster_id and not item.get("primary_cluster_id"):
            item["primary_cluster_id"] = primary_cluster_id
        if source_cluster_ids and not item.get("source_cluster_ids"):
            item["source_cluster_ids"] = list(source_cluster_ids)

        explicit_targets = _qid_values(item.get("target_qids") or [])
        grounding_targets = _qid_values(item.get("_grounding_target_qids") or [])
        target_qids = explicit_targets or grounding_targets or ag_qids
        if target_qids:
            item["target_qids"] = list(target_qids)
            item["_grounding_target_qids"] = list(target_qids)
        enriched.append(item)
    return enriched
```

- [ ] **Step 4: Call the helper before proposal inventory and patch cap**

In `harness.py`, after all proposal-to-patch expansion is complete and before the `PROPOSAL INVENTORY` / `PATCH CAP APPLIED` block, insert:

```python
        patches = _backfill_patch_causal_metadata(
            patches=patches,
            action_group=ag,
            source_clusters=strategy.get("_source_clusters", []) if isinstance(strategy, dict) else [],
        )
```

The insertion point is before:

```python
        if len(patches) > MAX_AG_PATCHES:
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_patch_causal_backfill.py tests/unit/test_patch_selection.py -q
```

Expected:

```text
8 passed
```

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_patch_causal_backfill.py
git commit -m "fix(optimizer): backfill causal targets on strategist patches"
```

---

## Task 6: Instrument Gate Disagreement And Guard Untrusted Quarantine

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_quarantine_control_plane.py`

- [ ] **Step 1: Add source-level gate and quarantine guard tests**

Append to `packages/genie-space-optimizer/tests/unit/test_quarantine_control_plane.py`:

```python
def test_harness_logs_target_fixed_disagreement_shape() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness)

    assert "def _log_target_fixed_disagreement(" in source
    assert "CONTROL PLANE TARGET-FIXED DISAGREEMENT" in source
    assert "target_fixed_qids" in source
    assert "_log_target_fixed_disagreement(" in inspect.getsource(harness._run_gate_checks)


def test_harness_tracks_unverified_rollback_before_quarantine_mutation() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)

    assert "_rollback_state_trusted_for_quarantine = True" in source
    assert "_rollback_state_trusted_for_quarantine = False" in source
    assert "if not _rollback_state_trusted_for_quarantine:" in source
    assert "Skipping convergence quarantine because live state is untrusted" in source
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_quarantine_control_plane.py::test_harness_logs_target_fixed_disagreement_shape \
  tests/unit/test_quarantine_control_plane.py::test_harness_tracks_unverified_rollback_before_quarantine_mutation \
  -q
```

Expected before implementation:

```text
FAILED tests/unit/test_quarantine_control_plane.py::test_harness_logs_target_fixed_disagreement_shape
FAILED tests/unit/test_quarantine_control_plane.py::test_harness_tracks_unverified_rollback_before_quarantine_mutation
```

- [ ] **Step 3: Add target-fixed disagreement instrumentation**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, near `_run_gate_checks(...)`, add:

```python
def _log_target_fixed_disagreement(
    *,
    ag_id: str,
    baseline_source: str,
    pre_hard_qids: list[str],
    post_hard_qids: list[str],
    target_qids: tuple[str, ...],
    target_fixed_qids: tuple[str, ...],
) -> None:
    """Log when eval row deltas imply a fix but the control plane reports none."""
    pre_hard = set(pre_hard_qids)
    post_hard = set(post_hard_qids)
    target_set = set(str(q) for q in target_qids if str(q))
    fixed_by_delta = tuple(sorted(target_set & (pre_hard - post_hard)))
    if not fixed_by_delta or target_fixed_qids:
        return
    logger.warning(
        "CONTROL PLANE TARGET-FIXED DISAGREEMENT: AG=%s source=%s "
        "pre_hard=%s post_hard=%s target_qids=%s fixed_by_delta=%s "
        "target_fixed_qids=%s",
        ag_id,
        baseline_source,
        pre_hard_qids,
        post_hard_qids,
        list(target_qids),
        list(fixed_by_delta),
        list(target_fixed_qids),
    )
    print(
        _section("CONTROL PLANE TARGET-FIXED DISAGREEMENT", "!") + "\n"
        + _kv("AG", ag_id) + "\n"
        + _kv("Baseline source", baseline_source) + "\n"
        + _kv("Target QIDs", list(target_qids)) + "\n"
        + _kv("Fixed by hard-row delta", list(fixed_by_delta)) + "\n"
        + _kv("Control-plane target_fixed_qids", list(target_fixed_qids)) + "\n"
        + _bar("!")
    )
```

In `_run_gate_checks(...)`, immediately after `_control_plane_decision = decide_control_plane_acceptance(...)`, add:

```python
    try:
        _log_target_fixed_disagreement(
            ag_id=ag_id,
            baseline_source=_baseline_source_for_control_plane,
            pre_hard_qids=_pre_hard_for_log,
            post_hard_qids=_post_hard_for_log,
            target_qids=tuple(_target_qids),
            target_fixed_qids=tuple(_control_plane_decision.target_fixed_qids),
        )
    except Exception:
        logger.debug("Failed to log target-fixed disagreement diagnostic", exc_info=True)
```

This is instrumentation only. It decides whether the AG1 `target_fixed_qids=(none)` symptom is baseline-row plumbing, QID normalization, or set arithmetic.

- [ ] **Step 4: Add rollback trust state**

In `harness.py`, near initialization of loop-local state for `_run_lever_loop`, add:

```python
    _rollback_state_trusted_for_quarantine = True
```

Inside the rollback verification failure block immediately before raising `FailedRollbackVerification`, add:

```python
                    _rollback_state_trusted_for_quarantine = False
```

Inside the accepted-AG path after `_accepted_baseline_rows_for_control_plane` is updated, restore trust:

```python
        _rollback_state_trusted_for_quarantine = True
```

- [ ] **Step 5: Guard convergence quarantine mutation**

At the start of the convergence quarantine block in `harness.py`, immediately before computing `_soft_skip_qids` and `_quarantine_qids`, add:

```python
            if not _rollback_state_trusted_for_quarantine:
                logger.warning(
                    "Skipping convergence quarantine because live state is untrusted; "
                    "hard failures must remain visible until rollback verification passes."
                )
                _quarantine_qids = set()
                _soft_skip_qids = set()
```

Keep the existing `decide_quarantine_continuation(...)` behavior for trusted state. Task 2 should make corrected rollback verification failure terminal, so this guard is defense-in-depth for any future untrusted-state path, skipped verification path, or partial refactor.

- [ ] **Step 6: Add strategist-coverage-gap diagnostic logging**

The latest stderr shows the same coverage-gap message firing on three separate iterations:

```text
Strategist did not cover 1 patchable hard cluster(s); appending diagnostic AGs: ['H004']
Strategist did not cover 2 patchable hard cluster(s); appending diagnostic AGs: ['H003', 'H004']
Strategist did not cover 1 patchable hard cluster(s); appending diagnostic AGs: ['H001']
```

The harness already appends diagnostic AGs as a fallback, but it does not say *why* the strategist missed those clusters. That is the difference between "the strategist response was schema-rejected", "RCA cards never reached the prompt", "token budget truncation dropped them", and "the cluster was never selected". Without that signal, every iteration costs another full eval to rediscover the same gap.

This step adds a single structured diagnostic call. It does not change control flow.

Append a source-level test to `packages/genie-space-optimizer/tests/unit/test_quarantine_control_plane.py`:

```python
def test_harness_logs_strategist_coverage_gap_diagnostic_shape() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness)

    assert "def _log_strategist_coverage_gap(" in source
    assert "STRATEGIST COVERAGE GAP" in source
    assert "uncovered_cluster_ids" in source
    assert "rca_cards_present" in source
    assert "_log_strategist_coverage_gap(" in source
```

Run it and confirm it fails:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_quarantine_control_plane.py::test_harness_logs_strategist_coverage_gap_diagnostic_shape \
  -q
```

Expected before implementation:

```text
FAILED tests/unit/test_quarantine_control_plane.py::test_harness_logs_strategist_coverage_gap_diagnostic_shape
```

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`, near `_log_target_fixed_disagreement(...)`, add:

```python
def _log_strategist_coverage_gap(
    *,
    iteration: int,
    uncovered_cluster_ids: list[str],
    cluster_question_counts: dict[str, int],
    rca_cards_present: dict[str, bool],
    strategist_action_groups: int,
    strategist_input_token_estimate: int | None,
    strategist_output_truncated: bool,
) -> None:
    """Log why the strategist did not cover one or more patchable hard clusters.

    This is instrumentation only. It distinguishes between "no RCA card present"
    (RCA pipeline gap), "card present but strategist returned no proposal"
    (strategist coverage bug), and "output likely truncated" (token-budget gap).
    """
    if not uncovered_cluster_ids:
        return
    logger.warning(
        "STRATEGIST COVERAGE GAP: iter=%s uncovered_cluster_ids=%s "
        "cluster_question_counts=%s rca_cards_present=%s "
        "strategist_action_groups=%s strategist_input_token_estimate=%s "
        "strategist_output_truncated=%s",
        iteration,
        uncovered_cluster_ids,
        {cid: cluster_question_counts.get(cid) for cid in uncovered_cluster_ids},
        {cid: rca_cards_present.get(cid, False) for cid in uncovered_cluster_ids},
        strategist_action_groups,
        strategist_input_token_estimate,
        strategist_output_truncated,
    )
    print(
        _section("STRATEGIST COVERAGE GAP", "!") + "\n"
        + _kv("Iteration", iteration) + "\n"
        + _kv("Uncovered cluster ids", uncovered_cluster_ids) + "\n"
        + _kv(
            "Cluster question counts",
            {cid: cluster_question_counts.get(cid) for cid in uncovered_cluster_ids},
        ) + "\n"
        + _kv(
            "RCA cards present",
            {cid: rca_cards_present.get(cid, False) for cid in uncovered_cluster_ids},
        ) + "\n"
        + _kv("Strategist action groups returned", strategist_action_groups) + "\n"
        + _kv("Strategist input token estimate", strategist_input_token_estimate) + "\n"
        + _kv("Strategist output truncated", strategist_output_truncated) + "\n"
        + _bar("!")
    )
```

In the existing harness branch that prints `Strategist did not cover N patchable hard cluster(s); appending diagnostic AGs: [...]`, immediately before that line, call:

```python
            try:
                _log_strategist_coverage_gap(
                    iteration=iteration_idx,
                    uncovered_cluster_ids=list(uncovered_cluster_ids),
                    cluster_question_counts={
                        str(c.get("cluster_id")): len(c.get("question_ids") or [])
                        for c in source_clusters or []
                        if c.get("cluster_id")
                    },
                    rca_cards_present={
                        str(c.get("cluster_id")): bool(c.get("rca_card"))
                        for c in source_clusters or []
                        if c.get("cluster_id")
                    },
                    strategist_action_groups=len(
                        (strategy or {}).get("action_groups") or []
                    ),
                    strategist_input_token_estimate=(strategy or {}).get(
                        "_input_token_estimate"
                    ),
                    strategist_output_truncated=bool(
                        (strategy or {}).get("_output_truncated")
                    ),
                )
            except Exception:
                logger.debug(
                    "Failed to log strategist coverage gap diagnostic",
                    exc_info=True,
                )
```

The optional `_input_token_estimate` and `_output_truncated` fields are forward-compatible: if the strategist call site does not yet populate them, the diagnostic prints `None` / `False` and still distinguishes "no RCA card" from "RCA card present but strategist returned no proposal." Populating those fields is a follow-on item, not a blocker for this step.

- [ ] **Step 7: Run the quarantine tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_quarantine_control_plane.py -q
```

Expected:

```text
all selected tests pass
```

- [ ] **Step 8: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_quarantine_control_plane.py
git commit -m "fix(optimizer): instrument gate disagreements and quarantine and strategist coverage gaps"
```

---

## Task 7: Fix Regression Debt Accounting For Passing-To-Hard Collateral

**Why this task exists:** The latest cluster log captured a clear contradiction in the control-plane gate. AG4 fixed its declared causal target `gs_001`, drove accuracy from 75% to 80% (+5pp), and produced one passing-to-hard regression on `gs_021`. Under `max_new_hard_regressions=1` the gate should have returned `accepted_with_regression_debt` with `regression_debt_qids=("gs_021",)`. Instead it returned `rejected_unbounded_collateral` with `regression_debt_qids=(none)`.

The root cause is in `decide_control_plane_acceptance(...)` in `optimization/control_plane.py`. The `collateral_bounded` predicate combines two budgets:

```python
collateral_bounded = (
    regression_count <= int(max_new_hard_regressions)
    and len(passing_to_hard) <= int(max_new_passing_to_hard_regressions)
    and regression_count <= max(fixed_count, 1)
    and not protected_regressed
)
```

`max_new_passing_to_hard_regressions` defaults to `0`, and the lever-loop harness never plumbs an override. Any passing-to-hard regression therefore falsifies `collateral_bounded`, which routes the decision to `rejected_unbounded_collateral`, which clears `regression_debt_qids` (because that field is gated on `accepted`). The strategist then re-attempts the same target, the next AG produces a rollback, and the loop cannot close the AG4-shaped pattern.

The fix is to default `max_new_passing_to_hard_regressions` to `max_new_hard_regressions` so the operator-configured overall hard-regression budget governs both buckets. Callers that want stricter passing-to-hard policy than overall hard-regression policy continue to pass an explicit lower override.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_control_plane.py`

- [ ] **Step 1: Add the failing AG4 control-plane decision test**

Append to `packages/genie-space-optimizer/tests/unit/test_control_plane.py`:

```python
def test_ag4_passing_to_hard_within_overall_budget_is_accepted_with_debt() -> None:
    """Replay of the observed AG4 control-plane decision shape.

    AG4 facts from the cluster log:
      - target QID: gs_001
      - target_fixed_qids: gs_001
      - out_of_target_regressed_qids: gs_021
      - passing_to_hard_regressed_qids: gs_021 (gs_021 was passing pre-AG)
      - baseline accuracy: 75.0
      - candidate accuracy: 80.0
      - max_new_hard_regressions: 1

    Expected: accepted_with_regression_debt, regression_debt_qids=("gs_021",).
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {"question_id": "gs_001", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_021", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_004", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_018", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_026", "feedback/arbiter/value": "both_correct"},
    ]
    post_rows = [
        {"question_id": "gs_001", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_021", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_004", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_018", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_026", "feedback/arbiter/value": "both_correct"},
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=75.0,
        candidate_accuracy=80.0,
        target_qids=("gs_001",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        max_new_hard_regressions=1,
    )

    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_regression_debt"
    assert decision.target_fixed_qids == ("gs_001",)
    assert decision.target_still_hard_qids == ()
    assert decision.passing_to_hard_regressed_qids == ("gs_021",)
    assert decision.out_of_target_regressed_qids == ("gs_021",)
    assert decision.regression_debt_qids == ("gs_021",)


def test_passing_to_hard_budget_can_be_tightened_below_overall_budget() -> None:
    """Operators can still set a stricter passing-to-hard policy explicitly."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {"question_id": "q1", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "q2", "feedback/arbiter/value": "both_correct"},
    ]
    post_rows = [
        {"question_id": "q1", "feedback/arbiter/value": "both_correct"},
        {"question_id": "q2", "feedback/arbiter/value": "ground_truth_correct"},
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=50.0,
        candidate_accuracy=50.0 + 1e-6,
        target_qids=("q1",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        max_new_hard_regressions=1,
        max_new_passing_to_hard_regressions=0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "rejected_unbounded_collateral"
    assert decision.passing_to_hard_regressed_qids == ("q2",)
```

- [ ] **Step 2: Run the tests and verify they fail**

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_control_plane.py::test_ag4_passing_to_hard_within_overall_budget_is_accepted_with_debt \
  tests/unit/test_control_plane.py::test_passing_to_hard_budget_can_be_tightened_below_overall_budget \
  -q
```

Expected before implementation:

```text
FAILED tests/unit/test_control_plane.py::test_ag4_passing_to_hard_within_overall_budget_is_accepted_with_debt
```

The first test fails because the current default of `max_new_passing_to_hard_regressions=0` rejects AG4. The second test should pass already because explicit overrides already work — keep it as a regression guard.

- [ ] **Step 3: Default passing-to-hard budget to overall hard-regression budget**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py`, change the signature of `decide_control_plane_acceptance(...)`:

```python
def decide_control_plane_acceptance(
    *,
    baseline_accuracy: float,
    candidate_accuracy: float,
    target_qids: Iterable[str],
    pre_rows: Iterable[dict],
    post_rows: Iterable[dict],
    min_gain_pp: float = 0.0,
    max_new_hard_regressions: int = 1,
    max_new_passing_to_hard_regressions: int | None = None,
    protected_qids: Iterable[str] = (),
) -> ControlPlaneAcceptance:
```

Immediately after parsing the row arguments, resolve the effective passing-to-hard budget:

```python
    if max_new_passing_to_hard_regressions is None:
        effective_passing_to_hard_budget = int(max_new_hard_regressions)
    else:
        effective_passing_to_hard_budget = int(max_new_passing_to_hard_regressions)
```

Then update the `collateral_bounded` predicate to use the resolved budget:

```python
    collateral_bounded = (
        regression_count <= int(max_new_hard_regressions)
        and len(passing_to_hard) <= effective_passing_to_hard_budget
        and regression_count <= max(fixed_count, 1)
        and not protected_regressed
    )
```

Update the `decide_control_plane_acceptance` docstring to record the new default semantics:

```text
max_new_passing_to_hard_regressions:
    Optional stricter cap for passing→hard regressions. When None (the
    default) the overall ``max_new_hard_regressions`` budget governs both
    buckets so a single bounded passing→hard regression cannot reject a
    net-positive AG that fixed its declared causal target. Pass an
    explicit non-negative integer to enforce a tighter passing→hard
    policy than overall hard-regression policy.
```

Do not change any other branch in `decide_control_plane_acceptance`. The `accepted_with_regression_debt` branch already handles populating `regression_debt_qids` correctly once `collateral_bounded` is true.

- [ ] **Step 4: Run the control-plane tests and verify they pass**

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_control_plane.py -q
```

Expected:

```text
all tests pass
```

- [ ] **Step 5: Re-run dependent suites to catch policy drift**

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_control_plane.py \
  tests/unit/test_static_judge_replay.py \
  tests/unit/test_quarantine_control_plane.py \
  -q
```

Expected:

```text
all tests pass
```

If any prior test asserted that one passing-to-hard regression must reject under `max_new_hard_regressions=1` with no explicit `max_new_passing_to_hard_regressions` override, update that test to either (a) pass `max_new_passing_to_hard_regressions=0` explicitly to keep the strict policy, or (b) accept the bounded-debt outcome. Do not relax `max_new_hard_regressions` to compensate.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py packages/genie-space-optimizer/tests/unit/test_control_plane.py
git commit -m "fix(optimizer): default passing-to-hard budget to overall hard-regression budget"
```

---

## Task 8: Harden Benchmark SQL Hygiene

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/evaluation.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/benchmarks.py`
- Create: `packages/genie-space-optimizer/tests/unit/test_benchmark_sql_hygiene.py`

- [ ] **Step 1: Write failing hygiene tests**

Create `packages/genie-space-optimizer/tests/unit/test_benchmark_sql_hygiene.py`:

```python
from __future__ import annotations


def test_strip_trailing_statement_semicolon_before_sample_wrap() -> None:
    from genie_space_optimizer.optimization.evaluation import (
        _strip_trailing_statement_semicolon,
    )

    sql = "SELECT * FROM cat.sch.table ORDER BY id;\n"

    assert _strip_trailing_statement_semicolon(sql) == (
        "SELECT * FROM cat.sch.table ORDER BY id"
    )


def test_same_store_filter_alignment_rejects_unmentioned_filter() -> None:
    from genie_space_optimizer.optimization.benchmarks import (
        deterministic_question_sql_alignment_issues,
    )

    issues = deterministic_question_sql_alignment_issues(
        {
            "question": "Show country-level performance: total sales and store count.",
            "expected_sql": (
                "SELECT country_code, SUM(total_sales_usd_day) "
                "FROM mv_esr_store_sales "
                "WHERE is_finance_monthly_same_store = 'Y' "
                "GROUP BY country_code"
            ),
        }
    )

    assert issues == [
        "EXTRA_FILTER: SQL filters on is_finance_monthly_same_store but the question does not ask for same-store or finance-monthly same-store results."
    ]


def test_same_store_filter_alignment_allows_mentioned_filter() -> None:
    from genie_space_optimizer.optimization.benchmarks import (
        deterministic_question_sql_alignment_issues,
    )

    issues = deterministic_question_sql_alignment_issues(
        {
            "question": "Show same-store country-level APSD sales.",
            "expected_sql": (
                "SELECT country_code, MEASURE(apsd_sales_usd_day) "
                "FROM mv_esr_store_sales "
                "WHERE is_finance_monthly_same_store = 'Y' "
                "GROUP BY country_code"
            ),
        }
    )

    assert issues == []
```

- [ ] **Step 2: Run the tests and verify they fail**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_benchmark_sql_hygiene.py -q
```

Expected before implementation:

```text
FAILED tests/unit/test_benchmark_sql_hygiene.py::test_strip_trailing_statement_semicolon_before_sample_wrap
```

- [ ] **Step 3: Add SQL semicolon stripping**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/evaluation.py`, near the row-capture helpers, add:

```python
def _strip_trailing_statement_semicolon(sql: str) -> str:
    """Remove trailing semicolons before embedding SQL in a subquery wrapper."""
    text = str(sql or "").strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    return text
```

In the row capture function, immediately after:

```python
        resolved = resolve_sql(sql, catalog=catalog, gold_schema=schema)
```

add:

```python
        resolved = _strip_trailing_statement_semicolon(resolved)
```

This prevents:

```sql
SELECT * FROM (SELECT ... ORDER BY d.calendar_day;) _gvse_sample LIMIT 20
```

- [ ] **Step 4: Add deterministic same-store alignment issues**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/benchmarks.py`, above `validate_question_sql_alignment(...)`, add:

```python
_SAME_STORE_QUESTION_TERMS = (
    "same-store",
    "same store",
    "same-store",
    "finance monthly same-store",
    "finance monthly same store",
    "comp store",
    "comparable store",
)


def deterministic_question_sql_alignment_issues(benchmark: dict) -> list[str]:
    question = str(benchmark.get("question") or "").lower()
    sql = str(benchmark.get("expected_sql") or "").lower()
    issues: list[str] = []
    has_same_store_filter = "is_finance_monthly_same_store" in sql
    question_mentions_same_store = any(term in question for term in _SAME_STORE_QUESTION_TERMS)
    if has_same_store_filter and not question_mentions_same_store:
        issues.append(
            "EXTRA_FILTER: SQL filters on is_finance_monthly_same_store but the question does not ask for same-store or finance-monthly same-store results."
        )
    return issues
```

- [ ] **Step 5: Apply deterministic issues before LLM alignment**

In `validate_question_sql_alignment(...)`, after initializing `results` and before batching LLM calls, insert:

```python
        deterministic_issues = deterministic_question_sql_alignment_issues(b)
        if deterministic_issues:
            results.append({
                "question": b.get("question", ""),
                "aligned": False,
                "issues": deterministic_issues,
            })
            continue
```

Make sure this block runs only for benchmarks with non-empty `expected_sql`; missing SQL handling remains unchanged.

- [ ] **Step 6: Harden `_extract_json` against empty / non-JSON LLM responses**

The latest stderr shows an unhandled `JSONDecodeError` thrown out of `_attempt_gt_repair(...)`:

```text
GT repair LLM call failed for: 
Traceback ...
  File ".../optimization/evaluation.py", line 623, in _extract_json
    return json.loads(content)
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

`_extract_json` is shared by GT repair, scoring, and other LLM-call paths. When the model returns an empty string, whitespace, or a fenced block with no body, the parser raises and the caller loses the soft-failure path. The fix is to make `_extract_json(...)` return a typed sentinel (`None`) for unparseable content while still raising on invocations that explicitly opt in. Callers that already handle `None` (or fall back to "no repair available") become correct without any other change.

Append failing tests to a new file `packages/genie-space-optimizer/tests/unit/test_evaluation_extract_json.py`:

```python
from __future__ import annotations


def test_extract_json_returns_none_for_empty_string() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json("") is None


def test_extract_json_returns_none_for_whitespace_only() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json("   \n\t  ") is None


def test_extract_json_returns_none_for_fenced_block_with_no_body() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json("```json\n```") is None


def test_extract_json_returns_none_for_non_json_prose() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json("I cannot answer that.") is None


def test_extract_json_still_parses_valid_json() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json('{"verdict": "ground_truth_correct"}') == {
        "verdict": "ground_truth_correct"
    }


def test_extract_json_can_still_raise_when_strict_is_set() -> None:
    import pytest

    from genie_space_optimizer.optimization.evaluation import _extract_json

    with pytest.raises(Exception):
        _extract_json("", strict=True)
```

Run them and confirm they fail:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_evaluation_extract_json.py -q
```

Expected before implementation:

```text
FAILED tests/unit/test_evaluation_extract_json.py::test_extract_json_returns_none_for_empty_string
FAILED tests/unit/test_evaluation_extract_json.py::test_extract_json_returns_none_for_whitespace_only
FAILED tests/unit/test_evaluation_extract_json.py::test_extract_json_returns_none_for_fenced_block_with_no_body
FAILED tests/unit/test_evaluation_extract_json.py::test_extract_json_returns_none_for_non_json_prose
FAILED tests/unit/test_evaluation_extract_json.py::test_extract_json_can_still_raise_when_strict_is_set
```

Update `_extract_json(...)` in `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/evaluation.py` to:

```python
def _extract_json(content: str | None, *, strict: bool = False) -> Any:
    """Parse the first JSON object found in ``content``.

    Returns ``None`` for empty / whitespace-only / fenced-but-empty / non-JSON
    content so callers can treat "no parseable response" as a typed soft
    failure. Pass ``strict=True`` to preserve the previous raise-on-error
    behavior for code paths that require a hard failure.
    """
    if content is None:
        if strict:
            raise ValueError("No content to parse as JSON")
        return None
    text = content.strip()
    if not text:
        if strict:
            raise ValueError("Empty content cannot be parsed as JSON")
        return None
    # Strip ```json fences if present and re-check for a body.
    if text.startswith("```"):
        text = text.strip("`").strip()
        if text.lower().startswith("json"):
            text = text[len("json"):].strip()
        if not text:
            if strict:
                raise ValueError("Empty fenced block cannot be parsed as JSON")
            return None
    try:
        return json.loads(text)
    except Exception as exc:
        last_err = exc
        # ... preserve any existing fallback parse attempts here ...
        if strict:
            raise last_err
        logger.debug(
            "_extract_json could not parse content; returning None. "
            "first_120_chars=%r error=%s",
            text[:120],
            exc,
        )
        return None
```

In `_attempt_gt_repair(...)` in `harness.py` (and any other caller that previously relied on `_extract_json` raising), update the path so a `None` return is treated as "no repair available", logged at warning level, and execution continues. The logged warning replaces the raw stack trace currently produced.

Re-run the new tests and confirm they pass:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_evaluation_extract_json.py -q
```

Expected:

```text
6 passed
```

- [ ] **Step 7: Run focused tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_benchmark_sql_hygiene.py tests/unit/test_evaluation_extract_json.py -q
```

Expected:

```text
9 passed
```

- [ ] **Step 8: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/evaluation.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/benchmarks.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py packages/genie-space-optimizer/tests/unit/test_benchmark_sql_hygiene.py packages/genie-space-optimizer/tests/unit/test_evaluation_extract_json.py
git commit -m "fix(optimizer): harden benchmark sql hygiene and llm json parsing"
```

---

## Task 9: Extend Static Judge Replay With Observed Failure Shapes

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/static_judge_replay.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_static_judge_replay.py`

- [ ] **Step 1: Add the observed-log replay tests**

Append to `packages/genie-space-optimizer/tests/unit/test_static_judge_replay.py`:

```python
def test_static_replay_observed_ag1_reports_target_fixed_qid() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    baseline_rows = [
        {"question_id": "gs_026", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_021", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_004", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_001", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_018", "feedback/arbiter/value": "both_correct"},
    ]
    candidate_rows = [
        {"question_id": "gs_026", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_021", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_004", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_001", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_018", "feedback/arbiter/value": "ground_truth_correct"},
    ]
    action_group = {
        "id": "AG1",
        "source_cluster_ids": ["H004"],
        "affected_questions": ["gs_004"],
    }
    proposals = [
        {
            "proposal_id": "P004_asset_routing",
            "type": "rewrite_instruction",
            "relevance_score": 1.0,
            "rca_id": "rca_gs004_asset_routing",
            "target_qids": ["gs_004"],
        },
    ]

    result = run_static_judge_replay(
        baseline_accuracy=20.0,
        candidate_accuracy=40.0,
        baseline_rows=baseline_rows,
        candidate_rows=candidate_rows,
        action_group=action_group,
        source_clusters=[{"cluster_id": "H004", "question_ids": ["gs_004"]}],
        proposals=proposals,
        max_patches=3,
        max_new_hard_regressions=1,
    )

    assert result.acceptance.target_qids == ("gs_004",)
    assert result.acceptance.target_fixed_qids == ("gs_004",)
    assert result.acceptance.target_still_hard_qids == ()
    assert result.acceptance.regression_debt_qids == ("gs_018",)
    assert result.acceptance.reason_code == "accepted_with_regression_debt"


def test_static_replay_preserves_rca_patches_and_accepts_bounded_debt() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    baseline_rows = [
        {"question_id": "q007", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "q009", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "q005", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "q002", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "q015", "feedback/arbiter/value": "both_correct"},
    ]
    candidate_rows = [
        {"question_id": "q007", "feedback/arbiter/value": "both_correct"},
        {"question_id": "q009", "feedback/arbiter/value": "both_correct"},
        {"question_id": "q005", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "q002", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "q015", "feedback/arbiter/value": "ground_truth_correct"},
    ]
    action_group = {
        "id": "AG2",
        "source_cluster_ids": ["H001", "H003", "H005", "H006"],
        "affected_questions": ["q007", "q009", "q005", "q002"],
    }
    proposals = [
        {
            "proposal_id": "P002_broad",
            "type": "update_column_description",
            "relevance_score": 1.0,
            "target_qids": ["q007", "q009", "q005", "q002"],
        },
        {
            "proposal_id": "P008_rca",
            "type": "update_column_description",
            "relevance_score": 1.0,
            "rca_id": "rca_q007_measure_swap",
            "target_qids": ["q007"],
        },
        {
            "proposal_id": "P047_filter",
            "type": "add_sql_snippet_filter",
            "relevance_score": 1.0,
            "rca_id": "rca_q007_filter",
            "_grounding_target_qids": ["q007"],
        },
    ]

    result = run_static_judge_replay(
        baseline_accuracy=57.1,
        candidate_accuracy=64.3,
        baseline_rows=baseline_rows,
        candidate_rows=candidate_rows,
        action_group=action_group,
        source_clusters=[
            {"cluster_id": "H001", "question_ids": ["q007"]},
            {"cluster_id": "H003", "question_ids": ["q009"]},
            {"cluster_id": "H005", "question_ids": ["q005"]},
            {"cluster_id": "H006", "question_ids": ["q002"]},
        ],
        proposals=proposals,
        max_patches=3,
        max_new_hard_regressions=1,
    )

    kept_ids = [patch["proposal_id"] for patch in result.kept_patches]
    assert "P008_rca" in kept_ids
    assert "P047_filter" in kept_ids
    assert result.acceptance.accepted is True
    assert result.acceptance.reason_code == "accepted_with_regression_debt"
    assert result.acceptance.regression_debt_qids == ("q015",)


def test_static_replay_observed_ag4_passing_to_hard_is_accepted_with_debt() -> None:
    """End-to-end replay of the observed AG4 acceptance shape from the cluster log.

    AG4 facts:
      - target QID: gs_001 (declared causal target was fixed)
      - passing-to-hard regression: gs_021 (passing pre-AG, hard post-AG)
      - baseline accuracy: 75.0
      - candidate accuracy: 80.0 (+5pp)
      - max_new_hard_regressions: 1
      - max_new_passing_to_hard_regressions: not configured by harness

    Expected after Task 7 lands: the static replay returns
    ``accepted_with_regression_debt`` with ``regression_debt_qids=("gs_021",)``
    instead of ``rejected_unbounded_collateral``.
    """
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    baseline_rows = [
        {"question_id": "gs_001", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_021", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_004", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_018", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_026", "feedback/arbiter/value": "both_correct"},
    ]
    candidate_rows = [
        {"question_id": "gs_001", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_021", "feedback/arbiter/value": "ground_truth_correct"},
        {"question_id": "gs_004", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_018", "feedback/arbiter/value": "both_correct"},
        {"question_id": "gs_026", "feedback/arbiter/value": "both_correct"},
    ]
    action_group = {
        "id": "AG4",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["gs_001"],
    }
    proposals = [
        {
            "proposal_id": "P001_asset_routing",
            "type": "rewrite_instruction",
            "relevance_score": 1.0,
            "rca_id": "rca_gs001_asset_routing",
            "target_qids": ["gs_001"],
        },
    ]

    result = run_static_judge_replay(
        baseline_accuracy=75.0,
        candidate_accuracy=80.0,
        baseline_rows=baseline_rows,
        candidate_rows=candidate_rows,
        action_group=action_group,
        source_clusters=[{"cluster_id": "H001", "question_ids": ["gs_001"]}],
        proposals=proposals,
        max_patches=3,
        max_new_hard_regressions=1,
    )

    assert result.acceptance.target_qids == ("gs_001",)
    assert result.acceptance.target_fixed_qids == ("gs_001",)
    assert result.acceptance.passing_to_hard_regressed_qids == ("gs_021",)
    assert result.acceptance.regression_debt_qids == ("gs_021",)
    assert result.acceptance.reason_code == "accepted_with_regression_debt"
    assert result.acceptance.accepted is True
```

- [ ] **Step 2: Run the test and verify it fails if prior tasks are incomplete**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_static_judge_replay.py::test_static_replay_observed_ag1_reports_target_fixed_qid \
  tests/unit/test_static_judge_replay.py::test_static_replay_preserves_rca_patches_and_accepts_bounded_debt \
  tests/unit/test_static_judge_replay.py::test_static_replay_observed_ag4_passing_to_hard_is_accepted_with_debt \
  -q
```

Expected before Tasks 4 and 7 are complete:

```text
FAILED tests/unit/test_static_judge_replay.py::test_static_replay_observed_ag1_reports_target_fixed_qid
FAILED tests/unit/test_static_judge_replay.py::test_static_replay_preserves_rca_patches_and_accepts_bounded_debt
FAILED tests/unit/test_static_judge_replay.py::test_static_replay_observed_ag4_passing_to_hard_is_accepted_with_debt
```

The AG4 replay specifically depends on Task 7 (regression-debt accounting) — without it, the replay reports `rejected_unbounded_collateral` with empty `regression_debt_qids`. The AG1 and bounded-debt cases depend on Tasks 4 and 5 (RCA-attributed patch preservation).

- [ ] **Step 3: Expose attribution-tier decisions in replay output**

In `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/static_judge_replay.py`, no behavior change is required if Task 4 added `causal_attribution_tier` to patch-cap decisions. Add this assertion-friendly field to `StaticJudgeReplayResult` only if the test needs direct access:

```python
    patch_cap_decisions: list[dict[str, Any]]
```

This field already exists in the current module; keep it as the contract.

- [ ] **Step 4: Run static replay tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest tests/unit/test_static_judge_replay.py tests/unit/test_patch_selection.py -q
```

Expected:

```text
all tests pass
```

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/static_judge_replay.py packages/genie-space-optimizer/tests/unit/test_static_judge_replay.py
git commit -m "test(optimizer): replay observed rca patch selection failure"
```

---

## Task 10: Create The Canonical Schema Document

**Files:**
- Create: `packages/genie-space-optimizer/docs/canonical-schema.md`

- [ ] **Step 1: Create the canonical schema document**

Create `packages/genie-space-optimizer/docs/canonical-schema.md` with this content:

```markdown
# Genie Space Optimizer Canonical Schema

This document is the rejection criterion for optimizer schema drift. New PRs
that introduce another alias for an existing concept must update this document
and delete an older alias in the same PR.

For the first 30 days after this document lands, treat it as frozen except for
typo fixes and corrections that remove ambiguity. After 30 days, incremental
edits are allowed only when each edit deletes or deprecates an existing alias.

## Canonical Vocabulary

| Slot | Canonical Name | Definition | Deprecated Aliases | Owner |
| --- | --- | --- | --- | --- |
| Raw judge metadata label | `asi_failure_type` | The failure label emitted by a judge's ASI metadata before optimizer normalization. | `failure_type` when used outside raw judge metadata | Judge/ASI extraction |
| Optimizer RCA class | `root_cause` | The normalized optimizer diagnosis used for clustering, ranking, and lever mapping. | `RCA defect`, `dominant defect`, `failure root` | `optimization/optimizer.py::cluster_failures` |
| Deterministic SQL diff enum | `diff_kind` | The AST-derived SQL difference enum from `feature_mining.DiffKind`. | `primary_kind` when referring to the enum type itself | `optimization/feature_mining.py` |
| Primary SQL diff value | `primary_diff_kind` | The single highest-priority `DiffKind` value surfaced to prompts and AFS. | `primary_kind` in prompt context, `structural_diff.primary_kind` | `optimization/feature_mining.py::compute_diff` |
| Prompt-safe failure projection | `failure_features` | Leak-safe, typed summary of deterministic SQL features and diff kinds. It may contain enum names and allowlisted identifiers, never raw benchmark SQL. | `_feature_diff` in prompt-facing data, raw `SqlDiff` object in AFS | `optimization/optimizer.py::_summarize_feature_diffs` |
| Cluster identity | `cluster_id` | Stable per-iteration cluster identifier such as `H001` or `S002`. | `signature` when used as the public ID | `optimization/optimizer.py::cluster_failures` |
| Action group source | `source_cluster_ids` | Cluster IDs the strategist claims the action group addresses. | `target_clusters`, `source_ids` | Strategist output schema |
| Action group questions | `affected_questions` | Benchmark question IDs the action group claims it may affect. | `target_qids` in strategist JSON | Strategist output schema |
| Patch causal targets | `target_qids` | Benchmark question IDs attached to a proposal or patch for gating and patch cap selection. | `_grounding_target_qids` outside grounding internals | Proposal grounding / patch selection |
| RCA theme ID | `rca_id` | Identifier for a typed RCA theme or execution plan that produced a proposal. | `theme_id` when used for patch provenance | RCA ledger / synthesis |
| Regression debt | `regression_debt_qids` | Bounded out-of-target hard regressions accepted because the action group produced a net causal win. | `collateral_qids`, `new regressions` when accepted | `optimization/control_plane.py::decide_control_plane_acceptance` |
| Rollback trust | `rollback_state_trusted` | Boolean control-plane state indicating whether the live space matches the pre-AG snapshot after rollback. | `rollback_verified` when used as a loop-wide learning flag | `optimization/harness.py` |

## Data Flow As Types

```text
EvalRow
  -> ASI[judge]
  -> SqlFeatures
  -> SqlDiff
  -> FailureEntry
  -> Cluster
  -> AFS
  -> ActionGroup
  -> Proposal
  -> Patch
  -> Outcome
```

| Arrow | Producer Function | Contract Test |
| --- | --- | --- |
| `EvalRow -> ASI[judge]` | `optimization/rca_failure_context.py::failure_contexts_by_qid` and ASI extraction inside `optimization/optimizer.py::analyze_failures` | `tests/unit/test_rca_failure_context.py` |
| `EvalRow -> SqlFeatures` | `optimization/feature_mining.py::mine_sql_features` | `tests/unit/test_ast_diff_threading.py` |
| `SqlFeatures -> SqlDiff` | `optimization/feature_mining.py::compute_diff` | `tests/unit/test_ast_diff_threading.py` |
| `ASI + SqlDiff -> FailureEntry` | `optimization/optimizer.py::analyze_failures` | `tests/unit/test_unified_rca_control_plane.py` |
| `FailureEntry -> Cluster` | `optimization/optimizer.py::cluster_failures` | `tests/unit/test_ast_diff_threading.py` |
| `Cluster -> AFS` | `optimization/afs.py::format_afs` | `tests/unit/test_ast_diff_threading.py` |
| `AFS -> ActionGroup` | `optimization/optimizer.py::_call_llm_for_adaptive_strategy` | `tests/unit/test_unified_rca_prompt_alignment.py` |
| `ActionGroup -> Proposal` | proposal generation paths in `optimization/harness.py`, `optimization/cluster_driven_synthesis.py`, and `optimization/synthesis.py` | `tests/unit/test_patch_causal_backfill.py` and `tests/unit/test_cluster_driven_synthesis.py` |
| `Proposal -> Patch` | `optimization/applier.py::normalize_patch` and `optimization/proposal_grounding.py` gates | `tests/unit/test_applier_proposal_metadata.py` and `tests/unit/test_proposal_grounding.py` |
| `Patch -> Outcome` | `optimization/harness.py::_run_gate_checks` and `optimization/control_plane.py::decide_control_plane_acceptance` | `tests/unit/test_static_judge_replay.py` and `tests/unit/test_control_plane.py` |

## Determinism Declaration

| Stage | Mechanism | Function Or LLM Justification |
| --- | --- | --- |
| Row classification | deterministic | `optimization/control_plane.py::row_status`, `hard_failure_qids`, and `is_actionable_soft_signal_row` classify rows from stored verdict fields. |
| ASI extraction | LLM | Judge ASI is produced by LLM-based judges; this is unavoidable because judge rationales and counterfactual fixes are semantic evaluations. |
| `SqlFeatures` / `SqlDiff` | deterministic | `optimization/feature_mining.py::mine_sql_features` and `compute_diff` use SQL parsing and structured comparison. |
| `DiffKind` classification | deterministic | `optimization/feature_mining.py::compute_diff` dispatches to fixed enum values. |
| Cluster formation | deterministic | `optimization/optimizer.py::cluster_failures` groups by normalized `root_cause` and blame. |
| Ranking | deterministic | `optimization/optimizer.py::rank_clusters` uses score formula and deterministic tiebreakers. |
| Strategist proposal | LLM | The strategist selects patch strategy and wording across competing RCA clusters; an LLM is used because cross-cluster conflict resolution and instruction wording are semantic planning tasks. |
| Synthesis: Lever 6 SQL example | LLM | SQL expression/example synthesis fills schema-bounded templates where wording and SQL shape depend on natural-language RCA context. |
| Apply, gates, acceptance | deterministic | `optimization/applier.py`, `optimization/proposal_grounding.py`, `optimization/patch_selection.py`, and `optimization/control_plane.py::decide_control_plane_acceptance` make fixed policy decisions from proposals, patches, and eval rows. |

## How To Use This Document

Use this document as a schema contract, not as an implementation plan.

- New PRs that introduce a new name for an existing slot must be rejected unless they update this document and remove or deprecate an older alias.
- New implementation plans must point at one row in the vocabulary table or one arrow in the data-flow table and state which contract they refine.
- Prompt-facing data must use canonical names unless a deprecated alias is required for backward compatibility at a boundary.
- Backward-compatible aliases are allowed only at parse boundaries and must be normalized before cluster formation, patch selection, or control-plane acceptance.

## Thirty-Day Freeze

The initial version should be reviewed by 2-3 engineers who have worked on the loop. After approval, freeze the document for 30 days. During the freeze, edits are limited to correctness fixes that reduce ambiguity. After the freeze, each schema edit must delete, rename, or explicitly deprecate at least one existing alias.
```

- [ ] **Step 2: Verify required sections exist**

Run:

```bash
python - <<'PY'
from pathlib import Path
doc = Path("packages/genie-space-optimizer/docs/canonical-schema.md").read_text()
required = [
    "## Canonical Vocabulary",
    "## Data Flow As Types",
    "## Determinism Declaration",
    "## How To Use This Document",
    "## Thirty-Day Freeze",
    "failure_type",
    "root_cause",
    "primary_diff_kind",
    "EvalRow",
    "ActionGroup",
    "Proposal",
    "Outcome",
]
missing = [item for item in required if item not in doc]
if missing:
    raise SystemExit(f"missing required canonical schema content: {missing}")
print("canonical schema document looks complete")
PY
```

Expected:

```text
canonical schema document looks complete
```

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/docs/canonical-schema.md
git commit -m "docs(optimizer): add canonical schema contract"
```

---

## Task 11: Run Focused Regression Suite

**Files:**
- No source edits.

- [ ] **Step 1: Run the focused optimizer tests**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_snapshot_contract.py \
  tests/unit/test_preflight_substeps.py::test_update_run_status_retries_delta_concurrent_append \
  tests/unit/test_patch_selection.py \
  tests/unit/test_patch_causal_backfill.py \
  tests/unit/test_quarantine_control_plane.py \
  tests/unit/test_benchmark_sql_hygiene.py \
  tests/unit/test_static_judge_replay.py \
  tests/unit/test_control_plane.py \
  -q
```

Expected:

```text
all selected tests pass
```

- [ ] **Step 2: Run the existing prompt and AST threading guards**

Run:

```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_ast_diff_threading.py \
  tests/unit/test_unified_rca_prompt_alignment.py \
  tests/unit/test_prompt_budgets.py \
  -q
```

Expected:

```text
all selected tests pass
```

- [ ] **Step 3: Commit any test-only follow-up fixes**

If Step 1 or Step 2 exposed a source/test mismatch and you fixed it, commit the focused fix:

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer packages/genie-space-optimizer/tests/unit
git commit -m "test(optimizer): verify lever loop control contracts"
```

If no fixes were required, do not create an empty commit.

---

## Task 12: Manual One-Iteration Validation

**Files:**
- No source edits.
- Capture run log as evidence in the PR description, not as a committed artifact.

- [ ] **Step 1: Start one fresh optimizer run**

Run the same deployed Databricks job path used for the latest ESR or 7Now corpus. Use a fresh `run_id` and ensure no other optimizer run is writing to the same `space_id` partition.

- [ ] **Step 2: Verify pre-AG snapshot capture**

In stdout/stderr, confirm every AG prints or logs:

```text
pre-AG snapshot captured
```

or an equivalent line containing the AG ID and a 64-character digest.

There must be no line matching:

```text
No config snapshot found in run row
```

after the preflight phase starts applying AGs.

- [ ] **Step 3: Verify patch cap selection**

For the first AG whose proposals include explicit RCA patches, confirm:

```text
PATCH CAP APPLIED
Selected proposal_ids
```

includes at least one proposal with `rca_id` and non-empty `target_qids` or `_grounding_target_qids`.

- [ ] **Step 4: Verify rollback terminal behavior**

If an AG rolls back and corrected rollback verification fails, confirm the run stops with:

```text
ROLLBACK VERIFICATION FAILED
failed_rollback_verification
```

There must not be a later strategist turn, later AG, or convergence quarantine block for unresolved hard QIDs from that run.

- [ ] **Step 5: Verify regression-debt accounting under bounded collateral**

For any AG where the post-AG eval shows exactly one passing-to-hard regression and a target-QID fix with positive net gain, confirm the control-plane block prints:

```text
reason=accepted_with_regression_debt
regression_debt_qids=...
```

There must not be a `rejected_unbounded_collateral` line that lists a non-empty `passing_to_hard_regressed_qids` while also reporting `regression_debt_qids=(none)` and at least one `target_fixed_qids` entry under `max_new_hard_regressions=1`. That exact contradiction was the AG4 symptom and is the failure shape Task 7 is designed to remove.

- [ ] **Step 6: Verify cap-log reconciliation fields**

For at least one AG, confirm the `PATCH CAP APPLIED` decision rows include `proposal_id`, `parent_proposal_id`, `expanded_patch_id`, `rca_id`, `target_qids`, `_grounding_target_qids`, `lever`, `patch_type`, and `causal_attribution_tier`. Each row must be reconcilable against a proposal-inventory entry by `parent_proposal_id`.

- [ ] **Step 7: Verify cap-log dedup**

For every `Dropped proposal_ids=[...]` line emitted by the patch cap, confirm no `expanded_patch_id` (or fallback `proposal_id`) appears more than once in the same list. The previous run produced `Dropped proposal_ids=['P001#2', 'P002#1', 'P001#2', 'P001#3', 'P001#4', 'P002#3', 'P002#4', 'P003#1']`. After Task 4 step 5b lands, that duplication must not recur.

- [ ] **Step 8: Verify run-level baseline snapshot ownership**

Confirm the run begins without printing:

```text
RUN-LEVEL CONFIG SNAPSHOT MISSING
```

If that warning appears, the app backend did not capture the run-level baseline at trigger time and the lever loop fell back to a one-shot API fetch. Record the warning text in the validation summary so the app-backend owner can fix the trigger-time path.

- [ ] **Step 9: Verify strategist coverage diagnostics**

If any iteration prints:

```text
STRATEGIST COVERAGE GAP
```

record the `uncovered_cluster_ids`, `cluster_question_counts`, and `rca_cards_present` from that block in the validation summary. This is signal for the next plan, not a failure of this one.

- [ ] **Step 10: Verify GT repair handles empty LLM responses**

There must be no `JSONDecodeError: Expecting value` stack trace in stderr. If `_attempt_gt_repair(...)` could not parse an LLM response, the loop should print a single warning identifying the repair attempt and continue. After Task 8 step 6 lands, that is the only acceptable behavior.

- [ ] **Step 11: Verify benchmark hygiene**

Confirm there are no result-row capture failures shaped like:

```text
SELECT * FROM (...;) _gvse_sample LIMIT
```

Same-store benchmark SQL may still be rejected by alignment, but the rejection should happen before persisting the benchmark as valid.

- [ ] **Step 12: Record validation summary**

Add this summary to the PR body:

```markdown
## Manual Validation

- Corpus:
- Run ID:
- Baseline accuracy:
- First AG selected RCA-attributed proposals:
- Rollback verification result:
- Terminal behavior after failed rollback verification:
- Quarantine after untrusted state:
- Regression-debt accounting on bounded passing-to-hard collateral:
- Cap-log reconciliation fields present:
- Cap-log dedup observed (no duplicate expanded_patch_ids):
- Run-level baseline snapshot present at trigger time (no `RUN-LEVEL CONFIG SNAPSHOT MISSING`):
- Strategist coverage gaps (cluster ids, RCA card present, token estimate):
- GT repair LLM JSON failures handled as soft warnings (no JSONDecodeError stack traces):
- Benchmark SQL hygiene observations:
```

Do not commit cluster stdout unless the repository already stores run artifacts for this workflow.

---

## Self-Review

### Spec Coverage

- Parsed-to-parsed rollback verification, first-diff diagnostics, terminal failed rollback verification, run-level baseline-snapshot trigger-time-capture contract, and `DELTA_CONCURRENT_APPEND` are covered by Tasks 1-3.
- RCA-attributed proposal preservation, cap-decision identity fields that allow proposal-inventory ↔ patch-cap reconciliation, and dedup of selected/dropped lists by `expanded_patch_id` are covered by Tasks 4-5 and replayed in Task 9.
- Gate disagreement instrumentation, quarantine not hiding unresolved hard failures from untrusted state, and strategist-coverage-gap diagnostics are covered by Task 6.
- The observed AG4 `rejected_unbounded_collateral` regression-debt accounting bug is covered by Task 7 and replayed end-to-end in Task 9.
- Benchmark SQL hygiene plus `_extract_json` safe handling of empty / non-JSON LLM responses (the GT-repair `JSONDecodeError` path) are covered by Task 8.
- Static replay coverage is extended in Task 9, including the observed AG1 `target_fixed_qids` case and the observed AG4 passing-to-hard accepted-with-debt case.
- The canonical schema document requested as Phase 3 is covered by Task 10.
- Verification is covered by Tasks 11-12, with explicit manual checks for the new run-level snapshot, cap dedup, coverage-gap, and JSON-safe-handling behaviors.

### Placeholder Scan

This plan contains no deferred implementation placeholders. Every task names concrete files, functions, tests, commands, and expected outcomes.

### Type Consistency

- Proposal/patch target fields use `target_qids` and `_grounding_target_qids`.
- Action-group fields use `primary_cluster_id`, `source_cluster_ids`, and `affected_questions`.
- The canonical schema document intentionally deprecates strategist JSON `target_qids` while preserving patch-level `target_qids`.
- Snapshot helpers consistently return parsed Genie config dictionaries with `captured`, `reason`, `snapshot`, and `digest`.
- Rollback verifier result dictionaries consistently use `verified`, `reason`, `expected_digest`, `live_digest`, and optional `first_diff_*` diagnostics.
