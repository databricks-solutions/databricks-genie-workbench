# Phase F2 (rca_evidence) Follow-up Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. This is a **decision-then-implementation** plan: Path A is operator-driven (cost-benefit decision), Paths B / C / D are TDD implementation tracks. Pick exactly ONE implementation track after Path A and execute it.

**Goal:** decide what to do with the F2 (`stages/rca_evidence`) module that currently produces an **empty `RcaEvidenceBundle`** when called from the harness because no caller passes `per_qid_judge` or `asi_metadata`, and no downstream consumer reads the bundle.

**Background:** the F+H wire-up audit ([`2026-05-04-phase-f-h-wireup-audit-findings.md`](./2026-05-04-phase-f-h-wireup-audit-findings.md), Section 1 A1) showed that F2's `collect()` early-returns when judge/ASI metadata is empty, leaving `per_qid_evidence={}`. The redrafted F+H Phase A plan defers F2's harness wire-up entirely. Phase F therefore lands as 8-of-9 stages wired (counting F1 as already wired) — F2 is the only outlier. This follow-up plan picks a permanent disposition.

**Tech Stack:** unchanged from F2 module (`rca._asi_finding_from_metadata`, `rca._top_n_collapse_metadata_override`, `rca._safe_rca_kind`).

**Status:** post-merge. NOT a blocker for Phase E. Schedule after the main F+H wire-up + merge is complete.

---

## Path A: decide which path to take (operator-driven)

This decision is a one-shot reasoning pass over four options. Output a recommendation that subsequent paths execute.

### Task A1: Read the actual consumer landscape

**Files:**
- Read: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/rca_evidence.py`
- Read: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/clustering.py`

- [ ] **Step A1.1: Verify F2 has zero downstream consumers**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
grep -rn "RcaEvidenceBundle\|rca_evidence_by_qid\|_rca_evidence_bundle" \
  packages/genie-space-optimizer/src \
  packages/genie-space-optimizer/tests
```

Expected: only `stages/rca_evidence.py:52, 114, 181, 193` (the dataclass definition + return path + INPUT/OUTPUT_CLASS exports). Plus possibly the F+H wire-up plan's deferred stub. If the count is non-zero outside `stages/rca_evidence.py`, pause — a real consumer exists and the disposition options narrow.

- [ ] **Step A1.2: Verify F3 does NOT consume F2's output**

```bash
grep -nE "rca_evidence_by_qid|_rca_evidence_bundle" \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/clustering.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py
```

Expected: zero matches. F3's `form()` calls `cluster_failures(...)` directly, which performs its OWN per-qid evidence shaping inline (via `read_asi_from_uc(...)` for UC ASI + per-row judge verdicts). F2's `RcaEvidenceBundle` is parallel observability that nothing reads.

### Task A2: Score the four options

| Option | Approach | Cost | Benefit | Risk |
|---|---|---|---|---|
| **B** Hoist evidence extraction | Move `cluster_failures`'s internal per-qid judge/ASI dict-building into a new harness helper, call it BEFORE the F2 stage call, pass results into `RcaEvidenceInput.per_qid_judge` + `.asi_metadata` | HIGH (~6-10 hours; 200+ LOC of `cluster_failures` body to dissect; replay byte-stability gate) | F2 bundle becomes fully populated; F3 can later switch to consuming F2's output (long-term unification) | HIGH (changes algorithm-adjacent code; replay regression possible) |
| **C** Self-source from eval_rows | Rewrite `F2.collect()` to derive `per_qid_judge` from `eval_rows[i].get("judge_rationale")` / `.get("metadata")` inline; optionally fetch ASI from UC via a spark-conditional helper similar to `optimizer.py:1913-1922` | MEDIUM (~3-5 hours; F2 module change only; harness adds spark/run_id/catalog/schema kwargs to `RcaEvidenceInput`) | F2 bundle populated; no harness algorithm churn; F2 module becomes self-sufficient | MEDIUM (per-row judge field name varies by judge; ASI UC fetch duplicates `cluster_failures`'s spark dependency) |
| **D** Declare F2 dead | Remove F2 from STAGES registry, the F+H wire-up plan's deferred stub, and the bundle stage list. Mark `stages/rca_evidence.py` with a `__deprecated__` docstring banner. Phase H bundles show 8 stage dirs instead of 9. | LOW (~30 min; module deprecation + registry edit + 1 test update) | Zero risk. Saves observability cruft. Reflects the reality that F2 currently has no caller and no consumer. | LOW (loses one row of "stage observability" in the bundle — but that row was empty anyway) |
| **E** Status quo (defer indefinitely) | Leave F2 as-is. Phase H wire-up plan never wires F2. Bundle's `iter_NN/stages/02_rca_evidence/` dirs stay empty. | NONE | NONE | Compounds technical debt — every future contributor wonders why F2 exists |

- [ ] **Step A2.1: Score each option against the project's current priorities**

| Priority | B | C | D | E |
|---|---|---|---|---|
| Time-to-completion | poor (multi-day) | medium | excellent (sub-hour) | excellent (no work) |
| Risk of replay regression | high | medium | none | none |
| Long-term clarity (LLM postmortem can navigate to F2) | excellent | good | acceptable (8-stage taxonomy) | poor (mystery module) |
| Engineering effort vs. observability gain | poor (high cost, low gain) | acceptable | excellent | poor (free, but invites confusion) |
| Reversibility | poor | good | good (un-deprecate is one commit) | excellent |

- [ ] **Step A2.2: Pick the recommended path**

**Recommended: Path C** (self-source from eval_rows). Lowest-risk path that actually populates the bundle. F2 becomes a self-contained module that doesn't ask the harness to plumb judge/ASI metadata it doesn't have in scope, and the bundle becomes useful for postmortem analysis.

**Fallback: Path D** (declare F2 dead) if Step C2.1 below uncovers that per-qid judge fields don't actually live on eval_rows in a stable, judge-agnostic form.

**Reject: Path B and Path E** unless circumstances change. B is over-investment; E compounds debt.

- [ ] **Step A2.3: Document the decision**

Append to this plan's footer (or to a new `docs/2026-05-05-phase-f2-decision-log.md`) the chosen path + rationale + decision date. Then execute the corresponding Path B/C/D below.

---

## Path B: hoist evidence extraction (REJECT-DEFAULT)

Skipped unless A2.2 selects it. If selected, plan body:

1. New helper `optimizer._extract_per_qid_evidence(eval_results, metadata_snapshot, *, spark, run_id, catalog, schema)` that returns `(per_qid_judge: dict, asi_metadata: dict)`.
2. Refactor `cluster_failures(...)` to call the new helper internally (no behavior change; pure extraction refactor with byte-stable replay test).
3. Harness call site (post-F+H wire-up) calls the helper before F2, passes results in.
4. F2's `collect()` no longer empty-returns.

Estimate: 6-10 hours. Detailed task breakdown out of scope for this follow-up plan; if Path B is chosen, write a separate detailed plan at `docs/2026-05-XX-phase-f2-evidence-hoist-plan.md`.

---

## Path C: self-source `per_qid_judge` and `asi_metadata` from `RcaEvidenceInput.eval_rows` and a spark-conditional UC fetch (RECOMMENDED)

Implementation track. TDD-style with byte-stability replay gate.

### Task C1: Verify per-qid judge data lives on `eval_rows`

**Files:**
- Read: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py` (around `cluster_failures` body — locate where it reads `row.get("judge_rationale")` or equivalent)

- [ ] **Step C1.1: Find the per-row judge field reads inside `cluster_failures`**

```bash
cd packages/genie-space-optimizer
grep -nE "judge_rationale|judge_verdict|asi_failure_type|judge_name|verdict|failure_type" \
  src/genie_space_optimizer/optimization/optimizer.py | \
  awk -F: '$2 >= 1865 && $2 <= 2902' | head -30
```

Expected: a list of per-row field reads inside `cluster_failures` body. Example reads (likely):
- `row.get("judge_rationale")`
- `row.get("metadata", {}).get("failure_type")`
- `row.get("judge_name", "")`

Record the actual field names in this plan's footer.

- [ ] **Step C1.2: Verify the field names are judge-agnostic**

If the field names are specific to one judge (e.g. `arbiter_v1_verdict`), Path C is BLOCKED — the F2 module would need a per-judge dispatch which is its own complexity. In that case, fall back to Path D.

If the field names are stable across judges (e.g. `judge_rationale`, `metadata.failure_type`), proceed.

- [ ] **Step C1.3: Sample 3-5 real eval rows from a recent pilot run**

Use the most recent F+H wire-up replay fixture's `eval_rows` to verify the field shape:

```bash
cd packages/genie-space-optimizer
python -c "
import json
with open('tests/replay/fixtures/airline_real_v1.json') as f:
    fix = json.load(f)
for it in fix.get('iterations', [])[:1]:
    rows = it.get('eval_result', {}).get('rows', [])[:5]
    for r in rows:
        print({k: r.get(k) for k in
               ['question_id', 'judge_rationale', 'judge_name', 'metadata', 'verdict']})
"
```

Expected: each row carries (some subset of) `judge_rationale`, `judge_name`, `metadata.failure_type`. Record the actual schema.

### Task C2: Add spark/UC parameters to `RcaEvidenceInput`

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/rca_evidence.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_rca_evidence_self_source.py`

- [ ] **Step C2.1: Write the failing test**

```python
# packages/genie-space-optimizer/tests/unit/test_rca_evidence_self_source.py
"""Phase F2 follow-up Path C: F2.collect() self-sources per_qid_judge
and asi_metadata from eval_rows + (optional) UC ASI fetch."""

from __future__ import annotations

from genie_space_optimizer.optimization.stages.rca_evidence import (
    RcaEvidenceInput, collect,
)


class _FakeCtx:
    run_id = "test-run"
    iteration = 1
    decision_emit = staticmethod(lambda r: None)


def test_collect_populates_bundle_from_eval_rows() -> None:
    """When eval_rows carry per-row judge metadata, collect() populates
    per_qid_evidence WITHOUT requiring per_qid_judge / asi_metadata."""
    inp = RcaEvidenceInput(
        eval_rows=(
            {
                "question_id": "q1",
                "result_correctness": "no",
                "judge_name": "judge_asi",
                "judge_rationale": "missing GROUP BY",
                "metadata": {
                    "failure_type": "missing_groupby",
                    "actual_objects": ["t1.col_a"],
                    "expected_objects": ["t1.col_a", "t1.col_b"],
                },
                "genie_sql": "SELECT col_a FROM t1",
            },
        ),
        hard_failure_qids=("q1",),
        soft_signal_qids=(),
        # NEW: per_qid_judge / asi_metadata fields stay default-empty;
        # F2 should self-source.
    )
    bundle = collect(_FakeCtx(), inp)
    assert "q1" in bundle.per_qid_evidence
    assert bundle.per_qid_evidence["q1"]["judge_verdict"]
    assert bundle.rca_kinds_by_qid.get("q1")
```

- [ ] **Step C2.2: Run the test, expect FAIL**

```bash
cd packages/genie-space-optimizer
pytest tests/unit/test_rca_evidence_self_source.py -v
```

Expected: FAIL — current `collect()` early-returns on empty `per_qid_judge`/`asi_metadata`.

- [ ] **Step C2.3: Refactor `collect()` to self-source**

In `stages/rca_evidence.py`, modify `collect()` so when `inp.per_qid_judge[qid]` is empty for a qid, it falls back to extracting from the corresponding eval row. Use the field names confirmed in Step C1.3.

```python
# stages/rca_evidence.py — refactored collect() body fragment
for qid in qids:
    qstr = str(qid)
    if not qstr:
        continue
    row = rows_by_qid.get(qstr) or {}
    sql = _row_sql(row)

    # Path C: judge from input dict OR fall back to row fields.
    judge = inp.per_qid_judge.get(qstr)
    if not judge:
        judge = {
            "verdict": str(row.get("judge_rationale") or ""),
            "judge_name": str(row.get("judge_name") or ""),
            "failure_type": str(
                (row.get("metadata") or {}).get("failure_type") or ""
            ),
        }

    # Path C: ASI from input dict OR fall back to row metadata.
    asi = inp.asi_metadata.get(qstr)
    if not asi:
        asi = dict(row.get("metadata") or {})

    metadata, failure_type = _build_metadata(judge=judge, asi=asi, sql=sql)
    # ... rest unchanged ...
```

The exact field names depend on Step C1.3's findings. Adjust verbatim per real schema.

- [ ] **Step C2.4: Run the test, expect PASS**

```bash
cd packages/genie-space-optimizer
pytest tests/unit/test_rca_evidence_self_source.py -v
```

Expected: PASS. Bundle now populated from eval_rows even with `per_qid_judge={}`.

- [ ] **Step C2.5: Run the full F2 unit suite**

```bash
cd packages/genie-space-optimizer
pytest tests/unit/test_stages_rca_evidence*.py -q
```

Expected: ALL pass. The pre-existing test that asserts "empty input → empty bundle" must be updated to reflect the new fallback (or removed if it was never the right contract).

- [ ] **Step C2.6: Commit**

```bash
cd /Users/prashanth.subrahmanyam/Projects/Genie-Workbench/databricks-genie-workbench
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/rca_evidence.py \
        packages/genie-space-optimizer/tests/unit/test_rca_evidence_self_source.py
git commit -m "feat(f2): self-source per_qid_judge and asi_metadata from eval_rows (Path C)"
```

### Task C3: Wire F2 into the harness now that the bundle is non-empty

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` (insert F2 stage call between F1 and F3)

- [ ] **Step C3.1: Locate F1's call site at `harness.py:9985`**

```bash
grep -n "_eval_stage\." packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py | head -5
```

Expected: F1's call site at `:9985` (post-F1 wire-up).

- [ ] **Step C3.2: Insert F2 call immediately after F1**

```python
# Phase F2 follow-up Path C: wire F2 (rca_evidence) now that bundle is populated.
# Verified against: stages/rca_evidence.py:33-49 (Input), 114-186 (collect).
from genie_space_optimizer.optimization.stages import rca_evidence as _rca_stage

try:
    _rca_evidence_inp = _rca_stage.RcaEvidenceInput(
        eval_rows=tuple(eval_result.get("rows") or ()),
        hard_failure_qids=tuple(_hard_qids_from_eval),
        soft_signal_qids=tuple(_soft_qids_from_eval),
        per_qid_judge={},   # F2 self-sources from eval_rows (Path C)
        asi_metadata={},
    )
    _rca_evidence_bundle = _rca_stage.collect(_stage_ctx, _rca_evidence_inp)
except Exception:
    logger.warning(
        "F2 follow-up Path C: rca_evidence stage failed (non-fatal)",
        exc_info=True,
    )
```

> Locate the actual harness locals `eval_result`, `_hard_qids_from_eval`, `_soft_qids_from_eval` at the F1 insertion point. If they don't exist with those exact names, read `:9985-:9990` and adjust. The audit doc Section 6.2 forbids `dir()` guards — use real local names.

- [ ] **Step C3.3: Run the byte-stability gate**

```bash
cd packages/genie-space-optimizer
pytest tests/replay/test_phase_f_h_wireup_byte_stable.py -q
```

Expected: PASS. F2's `collect()` does not call `ctx.decision_emit` (per Section 1 evidence at audit doc lines 20), so adding the call cannot regress byte-stability.

- [ ] **Step C3.4: Wrap with capture decorator (mirrors Phase B Commit B9)**

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_rca_wrapped = wrap_with_io_capture(
    execute=_rca_stage.execute, stage_key="rca_evidence",
)
_rca_evidence_bundle = _rca_wrapped(_stage_ctx, _rca_evidence_inp)
```

- [ ] **Step C3.5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(f2): wire rca_evidence stage with capture decorator (Path C)"
```

### Task C4: Update bundle's per-stage stage_keys to include `rca_evidence`

The Phase H bundle assembly already lists `rca_evidence` in PROCESS_STAGE_ORDER per `run_output_contract.py:63-71`, so no contract change is needed. Verify:

```bash
grep -n "rca_evidence" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_output_contract.py
```

Expected: at least one match (the PROCESS_STAGE_ORDER entry).

No commit needed for Task C4.

### Task C5: Run a small replay smoke

```bash
cd packages/genie-space-optimizer
pytest tests/integration/test_phase_h_bundle_populated.py -q -v
```

Expected: PASS. The bundle's `iter_NN/stages/02_rca_evidence/` dir is now populated with non-empty `input.json` and `output.json`.

---

## Path D: declare F2 dead (FALLBACK)

If Path C is blocked by Step C1.2 (per-judge dispatch required), execute Path D instead.

### Task D1: Mark F2 module deprecated

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/rca_evidence.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/_registry.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_output_contract.py`
- Modify: `packages/genie-space-optimizer/tests/unit/test_stage_conformance.py`

- [ ] **Step D1.1: Add deprecation banner to F2 module**

In `stages/rca_evidence.py`, add at the top of the module docstring:

```python
"""Stage 2: RCA Evidence shaping (Phase F2). DEPRECATED 2026-05-05.

This module produces an `RcaEvidenceBundle` that no downstream consumer
reads (see docs/2026-05-05-phase-f2-rca-evidence-followup-plan.md Path
A1.1). The harness's existing `cluster_failures(...)` performs its own
inline per-qid evidence shaping via `read_asi_from_uc(...)` and per-row
judge field reads; F2 was intended as a typed parallel surface but was
never wired into a real consumer.

Path D (this disposition): F2 is removed from the STAGES registry and
PROCESS_STAGE_ORDER. The module file is retained so existing tests +
imports keep working until a future cleanup, but it MUST NOT be added
to the harness iteration tape.

To revive: see docs/2026-05-05-phase-f2-rca-evidence-followup-plan.md
Paths B (hoist) or C (self-source).
"""
```

- [ ] **Step D1.2: Remove F2 from `STAGES`**

In `stages/_registry.py:65-84`, remove the `rca_evidence` entry:

```python
# stages/_registry.py — STAGES tuple (Path D removes the rca_evidence entry)
STAGES: tuple[StageEntry, ...] = (
    StageEntry("evaluation_state",       evaluation,    evaluation.execute,
               evaluation.INPUT_CLASS,    evaluation.OUTPUT_CLASS),
    # rca_evidence removed per Path D (2026-05-05)
    StageEntry("cluster_formation",      clustering,    clustering.execute,
               clustering.INPUT_CLASS,    clustering.OUTPUT_CLASS),
    # ... rest unchanged
)
```

- [ ] **Step D1.3: Remove F2 from `PROCESS_STAGE_ORDER`**

In `run_output_contract.py:53-148`, remove the `rca_evidence` `ProcessStage` entry. Conformance test at `tests/unit/test_process_stage_order_matches_stages_registry.py` enforces parity, so removing from one side requires removing from the other.

- [ ] **Step D1.4: Update conformance test fixture**

In `tests/unit/test_stage_conformance.py`, ensure the test fixture's expected stage_keys excludes `rca_evidence`. Verify all 8 remaining stages still pass conformance.

- [ ] **Step D1.5: Run the full unit suite**

```bash
cd packages/genie-space-optimizer
pytest -q
```

Expected: ALL pass. Any test that imported `rca_evidence` directly continues to work (the module file stays).

- [ ] **Step D1.6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/rca_evidence.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/_registry.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/run_output_contract.py \
        packages/genie-space-optimizer/tests/unit/test_stage_conformance.py
git commit -m "refactor(f2): declare rca_evidence stage dead per Path D (no downstream consumer)"
```

---

## Self-Review

After completing the chosen path:

- [ ] All tests pass (`pytest -q`).
- [ ] If Path C: `tests/integration/test_phase_h_bundle_populated.py` shows F2 dir non-empty.
- [ ] If Path D: STAGES tuple has 8 entries; PROCESS_STAGE_ORDER has 10 entries (down from 11).
- [ ] Decision rationale documented at this plan's footer or `docs/2026-05-05-phase-f2-decision-log.md`.
- [ ] Roadmap updated to mark Phase F2 follow-up complete.

---

## Footer: decision log placeholder

(Filled in during execution.)

- Decision date: <YYYY-MM-DD>
- Path chosen: <B / C / D>
- Step C1.3 (if C): per-row judge field schema confirmed: `<list>`
- Step C1.2 (if C): blocked by per-judge dispatch? <yes / no>
- Implementer: <name>
- Final commit hash: <hash>
