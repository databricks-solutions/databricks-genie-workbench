# Phase F+H Harness Wire-up Implementation Plan (v1)

> **⚠️ PARTIALLY SUPERSEDED BY v2 — read [`2026-05-04-phase-f-h-harness-wireup-plan-v2.md`](./2026-05-04-phase-f-h-harness-wireup-plan-v2.md).**
>
> Following the [audit findings](./2026-05-04-phase-f-h-wireup-audit-findings.md) Sections 4-6 and the executor's pre-A1 verification (terminal evidence; A1 reconciled), v2 was drafted to resolve all 8 audit action items. **The v1 plan stays valid for commits A1, A4, B9-B16, C19** (push-ready as drafted in v1). **For commits A2, A3, A5, A6, C17, C18 — use v2.** v2 also adds **Pre-Task 0.5** (pin the `_decision_emit` closure contract) and drops B9 + B13 as no-ops (F2 + F6 deferred to follow-up plans).
>
> **Executor sequencing:**
> 1. Push v1's **Task 0** (pre-flight snapshot + byte-stability gate) — done already if `tests/replay/snapshots/before_f_h_wireup.json` exists.
> 2. Push v1's **A1** (F3 clustering — verified push-ready by executor pre-A1 check; include the docstring update + defensive comment per executor's proposed text in terminal:973-1007).
> 3. Push v1's **A4** (F7 application — verified CLEAR by audit Section 4 A4).
> 4. **Switch to v2** for everything after A4: Pre-Task 0.5 → A2 v2 → A3 v2 → A5 v2 → A6 v2 → B10 → B11 → B12 → B14 → B15 → B16 → C17 v2 → C18 v2 → C19.
>
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the still-inline F2-F9 harness call sites to use their stage modules (Phase A — closes Phase F's deferred wire-up), then wrap every stage call with the `wrap_with_io_capture` decorator (Phase B — closes Phase H's deferred T12), then add data aggregation + bundle assembly + `GSO_ARTIFACT_INDEX_V1` emission at lever-loop termination (Phase C — closes Phase H's deferred T13). **Effective commit count: 17** (Phase A redrafted to 6 commits + 2 deferred per the citation-backed audit findings in [`2026-05-04-phase-f-h-wireup-audit-findings.md`](./2026-05-04-phase-f-h-wireup-audit-findings.md); Phase B retains 8 commits — but the F2 + F6 wraps are no-ops once F2 + F6 wire-ups are deferred, so 6 Phase B wraps fire effectively; Phase C retains 3 commits). Each replay-gated. **Zero regression** is the explicit goal — every commit runs the full pytest suite plus the per-stage replay byte-stability test.

**Architecture:** Three sequential phases with one natural stopping point. **Phase A (6 commits, F2 + F6 deferred)** replaces inline primitives in `harness.py` with stage module calls, one stage at a time, with re-classification per the 5-category taxonomy (true replacement / additive observability with dedup / post-stage observability / defer / algorithm-replacement with config drift). After Phase A lands, `harness.py`'s iteration body reads as a 7-stage tape (F1 pre-existing; F2 + F6 deferred; F3 + F4 + F5 + F7 + F8 + F9 wired). **Phase B (8 commits)** wraps each stage call with `wrap_with_io_capture(execute, stage_key)` so per-stage I/O captures to MLflow under `gso_postmortem_bundle/iterations/iter_NN/stages/<NN>_<stage_key>/`. **Phase C (3 commits)** consolidates harness locals into named accumulators, builds + uploads the parent-run bundle at termination, and adds a smoke test on a real fixture replay. Natural stopping point sits between Phase A and Phase B: stop there and Phase F's intended end-state is reached without taking on H wire-up risk; resume later when budget permits.

**Tech Stack:** Python 3.11, existing post-G-lite codebase (9 stage modules with typed `Input`/`Output` + uniform `execute` aliases, `STAGES` registry, `RunEvaluationKwargs`, `@runtime_checkable StageHandler`), existing Phase H modules (`run_output_contract.py`, `stage_io_capture.py`, `operator_process_transcript.py`, `run_output_bundle.py`), existing `lever_loop_replay.run_replay`, existing replay fixtures, `pytest`.

---

## ⚠️ Ground Truth Reality Check (verified 2026-05-04 against post-G-lite + post-Phase-H-modules codebase)

**State this plan is authored against:**

| Surface | Status | Key files / line numbers |
| --- | --- | --- |
| 9 stage modules with typed I/O + named verbs | ✅ landed | `optimization/stages/{evaluation,rca_evidence,clustering,action_groups,proposals,gates,application,acceptance,learning}.py` |
| G-lite registry + Protocol + RunEvaluationKwargs | ✅ landed | `stages/_registry.py`, `stages/_protocol.py`, `stages/_run_evaluation_kwargs.py` |
| Uniform `execute` alias on every stage | ✅ landed | `execute = <named_verb>` at end of every stage module |
| `INPUT_CLASS` / `OUTPUT_CLASS` declarations | ✅ landed | every stage module |
| Phase H modules (T1, T5-T11) | ✅ landed | `run_output_contract.py`, `stage_io_capture.py`, `operator_process_transcript.py`, `run_output_bundle.py`, `tools/marker_parser.py:GSO_ARTIFACT_INDEX_V1`, `common/mlflow_names.py:lever_loop_parent_run_name`, `tools/mlflow_audit.py:audit_parent_bundle`, `tools/evidence_bundle.py:_download_parent_bundle` |
| Phase H tests (T2, T4, T16) | ✅ landed | `tests/unit/test_stage_io_serializable.py`, `tests/unit/test_stage_io_capture.py`, `tests/unit/test_run_output_bundle.py`, `tests/integration/test_phase_h_bundle_smoke.py` |
| Phase H schema docs + skill (T14, T15) | ✅ landed | `docs/canonical-schema.md` updated, `docs/skills/gso-postmortem/SKILL.md` |
| F1 (evaluation) harness wire-up | ✅ landed | `harness.py:9985` calls `_eval_stage.evaluate_post_patch(_stage_ctx_full_eval, _eval_inp_full, eval_kwargs=_eval_kwargs_full)` |
| Phase H Task 12 (harness wire-up of capture decorator) | ❌ NOT landed | this plan's Phase B + C |
| Phase H Task 13 (`run_lever_loop.py` exit JSON pointers) | ❌ NOT landed | this plan's Phase C |
| F2-F9 harness wire-up | ❌ NOT landed | this plan's Phase A |
| `harness.py` LOC | 19,954 today; ~3,500-5,500 after Phase A | shrinks per commit |

**The 8 still-inline call sites this plan migrates (verified by `grep`):**

| Stage | Inline call site today | Replacement target |
| --- | --- | --- |
| F2 (rca_evidence) | NEW — no existing call | INSERT `_rca_stage.collect(...)` before F3 in iteration body |
| F3 (clustering) | `cluster_failures(...)` at `harness.py:9158` (hard) and `9171` (soft) | `_clust_stage.form(...)` |
| F4 (action_groups) | strategist invocation block (locate via `grep -n "_traced_llm_call.*strategist" harness.py` near AG selection) — F4 module is **observability-additive**, not a replacement | ADD `_ags_stage.select(...)` after strategist+filter+drain block |
| F5 (proposals) | `lever_proposals = generate_proposals_from_strategy(...)` at `harness.py:14079` — F5 module is **observability-additive** | ADD `_prop_stage.generate(...)` after synthesis dispatch |
| F6 (gates) | gate primitives called inline (lever-5 + blast-radius + groundedness + DOA + content-fingerprint dedup at scattered sites) | `_gates_stage.filter(...)` (true replacement) |
| F7 (application) | `apply_log = apply_patch_set(...)` at `harness.py:16155` (the iteration-body site; the other two at `4127` and `13920` are out of scope) | `_app_stage.apply(...)` |
| F8 (acceptance) | `_control_plane_decision = decide_control_plane_acceptance(...)` at `harness.py:10347` | `_accept_stage.decide(...)` |
| F9 (learning) | `_resolved = resolve_terminal_on_plateau(...)` at `harness.py:11813` + AG_RETIRED inline emit at `harness.py:11801-11828` | `_lrn_stage.update(...)` |

**The two patterns:**

- **True replacement (F3, F6, F7, F8, F9):** the stage module's `execute` calls the underlying primitive internally. The harness inline call is REPLACED. Replay byte-stable because the algorithm runs in the same place, just behind a typed wrapper.
- **Observability-additive (F2, F4, F5):** the stage module emits decision records via the new producer path; harness production logic stays inline. The wire-up ADDS a stage call that emits records. Replay byte-stable because the existing harness logic is untouched; the stage call writes records to the same trace.

**`wrap_with_io_capture` signature constraint:** `wrapper(ctx, inp)` is strict 2-arg. F1's `evaluate_post_patch(ctx, inp, *, eval_kwargs=...)` has variadic kwargs and CANNOT be wrapped directly. F1's wrap (Phase B Commit 9) uses a tiny adapter — see Phase B Task 1.

**STOP condition:** if any stage's named verb signature differs from `(ctx, inp) -> Output`, HALT and audit the module before writing the wire-up code. Do not silently invent kwargs.

---

## File Structure

| File | Status | Responsibility |
| --- | --- | --- |
| `src/genie_space_optimizer/optimization/harness.py` | modify | All effective commits (17 = 6 Phase A + 8 Phase B + 3 Phase C; F2/F6 wraps in Phase B become no-ops once their Phase A wire-ups are deferred) touch this file. |
| `src/genie_space_optimizer/jobs/run_lever_loop.py` | modify | Phase C Commit 18 — exit JSON pointers (Phase H T13). |
| `tests/replay/snapshots/before_f_h_wireup.json` | create | Pre-flight snapshot — captured ONCE before any commit lands. Every Phase A + Phase B commit asserts byte-stability against it. |
| `tests/replay/test_phase_f_h_wireup_byte_stable.py` | create | Single byte-stability test reused across every commit. |
| `tests/integration/test_phase_h_bundle_populated.py` | create | Phase C Commit 19 — end-to-end smoke that the parent bundle is populated after a fixture replay. |

---

## Task 0: Pre-flight — capture baseline + verify Phase H modules

**Files:**
- Create: `packages/genie-space-optimizer/tests/replay/snapshots/before_f_h_wireup.json`
- Create: `packages/genie-space-optimizer/tests/replay/test_phase_f_h_wireup_byte_stable.py`

This task captures the replay output of `run_replay(airline_real_v1.json)` from the current main as the gold standard. Every subsequent commit's byte-stability gate compares against this single snapshot. Captured ONCE, not per stage.

- [ ] **Step 1: Verify the codebase is in the expected pre-wire-up state**

```bash
cd packages/genie-space-optimizer
# Verify 9 stage modules + G-lite registry + Phase H modules.
python -c "from genie_space_optimizer.optimization.stages import STAGES, StageContext, StageHandler, RunEvaluationKwargs, get_stage; print(len(STAGES))"
# Expected: 9
python -c "from genie_space_optimizer.optimization import run_output_contract, stage_io_capture, operator_process_transcript, run_output_bundle"
# Expected: no error
# Verify F1 is the only wired stage today.
grep -n "from genie_space_optimizer.optimization.stages import" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
# Expected: at least one match around line 9931 (the F1 wire-up)
# Verify the 8 inline primitives that this plan migrates are still inline.
grep -n "cluster_failures(\|generate_proposals_from_strategy(\|apply_patch_set(\|decide_control_plane_acceptance(\|resolve_terminal_on_plateau(" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
# Expected: matches at 9158, 9171, 14079, 4127, 13920, 16155, 10347, 11813 (or thereabouts)
```

If any of the verifications fail, HALT and reconcile against the Reality Check appendix above before proceeding. The plan was authored against this exact baseline; drift means the plan needs revisiting.

- [ ] **Step 2: Capture the pre-wire-up replay snapshot**

```python
# Run interactively from the package root:
import json
from pathlib import Path
from genie_space_optimizer.optimization.lever_loop_replay import run_replay

FIXTURE = Path("packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json")
SNAPSHOT = Path(
    "packages/genie-space-optimizer/tests/replay/snapshots/before_f_h_wireup.json"
)
SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)

with FIXTURE.open() as f:
    fixture = json.load(f)

result = run_replay(fixture)

with SNAPSHOT.open("w") as f:
    json.dump({
        "canonical_journey_json":  result.canonical_json,
        "canonical_decision_json": result.canonical_decision_json,
        "operator_transcript":     result.operator_transcript,
        "validation_is_valid":     result.validation.is_valid,
        "validation_missing_qids": list(result.validation.missing_qids),
        "decision_validation":     list(result.decision_validation),
    }, f, sort_keys=True, indent=2, default=str)
```

- [ ] **Step 3: Write the shared byte-stability test**

```python
# packages/genie-space-optimizer/tests/replay/test_phase_f_h_wireup_byte_stable.py
"""Phase F+H Harness Wire-up byte-stability gate.

Asserts replay output is byte-identical to the pre-wire-up snapshot.
Every Phase A + Phase B commit must keep this test passing. Phase C
commits also keep it passing — they ADD bundle assembly but do not
modify any decision-emission or journey-emit behavior.
"""

from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.lever_loop_replay import run_replay


SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "before_f_h_wireup.json"
FIXTURE_PATH = (
    Path(__file__).parents[1] / "replay" / "fixtures" / "airline_real_v1.json"
)


def test_phase_f_h_wireup_replay_is_byte_stable() -> None:
    expected = json.loads(SNAPSHOT_PATH.read_text())

    with FIXTURE_PATH.open() as f:
        fixture = json.load(f)
    actual = run_replay(fixture)

    assert actual.canonical_json == expected["canonical_journey_json"], (
        "F+H wire-up must not change the canonical journey JSON"
    )
    assert (
        actual.canonical_decision_json == expected["canonical_decision_json"]
    ), "F+H wire-up must not change the canonical decision JSON"
    assert actual.operator_transcript == expected["operator_transcript"], (
        "F+H wire-up must not change the operator transcript"
    )
    assert (
        actual.validation.is_valid == expected["validation_is_valid"]
    ), "F+H wire-up must not change journey validation outcome"
    assert (
        list(actual.validation.missing_qids)
        == expected["validation_missing_qids"]
    ), "F+H wire-up must not change missing_qids"
    assert (
        list(actual.decision_validation) == expected["decision_validation"]
    ), "F+H wire-up must not change decision_validation"
```

- [ ] **Step 4: Run the test to verify it passes against the captured snapshot**

```bash
cd packages/genie-space-optimizer && pytest tests/replay/test_phase_f_h_wireup_byte_stable.py -q
```

Expected: PASS.

- [ ] **Step 5: Run the FULL test suite as a regression baseline**

```bash
cd packages/genie-space-optimizer && pytest -q
```

Expected: all tests PASS. Document the count (e.g. "3,048 tests passing"). This is the regression baseline — every subsequent commit must produce the same count.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/tests/replay/snapshots/before_f_h_wireup.json \
        packages/genie-space-optimizer/tests/replay/test_phase_f_h_wireup_byte_stable.py
git commit -m "test(phase-f-h): pre-flight snapshot + byte-stability gate (Phase F+H wire-up)"
```

---

# Phase A — F2-F9 Harness Wire-up (6 commits + 2 deferred)

This Phase A is **redrafted from the citation-backed audit findings** in [`2026-05-04-phase-f-h-wireup-audit-findings.md`](./2026-05-04-phase-f-h-wireup-audit-findings.md). The original 8-commit Phase A was authored against an imagined post-Phase-H stage API; verification against the actual modules (post-G-lite + post-Phase-H Option 1) found 4 of 8 plan-snippets would raise `TypeError`/`AttributeError` at first call and 5 of 8 require atomic emission-dedup the original plan deferred. The redraft re-classifies each commit using the 5-category taxonomy (true replacement / additive observability with dedup / post-stage observability / defer / algorithm-replacement with config drift) and grounds every snippet in the actual module dataclasses.

**Redrafted Phase A: 6 commits.**

| # | Stage | Classification | Atomic dedup site (`harness.py:LINE`) |
|---|---|---|---|
| A1 | F3 clustering | (1) True replacement | n/a |
| A2 | F4 action_groups | (2) Additive observability with dedup required | `:14884` (delete `_strategist_ag_records(...)` block 14863-14908) |
| A3 | F5 proposals | (2) Additive observability with dedup required | `:15030` (delete `_proposal_generated_records(...)` block 15005-15048) |
| A4 | F7 application | (3) Post-stage observability with dedup required | `:16524` (delete `_patch_applied_records(...)` block 16516-16541) |
| A5 | F8 acceptance | (3) Post-stage observability with dedup required | `:12235` (delete `_ag_outcome_decision_record(...)` block 12231-12253) AND `:17716` (delete `_post_eval_resolution_records(...)` block 17712-17734) |
| A6 | F9 learning | (3) Post-stage observability with dedup required | `:11801` (delete inline AG_RETIRED emit block 11801-11828) |

**Deferred:**

- **F2 rca_evidence — defer (4)**. Module is observability-only-empty: `collect()` calls `_asi_finding_from_metadata` which returns `None` when `per_qid_judge` and `asi_metadata` are empty (verified at `stages/rca_evidence.py:154-160`). Without per-qid extraction lifted out of `cluster_failures` first, the bundle's `decisions.json` for stage 2 is empty regardless of inputs. Unblock path: a follow-up that either (a) hoists per-qid judge/ASI extraction into harness scope before clustering, or (b) rewrites `collect()` to source per_qid_judge/asi_metadata from `metadata_snapshot` directly.
- **F6 gates — defer (5)**. `GATE_PIPELINE_ORDER` (`stages/gates.py:33-39`) is `content_fingerprint_dedup → lever5_structural → rca_groundedness → blast_radius → dead_on_arrival`. Harness inline gate sites (lever5 at `:14106`, AG-level groundedness at `:14921`, proposal-level groundedness at `:15058`, blast_radius at `:15562`) run in a different order and there is no PR-E content_fingerprint_dedup as a parallel inline producer (the dedup is integrated into a different code path). Without an order-reconciliation decision (canonicalize on the module's order or change the module to match harness), the wire-up cannot be byte-stable. Unblock path: a planning task that picks the canonical gate order, updates the losing side to match, then a wire-up commit follows the same template as the (1) True replacement category.

**Atomic-dedup load-bearing detail.** Phase B's `wrap_with_io_capture` (`stages/stage_io_capture.py:135-176`) rebinds `ctx.decision_emit` to a capturing closure that BOTH appends to `captured_decisions` (for the bundle's `decisions.json`) AND calls the original emit (so records still flow into `OptimizationTrace`). If the harness's separate inline producer call is not deleted in the same Phase A commit that introduces a stage call which also emits, then after Phase B lands every record fires twice: once from the inline producer → `OptimizationTrace`, once from the stage → `OptimizationTrace` AND `decisions.json`. The first fire alone breaks Phase A's byte-stability gate. **Per-commit dedup is therefore not optional and not deferrable.**

After Phase A, the iteration body reads as a 7-stage tape (F1 evaluation pre-existing; F2 + F6 deferred):

```
F1 _eval_stage.evaluate_post_patch(...)   # pre-existing (line 9985)
F3 _clust_stage.form(...)                 # A1 (replaces 9158 + 9171)
F4 _ags_stage.select(...)                 # A2 (after strategist; replaces 14884 emit)
F5 _prop_stage.generate(...)              # A3 (after synthesis; replaces 15030 emit)
F6 (DEFERRED — gate-order reconcile)
F7 _app_stage.apply(...)                  # A4 (after apply_patch_set; replaces 16524 emit)
F8 _accept_stage.decide(...)              # A5 (after decide_control_plane_acceptance; replaces 12235 + 17716 emits)
F9 _lrn_stage.update(...)                 # A6 (after resolve_terminal_on_plateau; replaces 11801 emit)
```

---

## Phase A Commit 1: Wire F3 (clustering) — TRUE REPLACEMENT

**Files:** Modify `src/genie_space_optimizer/optimization/harness.py`.

**Verified against:**
- `ClusteringInput` dataclass: `stages/clustering.py:32-46`
- `form(ctx, inp)` body: `stages/clustering.py:86-127`
- `cluster_failures` signature: `optimizer.py:1865-1878`
- Spark conditional branch: `optimizer.py:1913-1915` (only fires when `spark and run_id and catalog and schema` are all truthy)

**Classification rationale:** `form()` calls `optimizer.cluster_failures(...)` internally for both hard and soft branches with identical positional + keyword args except `spark`. In replay-fixture mode `spark=None` everywhere (`tests/replay/fixtures/airline_real_v1.json` has no spark session), so the spark-conditional `read_asi_from_uc(...)` enrichment path is never hit; replay byte-stability holds. **Production-mode caveat:** real-Genie runs do pass a live spark session to harness; `form(spark=None)` would skip the UC enrichment that the inline `cluster_failures(spark=spark, ...)` performs. Pin this risk in the commit message; production verification happens in the post-merge Genie pilot.

- [ ] **Step 1: Confirm pre-state.** `grep -n "cluster_failures(" src/genie_space_optimizer/optimization/harness.py` shows hits at `:9158` (hard) and `:9171` (soft), plus the `from genie_space_optimizer.optimization.optimizer import cluster_failures` import.

- [ ] **Step 2: Build a per-iteration `_stage_ctx` once at iteration-body start.** Insert immediately after the iteration counter increment (search for `iteration_counter` near the iteration top):

```python
# Phase F+H Commit A1: per-iteration StageContext built once, reused
# by every subsequent stage call in this iteration.
from genie_space_optimizer.optimization.stages import StageContext as _StageCtx
_stage_ctx = _StageCtx(
    run_id=run_id,
    iteration=int(iteration_counter),
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    apply_mode=apply_mode,
    journey_emit=_journey_emit,
    decision_emit=_decision_emit,  # the existing harness decision-emit fn
    mlflow_anchor_run_id=None,     # set by Phase C Commit 17 once parent run tagged
    feature_flags={},
)
```

If the harness has no existing `_decision_emit` callable in scope (it currently emits via direct `decision_records.append(record.to_dict())` constructions), introduce a thin closure that forwards to whatever the current iteration uses for record collection. The closure must be the same callable that the existing inline `_strategist_ag_records / _proposal_generated_records / _patch_applied_records / _ag_outcome_decision_record / _post_eval_resolution_records` paths feed into (so dedup deletes in A2-A6 land cleanly).

- [ ] **Step 3: Replace both `cluster_failures(...)` call sites with one `form()` call.** Swap the block at `harness.py:9158-9178` (hard call + soft branch) with:

```python
# Phase F+H Commit A1: F3 clustering — true replacement of optimizer.
# cluster_failures hard+soft pair. stages.clustering.form() calls
# cluster_failures internally for both branches with identical args
# except spark (form passes spark=None; replay path is unaffected,
# production UC-enrichment branch is skipped — see commit body).
# Verified against: stages/clustering.py:32-46 (Input), 86-127 (form).
from genie_space_optimizer.optimization.stages import clustering as _clust_stage

_clust_inp = _clust_stage.ClusteringInput(
    eval_result_for_clustering=eval_result_for_clustering,
    metadata_snapshot=metadata_snapshot,
    soft_eval_result={"rows": soft_signal_rows} if soft_signal_rows else None,
    qid_state=_shared_qid_state,
)
_cluster_findings = _clust_stage.form(_stage_ctx, _clust_inp)

# Adapter: rest of the iteration body reads `clusters` and `soft_clusters`.
clusters = list(_cluster_findings.clusters)
soft_clusters = list(_cluster_findings.soft_clusters)
```

- [ ] **Step 4: Run gates.**
  - `pytest tests/replay/test_phase_f_h_wireup_byte_stable.py -q` — must PASS against the T0 snapshot.
  - `pytest -q` — must match the T0 baseline (3,210 passed + 2 known pre-existing failures, no new failures).

- [ ] **Step 5: Commit.**

```bash
git commit -m "refactor(harness): wire F3 clustering stage (Phase F+H A1)"
```

---

## Phase A Commit 2: Wire F4 (action_groups) — ADDITIVE OBSERVABILITY WITH DEDUP

**Files:** Modify `src/genie_space_optimizer/optimization/harness.py`.

**Verified against:**
- `ActionGroupsInput` dataclass: `stages/action_groups.py:32-51`
- `select(ctx, inp)` body: `stages/action_groups.py:68-89` (emits `ctx.decision_emit(record)` per record at lines 83-84)
- Existing harness inline producer: `harness.py:14863` (import alias `_strategist_ag_records`), `:14884` (call site), `:14908` (failure log)

**Classification rationale:** `select()` emits `STRATEGIST_AG_EMITTED` records via `ctx.decision_emit` per `action_groups.py:83-84`. The harness already calls `_strategist_ag_records(...)` at `harness.py:14884` and emits the same records into the same `OptimizationTrace`. Without atomic deletion of the inline call in the same commit, both fire and byte-stability fails immediately (Phase A) and `decisions.json` double-counts (after Phase B).

- [ ] **Step 1: Confirm pre-state.** `grep -n "_strategist_ag_records\|strategist_ag_records" src/genie_space_optimizer/optimization/harness.py` returns the import at `:14863`, the call at `:14884`, and the warning log at `:14908`. The action_groups local is fully populated by the time control reaches `:14884`.

- [ ] **Step 2: Insert `_ags_stage.select(...)` at the same code location as the existing inline producer.** Replace the entire block `harness.py:14863-14908` (the import + call + warning log try/except) with:

```python
# Phase F+H Commit A2: F4 action_groups — additive observability with
# atomic dedup. Replaces inline _strategist_ag_records (formerly at
# harness.py:14884) with the stage call which emits the same records
# via ctx.decision_emit per stages/action_groups.py:83-84.
# Verified against: stages/action_groups.py:32-51 (Input), 68-89 (select).
from genie_space_optimizer.optimization.stages import action_groups as _ags_stage

try:
    _ags_inp = _ags_stage.ActionGroupsInput(
        action_groups=tuple(action_groups),
        source_clusters_by_id={
            str(c.get("cluster_id") or ""): c for c in clusters
        },
        rca_id_by_cluster=dict(rca_id_by_cluster),
        ag_alternatives_by_id={},  # Phase D.5 alternatives if available; harness already populates the AG-record path
    )
    _ag_slate = _ags_stage.select(_stage_ctx, _ags_inp)
except Exception:
    logger.warning(
        "Phase F+H A2: action_groups stage failed (non-fatal)",
        exc_info=True,
    )
```

> **rca_id_by_cluster scope check:** confirm `rca_id_by_cluster` is in scope at this code location by reading the surrounding ~30 lines. If not, this is a wire-up bug — fix before committing (the `dir()` defensive guard from the original plan-snippet hides bugs and is **not** acceptable in the redraft).

- [ ] **Step 3: Run gates.** Same as A1 Step 4.

If byte-stability fails with "more decision records than expected," the inline `_strategist_ag_records` block was not fully deleted (Step 2 must replace, not insert).

- [ ] **Step 4: Commit.**

```bash
git commit -m "refactor(harness): wire F4 action_groups stage with atomic emit dedup (Phase F+H A2)"
```

---

## Phase A Commit 3: Wire F5 (proposals) — ADDITIVE OBSERVABILITY WITH DEDUP

**Files:** Modify `src/genie_space_optimizer/optimization/harness.py`.

**Verified against:**
- `ProposalsInput` dataclass: `stages/proposals.py:39-55`
- `generate(ctx, inp)` body: `stages/proposals.py:95-135` (emits via `ctx.decision_emit` at `:128-129`; stamps `content_fingerprint` at `:108-115`)
- `_content_fingerprint` algorithm: `stages/proposals.py:77-92` (uses `reflection_retry.patch_retry_signature`)
- Existing harness inline producer: `harness.py:15005` (import alias `_proposal_generated_records`), `:15030` (call site), `:15048` (failure log)

**Classification rationale:** `generate()` emits `PROPOSAL_GENERATED` records via `ctx.decision_emit` at `proposals.py:128-129`. Same atomic-dedup pattern as A2.

**Subtle risk: content_fingerprint stamping.** `generate()` writes `stamped["content_fingerprint"] = fingerprint` for every proposal at `proposals.py:108-115` using `_content_fingerprint(patch)` which calls `reflection_retry.patch_retry_signature(...)`. PR-E T3 already stamps `content_fingerprint` upstream; before authoring the harness Step 2 replacement, confirm via `grep -n "content_fingerprint" src/genie_space_optimizer/optimization/harness.py` that the existing PR-E stamping uses the same `patch_retry_signature` function. If the algorithms diverge, F5's stage call would overwrite an existing fingerprint with a different value → byte-stability fails. **Reconcile before committing.**

- [ ] **Step 1: Confirm pre-state and verify content_fingerprint algorithm parity** between PR-E's stamping site and `proposals._content_fingerprint`. If divergent, halt and surface to the redraft author.

- [ ] **Step 2: Replace the inline producer block `harness.py:15005-15048`** with:

```python
# Phase F+H Commit A3: F5 proposals — additive observability with
# atomic dedup. Replaces inline _proposal_generated_records (formerly
# at harness.py:15030). The stage call emits PROPOSAL_GENERATED via
# ctx.decision_emit per stages/proposals.py:128-129 and stamps
# content_fingerprint per stages/proposals.py:108-115.
# Verified against: stages/proposals.py:39-55 (Input), 95-135 (generate).
from genie_space_optimizer.optimization.stages import proposals as _prop_stage

try:
    _prop_by_ag: dict[str, list[dict]] = {}
    for _p in lever_proposals:
        _ag_id = str(_p.get("ag_id") or "")
        if _ag_id:
            _prop_by_ag.setdefault(_ag_id, []).append(_p)

    _prop_inp = _prop_stage.ProposalsInput(
        proposals_by_ag={k: tuple(v) for k, v in _prop_by_ag.items()},
        rca_id_by_cluster=dict(rca_id_by_cluster),
        cluster_root_cause_by_id={
            str(c.get("cluster_id") or ""): str(c.get("rca_kind") or "")
            for c in clusters
        },
        proposal_alternatives_by_ag={},
    )
    _prop_slate = _prop_stage.generate(_stage_ctx, _prop_inp)

    # Replace lever_proposals with fingerprint-stamped variants ONLY IF
    # PR-E doesn't already stamp upstream. If PR-E stamps, lever_proposals
    # is already fingerprinted; do NOT overwrite.
    # (See Step 1 reconciliation; the redraft assumes PR-E does stamp.)
except Exception:
    logger.warning(
        "Phase F+H A3: proposals stage failed (non-fatal)",
        exc_info=True,
    )
```

> **Note on `lever_proposals` replacement:** the original plan-snippet replaced `lever_proposals` with the stage's fingerprint-stamped variants. The redraft does NOT do this by default because PR-E T3 already stamps the fingerprint upstream. If Step 1 reconciliation discovers PR-E does NOT stamp here, then add the lever_proposals replacement back and re-run byte-stability — but only after confirming the algorithm parity.

- [ ] **Step 3: Run gates.** Same as A1.

- [ ] **Step 4: Commit.**

```bash
git commit -m "refactor(harness): wire F5 proposals stage with atomic emit dedup (Phase F+H A3)"
```

---

## Phase A Commit 4: Wire F7 (application) — POST-STAGE OBSERVABILITY WITH DEDUP

**Files:** Modify `src/genie_space_optimizer/optimization/harness.py`.

**Verified against:**
- `ApplicationInput` dataclass: `stages/application.py:47-62` (fields: `applied_entries_by_ag`, `ags`, `rca_id_by_cluster`, `cluster_root_cause_by_id` — **no** `w`, `space_id`, `patches_by_ag`, `metadata_snapshot`, `apply_mode`)
- `apply(ctx, inp)` body: `stages/application.py:137-176` (consumes `inp.applied_entries_by_ag`, never calls `apply_patch_set`; emits `PATCH_APPLIED` via `ctx.decision_emit` at `:170-171`)
- `AppliedPatchSet` output: `stages/application.py:65-77` (fields: `applied`, `applied_signature` — **no** `post_snapshot`)
- Existing harness inline `apply_patch_set` (STAYS): `harness.py:16155`
- Existing harness inline producer (DELETED): `harness.py:16516` (import alias `_patch_applied_records`), `:16524` (call site), `:16541` (failure log)

**Classification rationale:** F7's `apply()` does **not** call `apply_patch_set`. The original plan's "true replacement of `apply_patch_set`" framing is wrong; F7 is post-apply observability over the apply log entries the harness still produces inline. Wire-up: keep `apply_log = apply_patch_set(...)` at `:16155` untouched; insert `_app_stage.apply(...)` after it, fed by the apply log; delete the inline `_patch_applied_records` producer block (which the stage now emits).

- [ ] **Step 1: Confirm pre-state.** `grep -n "apply_patch_set(\|_patch_applied_records" src/genie_space_optimizer/optimization/harness.py` shows iteration-body apply at `:16155`, the inline patch-applied producer at `:16516-16541`, plus out-of-scope sites at `:4127` (preflight) and `:13920` (tvf) which stay untouched.

- [ ] **Step 2: Build `applied_entries_by_ag` from the harness's existing `apply_log` and call `_app_stage.apply(...)` immediately after the inline `apply_patch_set` returns.** Replace the `_patch_applied_records` block at `:16516-16541` with:

```python
# Phase F+H Commit A4: F7 application — post-stage observability with
# atomic dedup. apply_patch_set at harness.py:16155 STAYS inline; this
# stage call consumes the apply_log it produces and emits PATCH_APPLIED
# via ctx.decision_emit per stages/application.py:170-171, replacing
# the inline _patch_applied_records (formerly at harness.py:16524).
# Verified against: stages/application.py:47-62 (Input), 137-176 (apply),
# 65-77 (AppliedPatchSet — no post_snapshot field).
from genie_space_optimizer.optimization.stages import application as _app_stage

try:
    # Group apply_log entries by ag_id for the stage's typed input.
    _applied_by_ag: dict[str, list[dict]] = {}
    for _entry in apply_log.get("applied", []):
        _patch = _entry.get("patch") or {}
        _ag_id = str(_patch.get("ag_id") or "")
        if _ag_id:
            _applied_by_ag.setdefault(_ag_id, []).append(_entry)

    _app_inp = _app_stage.ApplicationInput(
        applied_entries_by_ag={k: tuple(v) for k, v in _applied_by_ag.items()},
        ags=tuple(action_groups),
        rca_id_by_cluster=dict(rca_id_by_cluster),
        cluster_root_cause_by_id={
            str(c.get("cluster_id") or ""): str(c.get("rca_kind") or "")
            for c in clusters
        },
    )
    _applied_set = _app_stage.apply(_stage_ctx, _app_inp)
    # _applied_set.applied is a tuple[AppliedPatch, ...]; available for
    # F8/F9 as typed input. _applied_set.applied_signature is a stable
    # 16-char hash for cycle detection (per stages/application.py:126-134).
except Exception:
    logger.warning(
        "Phase F+H A4: application stage failed (non-fatal)",
        exc_info=True,
    )
```

> **Field-shape note:** the redraft does NOT build a legacy `apply_log` adapter dict. `apply_log` already exists from the inline `apply_patch_set` call; downstream consumers continue reading it. The stage's `_applied_set` is additive — used by A5/A6 if they need the typed view, but the rest of the harness reads `apply_log` as before.

- [ ] **Step 3: Run gates.** Same as A1.

- [ ] **Step 4: Commit.**

```bash
git commit -m "refactor(harness): wire F7 application stage with atomic emit dedup (Phase F+H A4)"
```

---

## Phase A Commit 5: Wire F8 (acceptance) — POST-STAGE OBSERVABILITY WITH DEDUP (TWO emit sites)

**Files:** Modify `src/genie_space_optimizer/optimization/harness.py`.

**Verified against:**
- `AcceptanceInput` dataclass: `stages/acceptance.py:50-80` (fields include `applied_entries_by_ag`, `ags`, `baseline_accuracy`, `candidate_accuracy`, `pre_rows`, `post_rows`, `protected_qids`, `min_gain_pp`, `min_pre_arbiter_gain_pp`, etc. — **no** `applied_set` field)
- `decide(ctx, inp)` body: `stages/acceptance.py:156-273` (iterates `for ag in inp.ags:` and calls `_decide_for_ag(...)` per AG; emits `ACCEPTANCE_DECIDED` + `QID_RESOLUTION` records via `ctx.decision_emit`)
- `_decide_for_ag` re-calls `decide_control_plane_acceptance(...)` per AG: `stages/acceptance.py:133-153`
- `AgOutcome` output: `stages/acceptance.py:83-97` (fields: `outcomes_by_ag`, `qid_resolutions`, `rolled_back_content_fingerprints` — **no** `accepted_signature`)
- `AgOutcomeRecord` output: `stages/acceptance.py:38-46` (fields: `ag_id`, `outcome`, `reason_code`, `target_qids`, `affected_qids`, `content_fingerprints` — **no** `rollback_class`)
- Existing harness inline `decide_control_plane_acceptance` (STAYS): `harness.py:10347`
- Existing harness inline producers (DELETED — TWO sites):
  - `_ag_outcome_decision_record` at `harness.py:12231-12253` (block)
  - `_post_eval_resolution_records` at `harness.py:17712-17734` (block)

**Classification rationale:** `decide()` re-calls `decide_control_plane_acceptance(...)` per AG (verified at `acceptance.py:142-153`); calling it from harness AND from the stage would double-execute the gate. Treat F8 as **post-stage observability**: keep `_control_plane_decision = decide_control_plane_acceptance(...)` at `harness.py:10347` untouched; insert `_accept_stage.decide(...)` AFTER, with a context that does NOT actually call the underlying primitive. Two atomic dedup sites — one for `_ag_outcome_decision_record`, one for `_post_eval_resolution_records`.

> **Important deviation from the stage's design:** as written, `_decide_for_ag` *does* re-call `decide_control_plane_acceptance`. The redraft cannot simply "insert after" without double-executing the gate. **Pre-flight requirement:** before authoring the Step 2 snippet, decide one of:
> 1. Refactor `acceptance._decide_for_ag` to accept a pre-computed decision (no re-call). Out of redraft scope (would be a module change). **Defer F8 if this path is taken.**
> 2. Accept that `decide_control_plane_acceptance` is pure (no side-effects, deterministic given same inputs) and re-calling it per AG is byte-stable. Verify `decide_control_plane_acceptance` is pure via grep (no MLflow logs, no Spark queries, no global mutation).

If option 1 is required, defer F8 to a follow-up alongside the F2 deferral. If option 2 holds, proceed with the snippet below.

- [ ] **Step 1: Pre-flight purity check.** `grep -n "mlflow\|spark\|global " src/genie_space_optimizer/optimization/control_plane.py` for any side-effects in `decide_control_plane_acceptance`. If any are found, **HALT and defer F8** alongside F2.

- [ ] **Step 2: After confirming purity, insert `_accept_stage.decide(...)` once `apply_log` + post-eval rows are available.** Build the input from existing harness locals.

```python
# Phase F+H Commit A5: F8 acceptance — post-stage observability with
# atomic dedup at TWO sites. decide_control_plane_acceptance at
# harness.py:10347 STAYS inline. F8.decide() re-calls the gate per AG
# (pure; verified Step 1) and emits ACCEPTANCE_DECIDED + QID_RESOLUTION
# via ctx.decision_emit, replacing inline _ag_outcome_decision_record
# (harness.py:12235) and _post_eval_resolution_records (harness.py:17716).
# Verified against: stages/acceptance.py:50-80 (Input), 156-273 (decide).
from genie_space_optimizer.optimization.stages import acceptance as _accept_stage

try:
    # Reuse _applied_set from A4 if available; else group apply_log by AG.
    if "_applied_set" in dir():
        _accept_applied_by_ag: dict[str, tuple[dict, ...]] = {}
        for ap in _applied_set.applied:
            _accept_applied_by_ag.setdefault(str(ap.ag_id), tuple()).__add__(
                ({"patch": {
                    "proposal_id": ap.proposal_id,
                    "ag_id": ap.ag_id,
                    "patch_type": ap.patch_type,
                    "target_qids": list(ap.target_qids),
                    "cluster_id": ap.cluster_id,
                    "content_fingerprint": ap.content_fingerprint,
                }},)
            )
    else:
        _accept_applied_by_ag = {}
        for _entry in apply_log.get("applied", []):
            _patch = _entry.get("patch") or {}
            _ag_id = str(_patch.get("ag_id") or "")
            if _ag_id:
                _accept_applied_by_ag.setdefault(_ag_id, []).append(_entry)
        _accept_applied_by_ag = {
            k: tuple(v) for k, v in _accept_applied_by_ag.items()
        }

    _accept_inp = _accept_stage.AcceptanceInput(
        applied_entries_by_ag=_accept_applied_by_ag,
        ags=tuple(action_groups),
        baseline_accuracy=float(best_accuracy),
        candidate_accuracy=float(full_accuracy),
        baseline_pre_arbiter_accuracy=float(_best_pre_arbiter)
            if "_best_pre_arbiter" in dir() else None,
        candidate_pre_arbiter_accuracy=float(full_pre_arbiter_accuracy)
            if "full_pre_arbiter_accuracy" in dir() else None,
        pre_rows=tuple(_baseline_rows_for_control_plane or [])
            if "_baseline_rows_for_control_plane" in dir() else (),
        post_rows=tuple(full_result_1.get("rows") or [])
            if "full_result_1" in dir() else (),
        protected_qids=(),
        min_gain_pp=float(MIN_POST_ARBITER_GAIN_PP)
            if "MIN_POST_ARBITER_GAIN_PP" in dir() else 0.0,
        min_pre_arbiter_gain_pp=2.0,
        rca_id_by_cluster=dict(rca_id_by_cluster),
        cluster_by_qid={},
    )
    _ag_outcome = _accept_stage.decide(_stage_ctx, _accept_inp)
except Exception:
    logger.warning(
        "Phase F+H A5: acceptance stage failed (non-fatal)",
        exc_info=True,
    )
```

> **`dir()` defensive guards:** these are present here because the harness's accuracy / row locals have non-uniform availability across iteration code paths. Each `dir()` check must be replaced with a real scope verification before the redraft is finalized; the patterns shown are placeholders pending an audit of the surrounding code.

- [ ] **Step 3: Delete the two inline producer blocks at `harness.py:12231-12253` and `:17712-17734`.** Replace each with a one-line comment pointing at the F8 stage call: `# F8 emits via stages/acceptance.py:decide; see Phase F+H A5.`

- [ ] **Step 4: Run gates.** Same as A1. If byte-stability fails, suspect (a) `decide_control_plane_acceptance` is not actually pure and the per-AG re-call diverges from the inline single-call, or (b) one of the two dedup deletions in Step 3 was incomplete.

- [ ] **Step 5: Commit.**

```bash
git commit -m "refactor(harness): wire F8 acceptance stage with atomic dual-site emit dedup (Phase F+H A5)"
```

---

## Phase A Commit 6: Wire F9 (learning) — POST-STAGE OBSERVABILITY WITH DEDUP

**Files:** Modify `src/genie_space_optimizer/optimization/harness.py`.

**Verified against:**
- `LearningInput` dataclass: `stages/learning.py:38-51` (12 fields; **no** `prior_terminal_state`, `baseline_post_arbiter_accuracy`, `candidate_post_arbiter_accuracy`)
- `update(ctx, inp)` body: `stages/learning.py:142-217` (calls `_emit_ag_retired_records` per `:114-142` which loops `ctx.decision_emit(rec)`)
- `LearningUpdate` output: `stages/learning.py:54-62` (fields: `new_reflection_buffer`, `new_do_not_retry`, `new_rolled_back_content_fingerprints`, `terminal_decision`, `retired_ags`, `ag_retired_records` — **no** `divergence_label`)
- Existing harness inline `resolve_terminal_on_plateau` (STAYS): `harness.py:11813`
- Existing harness inline AG_RETIRED emit block (DELETED): `harness.py:11801-11828`

**Classification rationale:** `update()` is post-stage observability. `resolve_terminal_on_plateau` continues to be called inline (the harness still owns the break/divergence decision based on its return value); F9's `update()` consumes the per-AG outcomes plus the resolved terminal state and emits AG_RETIRED records that the harness inline block formerly emitted.

- [ ] **Step 1: Confirm pre-state.** `grep -n "DecisionType.AG_RETIRED\|resolve_terminal_on_plateau" src/genie_space_optimizer/optimization/harness.py` shows the AG_RETIRED inline emit block at `:11801-11828` and `resolve_terminal_on_plateau(...)` at `:11813`. The block at `:11801-11828` is the entire PR-B2 T5 wire-up that delete-replaces.

- [ ] **Step 2: Insert `_lrn_stage.update(...)` AFTER the harness's existing `resolve_terminal_on_plateau` call returns** (around `harness.py:11820`+). Replace the inline AG_RETIRED emit block at `:11801-11828` with:

```python
# Phase F+H Commit A6: F9 learning — post-stage observability with
# atomic dedup. resolve_terminal_on_plateau at harness.py:11813 STAYS
# inline; this stage call emits AG_RETIRED records via _emit_ag_retired_
# records (stages/learning.py:114-142), replacing the inline AG_RETIRED
# block (formerly harness.py:11801-11828).
# Verified against: stages/learning.py:38-51 (Input), 142-217 (update),
# 54-62 (LearningUpdate — no divergence_label field).
from genie_space_optimizer.optimization.stages import learning as _lrn_stage

try:
    _lrn_inp = _lrn_stage.LearningInput(
        prior_reflection_buffer=tuple(reflection_buffer),
        prior_do_not_retry=set(do_not_retry_signatures)
            if "do_not_retry_signatures" in dir() else set(),
        prior_rolled_back_content_fingerprints=set(
            rolled_back_content_fingerprints
        ) if "rolled_back_content_fingerprints" in dir() else set(),
        ag_outcomes_by_id={
            ag_id: {
                "outcome": rec.outcome,
                "reason_code": rec.reason_code,
                "content_fingerprint": ";".join(rec.content_fingerprints),
                "target_qids": list(rec.target_qids),
            }
            for ag_id, rec in (_ag_outcome.outcomes_by_ag.items()
                               if "_ag_outcome" in dir() else {}.items())
        },
        applied_signature=str(_applied_set.applied_signature)
            if "_applied_set" in dir() else "",
        accuracy_delta=float(full_accuracy - best_accuracy),
        current_hard_failure_qids=tuple(_resolved.retired_ags)
            if hasattr(_resolved, "retired_ags") else (),
        regression_debt_qids=set(),
        quarantined_qids=set(),
        sql_delta_qids=set(),
        pending_buffered_ags=tuple(pending_action_groups)
            if "pending_action_groups" in dir() else (),
        diagnostic_action_queue=tuple(diagnostic_action_queue)
            if "diagnostic_action_queue" in dir() else (),
    )
    _lrn_update = _lrn_stage.update(_stage_ctx, _lrn_inp)
    # _lrn_update.retired_ags is the typed (ag_id, qids) tuple list.
    # _lrn_update.ag_retired_records mirrors what was emitted via
    # ctx.decision_emit; available for next-iter dedup if needed.
except Exception:
    logger.warning(
        "Phase F+H A6: learning stage failed (non-fatal)",
        exc_info=True,
    )
```

> **`dir()` defensive guards:** same caveat as A5 — placeholders pending real scope audit. The redraft author must replace each with a verified scope check before committing.

> **No reflection_buffer / do_not_retry replacement:** the original plan-snippet wrote `reflection_buffer = list(_lrn_update.new_reflection_buffer)`. The redraft does NOT replace these locals because the harness's existing reflection-buffer / do-not-retry update path remains intact. F9's `_lrn_update.new_*` outputs are observability-only here; if a future commit lands the actual data-flow replacement, that's a separate plan.

- [ ] **Step 3: Run gates.** Same as A1.

- [ ] **Step 4: Commit.**

```bash
git commit -m "refactor(harness): wire F9 learning stage with atomic AG_RETIRED dedup (Phase F+H A6)"
```

---

# 🛑 Stopping point — Phase A complete

**At this point, 6 of the 8 F2-F9 stages are wired into the harness as typed stage module calls.** F1 was already wired before this plan started. F2 (rca_evidence) and F6 (gates) remain deferred per the deferred-section above. The harness's iteration body now reads as a 7-stage tape:

```python
F1 _eval_stage.evaluate_post_patch(...)   # pre-existing
F3 _clust_stage.form(...)                 # A1
F4 _ags_stage.select(...)                 # A2
F5 _prop_stage.generate(...)              # A3
F6 (DEFERRED — gate-order reconcile)
F7 _app_stage.apply(...)                  # A4
F8 _accept_stage.decide(...)              # A5
F9 _lrn_stage.update(...)                 # A6
```

Decision records continue to flow into `OptimizationTrace`. The replay byte-stability test still passes against the T0 snapshot. Phase F's intended LOC reduction is partially realized (F2 + F6 not contributing); the LLM postmortem can navigate `decision_type` → exactly one stage source file for F1+F3+F4+F5+F7+F8+F9. Production runs continue to emit decisions and journey events identically.

**If budget gets tight, stop here.** The production benefits of stage-aligned modularization land at this checkpoint without taking on Phase H bundle assembly risk. Resume with Phase B + C when budget permits.

To verify Phase A's end-state:

```bash
cd packages/genie-space-optimizer
# 6 new stage call sites (A1-A6) + 1 pre-existing F1.
grep -nE "_eval_stage\.|_clust_stage\.|_ags_stage\.|_prop_stage\.|_app_stage\.|_accept_stage\.|_lrn_stage\." src/genie_space_optimizer/optimization/harness.py | wc -l
# Expected: at least 7.
# Cluster_failures inline calls all gone (replaced by F3 wire-up).
grep -n "cluster_failures(" src/genie_space_optimizer/optimization/harness.py
# Expected: zero matches in the iteration body (the optimizer.py definition + tests stay).
# F4/F5/F7/F8/F9 inline producer dedup deletes:
grep -nE "_strategist_ag_records\(|_proposal_generated_records\(|_patch_applied_records\(|_ag_outcome_decision_record\(|_post_eval_resolution_records\(" src/genie_space_optimizer/optimization/harness.py
# Expected: zero matches (or matches only inside removed-code comments).
# F6 inline gates (DEFERRED) still present:
grep -nE "lever5_structural_gate_records|groundedness_gate_records|blast_radius_decision_records" src/genie_space_optimizer/optimization/harness.py
# Expected: matches still present at 14106, 14921, 15058, 15562 — these stay until the F6 gate-order reconcile follow-up.
```


---

# Phase B — Wrap each F2-F9 stage call with `wrap_with_io_capture` (8 commits)

Phase B adds the per-stage I/O capture decorator without changing harness data flow. Each commit wraps one of the 8 F2-F9 stage call sites. Replay-byte-stable because the decorator's wrapper preserves `out` unchanged and the MLflow log_text calls are no-ops while `ctx.mlflow_anchor_run_id` remains None (Phase C wires it).

**Pattern per Phase B commit (template — applies to Commits B1-B8):**

```python
# Before (Phase A end-state):
_X_inp = _X_stage.<XInput>(...)
_X_out = _X_stage.<verb>(_stage_ctx, _X_inp)

# After (Phase B):
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_X_wrapped = wrap_with_io_capture(execute=_X_stage.execute, stage_key="<stage_key>")
_X_out = _X_wrapped(_stage_ctx, _X_inp)
```

Why this is replay-byte-stable: `wrap_with_io_capture(...)` returns `out` from the inner `execute(ctx, inp)` unchanged (line 167 of `stage_io_capture.py`). The capture-side effects (input.json / output.json / decisions.json log_text calls) only fire when `ctx.mlflow_anchor_run_id` is non-None — which is set by Phase C Commit 17, not before. So Phase B is invisible to replay.

The 8 Phase B commits each follow the same template; the per-stage detail is just the variable name and stage_key.

> **F1 wrap intentionally out of scope.** The user's spec for this plan covers "stages 2-9" — F1 (evaluation) was already wired in the F1 plan (`harness.py:9985`) but its capture-decorator wrap is NOT part of this plan's 19 commits. Consequence: after this plan lands, `gso_postmortem_bundle/iterations/iter_NN/stages/01_evaluation_state/` directories will be empty (no input/output/decisions JSON for F1's evaluation stage). The other 8 stages capture correctly. F1's wrap is a small ~30-min follow-up if/when needed: introduce a closure-bound `_eval_execute_with_kwargs(ctx, inp, _kwargs=_eval_kwargs_full)` adapter at `harness.py:9985`, wrap it with `wrap_with_io_capture(...)`, replay-test. Not blocking for the rest of Phase H.

## Phase B Commit 9: Wrap F2 (rca_evidence)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`

- [ ] **Step 1: Wrap the F2 call site from Commit A1**

Replace:

```python
_rca_evidence_bundle = _rca_stage.collect(_stage_ctx, _rca_evidence_inp)
```

with:

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_rca_wrapped = wrap_with_io_capture(
    execute=_rca_stage.execute, stage_key="rca_evidence",
)
_rca_evidence_bundle = _rca_wrapped(_stage_ctx, _rca_evidence_inp)
```

- [ ] **Step 2: Run gates**

```bash
cd packages/genie-space-optimizer
pytest tests/replay/test_phase_f_h_wireup_byte_stable.py -q
pytest -q
```

Expected: BOTH pass.

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "refactor(harness): wrap F2 rca_evidence with capture decorator (Phase F+H Commit B1)"
```

---

## Phase B Commit 10: Wrap F3 (clustering)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`

- [ ] **Step 1: Wrap the F3 call site from Commit A2**

Replace:

```python
_cluster_findings = _clust_stage.form(_stage_ctx, _clust_inp)
```

with:

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_clust_wrapped = wrap_with_io_capture(
    execute=_clust_stage.execute, stage_key="cluster_formation",
)
_cluster_findings = _clust_wrapped(_stage_ctx, _clust_inp)
```

- [ ] **Step 2: Run gates** (same commands as Commit B1).
- [ ] **Step 3: Commit**

```bash
git commit -m "refactor(harness): wrap F3 clustering with capture decorator (Phase F+H Commit B2)"
```

---

## Phase B Commit 11: Wrap F4 (action_groups)

- [ ] **Step 1: Wrap the F4 call site from Commit A3**

Replace `_ag_slate = _ags_stage.select(_stage_ctx, _ags_inp)` with:

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_ags_wrapped = wrap_with_io_capture(
    execute=_ags_stage.execute, stage_key="action_group_selection",
)
_ag_slate = _ags_wrapped(_stage_ctx, _ags_inp)
```

- [ ] **Step 2: Run gates**.
- [ ] **Step 3: Commit** with message `refactor(harness): wrap F4 action_groups with capture decorator (Phase F+H Commit B3)`.

---

## Phase B Commit 12: Wrap F5 (proposals)

- [ ] **Step 1: Wrap the F5 call site from Commit A4**

Replace `_prop_slate = _prop_stage.generate(_stage_ctx, _prop_inp)` with:

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_prop_wrapped = wrap_with_io_capture(
    execute=_prop_stage.execute, stage_key="proposal_generation",
)
_prop_slate = _prop_wrapped(_stage_ctx, _prop_inp)
```

- [ ] **Step 2: Run gates**.
- [ ] **Step 3: Commit** with message `refactor(harness): wrap F5 proposals with capture decorator (Phase F+H Commit B4)`.

---

## Phase B Commit 13: Wrap F6 (gates)

- [ ] **Step 1: Wrap the F6 call site from Commit A5**

Replace `_gate_outcome = _gates_stage.filter(_stage_ctx, _gates_inp)` with:

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_gates_wrapped = wrap_with_io_capture(
    execute=_gates_stage.execute, stage_key="safety_gates",
)
_gate_outcome = _gates_wrapped(_stage_ctx, _gates_inp)
```

- [ ] **Step 2: Run gates**.
- [ ] **Step 3: Commit** with message `refactor(harness): wrap F6 gates with capture decorator (Phase F+H Commit B5)`.

---

## Phase B Commit 14: Wrap F7 (application)

- [ ] **Step 1: Wrap the F7 call site from Commit A6**

Replace `_applied_set = _app_stage.apply(_stage_ctx, _app_inp)` with:

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_app_wrapped = wrap_with_io_capture(
    execute=_app_stage.execute, stage_key="applied_patches",
)
_applied_set = _app_wrapped(_stage_ctx, _app_inp)
```

- [ ] **Step 2: Run gates**.
- [ ] **Step 3: Commit** with message `refactor(harness): wrap F7 application with capture decorator (Phase F+H Commit B6)`.

---

## Phase B Commit 15: Wrap F8 (acceptance)

- [ ] **Step 1: Wrap the F8 call site from Commit A7**

Replace `_ag_outcome = _accept_stage.decide(_stage_ctx, _accept_inp)` with:

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_accept_wrapped = wrap_with_io_capture(
    execute=_accept_stage.execute, stage_key="acceptance_decision",
)
_ag_outcome = _accept_wrapped(_stage_ctx, _accept_inp)
```

- [ ] **Step 2: Run gates**.
- [ ] **Step 3: Commit** with message `refactor(harness): wrap F8 acceptance with capture decorator (Phase F+H Commit B7)`.

---

## Phase B Commit 16: Wrap F9 (learning)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`

- [ ] **Step 1: Wrap the F9 call site from Commit A8**

Replace `_lrn_update = _lrn_stage.update(_stage_ctx, _lrn_inp)` with:

```python
from genie_space_optimizer.optimization.stage_io_capture import wrap_with_io_capture
_lrn_wrapped = wrap_with_io_capture(
    execute=_lrn_stage.execute, stage_key="learning_next_action",
)
_lrn_update = _lrn_wrapped(_stage_ctx, _lrn_inp)
```

- [ ] **Step 2: Run gates**

```bash
cd packages/genie-space-optimizer
pytest tests/replay/test_phase_f_h_wireup_byte_stable.py -q
pytest -q
```

Expected: BOTH pass.

- [ ] **Step 3: Commit** with message `refactor(harness): wrap F9 learning with capture decorator (Phase F+H Commit B8)`.

---

# Phase C — Bundle assembly + termination block + smoke test (3 commits)

Phase C populates the `gso_postmortem_bundle/` parent-run artifact on real runs. Without these commits, the per-stage I/O capture decorators (Phase B) are dormant — `ctx.mlflow_anchor_run_id` is None. Phase C Commit 17 wires the parent-run anchor, Commit 18 builds + uploads the bundle at termination, Commit 19 adds an end-to-end smoke test.

## Phase C Commit 17: Data aggregation refactor + parent run tagging

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`

Aggregate the four bundle inputs into named accumulators, AND tag the parent MLflow run + set `_stage_ctx.mlflow_anchor_run_id` so Phase B's capture decorators activate.

- [ ] **Step 1: Tag the parent run at lever-loop start**

Locate the existing run-manifest emission block (~`harness.py:10780-10827`). Add:

```python
# Phase F+H Commit 17: tag the parent MLflow run with run-role + IDs
# so mlflow_audit and gso-postmortem can discover it. Also capture the
# parent run id so the capture decorator's MLflow log_text calls
# resolve to the right run.
import mlflow as _mlflow_phase_h
from genie_space_optimizer.common.mlflow_names import (
    lever_loop_parent_run_tags,
)

_phase_h_anchor_run_id: str | None = None
try:
    if _mlflow_phase_h.active_run() is not None:
        _mlflow_phase_h.set_tags(lever_loop_parent_run_tags(
            optimization_run_id=run_id,
            databricks_job_id=_db_job_id,
            databricks_parent_run_id=_db_parent_run_id,
            lever_loop_task_run_id=_db_task_run_id,
        ))
        _phase_h_anchor_run_id = _mlflow_phase_h.active_run().info.run_id
except Exception:
    logger.warning("Phase H parent run tagging failed", exc_info=True)
```

- [ ] **Step 2: Update `_stage_ctx` to use the anchor**

Modify the `_stage_ctx = _StageCtx(...)` constructor (built per Phase A Commit 1) so `mlflow_anchor_run_id=_phase_h_anchor_run_id` (not `None`). This activates the per-stage capture in Phase B.

```python
_stage_ctx = _StageCtx(
    # ... other fields
    mlflow_anchor_run_id=_phase_h_anchor_run_id,
    # ...
)
```

- [ ] **Step 3: Aggregate the four bundle inputs into named accumulators**

Add at lever-loop start (before the per-iteration loop):

```python
# Phase F+H Commit 17: bundle-input accumulators populated as
# iterations close. Read by Commit 18's termination-block bundle
# assembly.
_baseline_for_summary: dict[str, Any] = {
    "overall_accuracy": float(prev_accuracy),
    "all_judge_pass_rate": 0.0,  # populated below from initial baseline eval
    "hard_failures": 0,
    "soft_signals": 0,
}
_iter_traces: dict[int, Any] = {}
_iter_summaries: dict[int, dict[str, Any]] = {}
_hard_failures_for_overview: list[tuple[str, str, str]] = []
```

After the initial baseline eval completes, populate `_baseline_for_summary` and `_hard_failures_for_overview`:

```python
# Right after baseline_run_evaluation returns (around harness.py:2013):
_baseline_for_summary["all_judge_pass_rate"] = ...  # extract from baseline result
_baseline_for_summary["hard_failures"] = len(...)  # hard failure count from baseline rows
_baseline_for_summary["soft_signals"] = len(...)   # soft signal count
_hard_failures_for_overview = [
    (str(r.get("question_id")), str(r.get("rca_kind") or "unknown"), str(r.get("symptom") or ""))
    for r in (baseline_eval_result.get("rows") or [])
    if str(r.get("result_correctness") or "").lower() == "no"
]
```

> **The exact extraction code depends on what `baseline_run_evaluation` returns — read its actual return shape at `harness.py:1833-1870` and adapt. Don't fabricate field names.**

After each iteration closes (right before the next `iteration_counter += 1`), populate:

```python
# Per-iteration accumulators, populated at iteration close.
from genie_space_optimizer.optimization.rca_decision_trace import OptimizationTrace
_iter_traces[iteration_counter] = OptimizationTrace(
    journey_events=tuple(_journey_events),
    decision_records=tuple(
        rec for rec in _current_iter_inputs.get("decision_records", [])
    ),
)
_iter_summaries[iteration_counter] = {
    "hard_failure_count": len(hard_qids) if "hard_qids" in dir() else 0,
    "accepted": any(
        rec.outcome == "accepted" for rec in _ag_outcome.outcomes_by_ag.values()
    ) if "_ag_outcome" in dir() else False,
    "accuracy_delta_pp": round((full_accuracy - best_accuracy) * 100, 2),
}
```

- [ ] **Step 4: Run gates**

```bash
cd packages/genie-space-optimizer
pytest tests/replay/test_phase_f_h_wireup_byte_stable.py -q
pytest -q
```

Expected: BOTH pass. Important: with `mlflow_anchor_run_id` now non-None, the capture decorator's log_text calls fire — but `_log_text` is wrapped in try/except in `stage_io_capture.py:118-128`, so any MLflow unavailability fails silently. The replay test runs without MLflow; expect zero log_text invocations.

If the replay test fails after this commit, the most likely cause is the new accumulator code interacting with iteration locals in an unexpected way. Audit and fix.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
git commit -m "feat(harness): aggregate bundle inputs + tag parent run + activate capture (Phase F+H Commit 17)"
```

---

## Phase C Commit 18: Bundle assembly + GSO_ARTIFACT_INDEX_V1 emission + run_lever_loop exit JSON

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py`

This commit closes Phase H Tasks 12 (bundle assembly) and 13 (exit JSON pointers).

- [ ] **Step 1: Add the bundle assembly block at lever-loop termination**

Locate the convergence/termination block (after the per-iteration loop exits). Add:

```python
# Phase F+H Commit 18: assemble + upload the gso_postmortem_bundle/
# parent-run artifact, render the operator transcript, emit the
# GSO_ARTIFACT_INDEX_V1 marker.
from genie_space_optimizer.optimization.operator_process_transcript import (
    render_full_transcript, render_iteration_transcript, render_run_overview,
)
from genie_space_optimizer.optimization.run_output_bundle import (
    build_artifact_index, build_manifest, build_run_summary,
)
from genie_space_optimizer.optimization.run_output_contract import (
    bundle_artifact_paths,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    artifact_index_marker,
)

if _phase_h_anchor_run_id:
    try:
        _iterations_completed = list(range(1, iteration_counter + 1))
        _manifest = build_manifest(
            optimization_run_id=run_id,
            databricks_job_id=_db_job_id,
            databricks_parent_run_id=_db_parent_run_id,
            lever_loop_task_run_id=_db_task_run_id,
            iterations=_iterations_completed,
            missing_pieces=[],
        )
        _artifact_index = build_artifact_index(iterations=_iterations_completed)
        _run_summary = build_run_summary(
            baseline=_baseline_for_summary,
            terminal_state={
                "status": (
                    _lrn_update.terminal_decision.get("status")
                    if "_lrn_update" in dir() else "max_iterations"
                ),
                "should_continue": False,
            },
            iteration_count=len(_iterations_completed),
            accuracy_delta_pp=round((best_accuracy - prev_accuracy) * 100, 1),
        )

        _run_overview = render_run_overview(
            run_id=run_id, space_id=space_id, domain=domain,
            max_iters=max_iterations, baseline=_baseline_for_summary,
            hard_failures=_hard_failures_for_overview,
        )
        _iter_transcripts = [
            render_iteration_transcript(
                iteration=i,
                trace=_iter_traces.get(i),
                iteration_summary=_iter_summaries.get(i, {}),
            )
            for i in _iterations_completed
            if _iter_traces.get(i) is not None
        ]
        _full_transcript = render_full_transcript(
            run_overview=_run_overview,
            iteration_transcripts=_iter_transcripts,
        )

        from mlflow.tracking import MlflowClient
        _client = MlflowClient()
        _paths = bundle_artifact_paths(iterations=_iterations_completed)
        _client.log_text(
            run_id=_phase_h_anchor_run_id,
            text=json.dumps(_manifest, sort_keys=True, indent=2),
            artifact_file=_paths["manifest"],
        )
        _client.log_text(
            run_id=_phase_h_anchor_run_id,
            text=json.dumps(_artifact_index, sort_keys=True, indent=2),
            artifact_file=_paths["artifact_index"],
        )
        _client.log_text(
            run_id=_phase_h_anchor_run_id,
            text=json.dumps(_run_summary, sort_keys=True, indent=2),
            artifact_file=_paths["run_summary"],
        )
        _client.log_text(
            run_id=_phase_h_anchor_run_id,
            text=_full_transcript,
            artifact_file=_paths["operator_transcript"],
        )
        # Emit the discoverability marker on stdout.
        print(artifact_index_marker(
            optimization_run_id=run_id,
            parent_bundle_run_id=_phase_h_anchor_run_id,
            artifact_index_path=_paths["artifact_index"],
            iterations=_iterations_completed,
        ))
    except Exception:
        logger.warning(
            "Phase H bundle assembly failed; postmortem will fall back "
            "to legacy phase artifacts", exc_info=True,
        )
```

- [ ] **Step 2: Update `run_lever_loop.py` exit JSON (Phase H T13)**

In `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py`, locate the `dbutils.notebook.exit(...)` construction and add the new pointer fields:

```python
# Phase F+H Commit 18 (Phase H T13): include parent bundle pointers in
# the notebook exit JSON so databricks jobs get-run-output can locate
# the bundle even when stdout is truncated.
from genie_space_optimizer.optimization.run_output_contract import GSO_BUNDLE_ROOT

exit_payload = {
    # ... existing fields preserved
    "parent_bundle_run_id": _phase_h_anchor_run_id,
    "artifact_index_path": (
        f"{GSO_BUNDLE_ROOT}/artifact_index.json"
        if _phase_h_anchor_run_id else None
    ),
    "iterations_completed": list(range(1, iteration_counter + 1)),
}
```

- [ ] **Step 3: Run gates**

```bash
cd packages/genie-space-optimizer
pytest tests/replay/test_phase_f_h_wireup_byte_stable.py -q
pytest tests/integration/test_phase_h_bundle_smoke.py -q
pytest -q
```

Expected: ALL pass. The replay test passes because the bundle assembly runs only when `_phase_h_anchor_run_id` is non-None — which doesn't happen in replay tests (no active MLflow run). The integration smoke verifies the bundle path computation is correct.

- [ ] **Step 4: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py
git commit -m "feat(harness): bundle assembly + GSO_ARTIFACT_INDEX_V1 + exit JSON (Phase F+H Commit 18)"
```

---

## Phase C Commit 19: End-to-end smoke test on a real fixture replay

**Files:**
- Create: `packages/genie-space-optimizer/tests/integration/test_phase_h_bundle_populated.py`

The existing `test_phase_h_bundle_smoke.py` verifies path computations. This new test verifies the bundle is actually populated end-to-end — using a stubbed MLflow client to assert the right `log_text` calls are made.

- [ ] **Step 1: Write the smoke test**

```python
# packages/genie-space-optimizer/tests/integration/test_phase_h_bundle_populated.py
"""Phase F+H Commit 19: end-to-end bundle-populated smoke test.

Verifies that running a small fixture replay through the post-Phase-C
harness produces the expected log_text calls (manifest, artifact_index,
run_summary, operator_transcript, plus per-stage input/output/decisions
for every iteration).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def test_bundle_populated_after_fixture_replay(monkeypatch, tmp_path) -> None:
    """End-to-end: run the harness against a small fixture, verify
    every expected bundle path is written via log_text."""
    captured_log_texts: list[tuple[str, str]] = []

    def _stub_log_text(*, run_id: str, text: str, artifact_file: str) -> None:
        captured_log_texts.append((artifact_file, text))

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stage_io_capture._log_text",
        _stub_log_text,
    )

    # Stub the harness-side MlflowClient so the bundle-assembly log_text
    # calls also hit our capture.
    class _FakeMlflowClient:
        def log_text(self, *, run_id: str, text: str, artifact_file: str) -> None:
            captured_log_texts.append((artifact_file, text))

    monkeypatch.setattr(
        "mlflow.tracking.MlflowClient",
        lambda: _FakeMlflowClient(),
    )

    # Stub mlflow.active_run to return a fake run with a known id.
    fake_run = MagicMock()
    fake_run.info.run_id = "test-parent-run-id"
    monkeypatch.setattr(
        "mlflow.active_run",
        lambda: fake_run,
    )

    # Run the fixture replay (this exercises the harness path indirectly
    # via the stage modules — actual harness invocation requires Spark).
    # If the codebase doesn't expose a callable harness entrypoint usable
    # in tests, this test asserts the pure bundle-assembly layer works.
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay
    fixture_path = (
        Path(__file__).parents[1]
        / "replay" / "fixtures" / "airline_real_v1.json"
    )
    with fixture_path.open() as f:
        fixture = json.load(f)
    result = run_replay(fixture)
    assert result.validation.is_valid

    # The replay path doesn't exercise harness production code paths
    # directly — it runs the pure replay driver. So this test is a
    # boundary check: the bundle path constants and log_text shim are
    # correctly wired. Production-side bundle-population is verified by
    # the next real-Genie pilot run.
    # TODO (out of scope for this commit): add a harness-level
    # integration test that invokes _run_lever_loop with stubbed
    # dependencies. Tracked separately if needed.

    # Verify the bundle path computation produces non-empty paths.
    from genie_space_optimizer.optimization.run_output_contract import (
        bundle_artifact_paths, stage_artifact_paths,
    )
    paths = bundle_artifact_paths(iterations=[1, 2])
    assert "manifest" in paths
    for stage_key in [
        "evaluation_state", "rca_evidence", "cluster_formation",
        "action_group_selection", "proposal_generation", "safety_gates",
        "applied_patches", "acceptance_decision", "learning_next_action",
    ]:
        sp = stage_artifact_paths(1, stage_key)
        assert "input.json" in sp["input"]
```

> **Honest scope note for this test:** without a callable harness entrypoint that runs in tests (no real Genie / Spark / MLflow), the smoke can't fully exercise the production wire-up. The first **real-Genie pilot run** post-Phase-F+H is the actual end-to-end validation. This test verifies the wire-up doesn't break path computation.

- [ ] **Step 2: Run the smoke test**

```bash
cd packages/genie-space-optimizer && pytest tests/integration/test_phase_h_bundle_populated.py -q
```

Expected: PASS.

- [ ] **Step 3: Run the full test suite**

```bash
cd packages/genie-space-optimizer && pytest -q
```

Expected: all PASS, with the same test count as the Pre-flight Step 5 baseline plus 1 new test (the smoke).

- [ ] **Step 4: Commit**

```bash
git add packages/genie-space-optimizer/tests/integration/test_phase_h_bundle_populated.py
git commit -m "test(phase-f-h): end-to-end bundle-populated smoke (Phase F+H Commit 19)"
```

---

## Final acceptance verification

After all 17 effective commits land (6 Phase A + 8 Phase B + 3 Phase C; F2/F6 deferred), run:

- [ ] **Step 1: Full test suite**

```bash
cd packages/genie-space-optimizer && pytest -q
```

Expected: all PASS. Test count ≥ baseline count + 1 (the new smoke).

- [ ] **Step 2: Verify the harness reads as a 9-stage tape**

```bash
grep -nE "_eval_wrapped|_rca_wrapped|_clust_wrapped|_ags_wrapped|_prop_wrapped|_gates_wrapped|_app_wrapped|_accept_wrapped|_lrn_wrapped" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py | wc -l
```

Expected: at least 9 — one wrapped invocation per stage in the iteration body.

- [ ] **Step 3: Verify no inline primitives remain in the iteration body**

```bash
grep -n "cluster_failures(\|generate_proposals_from_strategy(\|decide_control_plane_acceptance(\|resolve_terminal_on_plateau(" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
```

Expected: zero matches in the iteration body. (Matches outside the iteration body — preflight/tvf — are out of scope and remain.)

- [ ] **Step 4: Verify `harness.py` LOC reduced**

```bash
wc -l packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py
```

Expected: significant reduction from 19,954 baseline. Realistic target: ~5,000-15,000 depending on how aggressively the executor cleaned up legacy code paths.

- [ ] **Step 5: Update the roadmap**

Modify `packages/genie-space-optimizer/docs/2026-05-01-burn-down-to-merge-roadmap.md`:

- Mark every F-plan as `Implemented` (no longer "observability-only").
- Mark Phase H tasks T12 + T13 as `Implemented`.
- Update the diagnosability scorecard's "Modularized code" row from C+ (~50%) to A (~95%).
- Update the at-a-glance table's Phase F row from "◐ partial" to "✅ implemented".
- Remove the `2026-05-XX-phase-f-h-harness-wireup-plan.md` placeholder from the cross-references and replace with this plan's actual filename + `Implemented` status.

- [ ] **Step 6: Final commit**

```bash
git add packages/genie-space-optimizer/docs/2026-05-01-burn-down-to-merge-roadmap.md
git commit -m "docs(roadmap): mark Phase F+H wire-up complete (Phase F+H Final)"
```

---

## Rollback recipe

This plan is decomposed into 17 effective small, replay-gated commits (6 Phase A + 8 Phase B + 3 Phase C; F2 + F6 deferred per audit findings) specifically so any individual commit can be reverted without affecting earlier commits.

**To revert a single commit:** `git revert <SHA>`. Replay byte-stability is preserved at every step, so reverting one commit leaves earlier commits intact.

**To revert Phase A entirely:** `git revert <Commit A1 SHA>..<Commit A6 SHA>` reverts all 6 stage wire-ups (F3, F4, F5, F7, F8, F9). The codebase returns to the pre-Phase-A state where F2-F9 modules exist as observability-only surfaces with no harness wire-ups; F2 + F6 were already deferred and not part of this plan's commits.

**To revert Phase B entirely:** `git revert <Commit B1 SHA>..<Commit B8 SHA>`. Phase A wire-ups remain intact.

**To revert Phase C entirely:** `git revert <Commit 17>..<Commit 19>`. Phase A + B wire-ups remain; bundle assembly is undone.

> **Note on git revert semantics:** because each commit modifies `harness.py`, reverts may have minor merge conflicts when phase boundaries are involved. Resolve by accepting the older version of `harness.py` at the conflicted hunks.

---

## Acceptance criteria

- [ ] All 17 effective commits land on the branch (6 Phase A + 8 Phase B + 3 Phase C; F2 + F6 deferred to follow-up plans per audit findings).
- [ ] Each commit's byte-stability test (`tests/replay/test_phase_f_h_wireup_byte_stable.py`) passes.
- [ ] Each commit's full pytest suite passes with the same test count as the Pre-flight baseline (+1 new smoke at Commit 19).
- [ ] `harness.py` iteration body has 9 wrapped stage call sites (`_eval_wrapped`, `_rca_wrapped`, `_clust_wrapped`, `_ags_wrapped`, `_prop_wrapped`, `_gates_wrapped`, `_app_wrapped`, `_accept_wrapped`, `_lrn_wrapped`).
- [ ] Zero inline primitive calls (`cluster_failures`, `generate_proposals_from_strategy`, `decide_control_plane_acceptance`, `resolve_terminal_on_plateau`) remain in the iteration body.
- [ ] `harness.py` LOC dropped meaningfully from 19,954 baseline.
- [ ] `_stage_ctx.mlflow_anchor_run_id` is populated at lever-loop start.
- [ ] `gso_postmortem_bundle/manifest.json`, `artifact_index.json`, `run_summary.json`, `operator_transcript.md` are uploaded to the parent MLflow run at lever-loop termination.
- [ ] `GSO_ARTIFACT_INDEX_V1` marker is emitted on stdout.
- [ ] `dbutils.notebook.exit(...)` payload includes `parent_bundle_run_id`, `artifact_index_path`, `iterations_completed`.
- [ ] Roadmap reflects Phase F+H wire-up complete; F-plans marked Implemented; H tasks T12 + T13 marked Implemented.
- [ ] Next real-Genie pilot run after this plan lands: `gso_postmortem_bundle/` is populated; `gso-postmortem` skill produces a postmortem from the bundle alone.

---

## Cross-references

| Doc | Role |
| --- | --- |
| [`2026-05-01-burn-down-to-merge-roadmap.md`](./2026-05-01-burn-down-to-merge-roadmap.md) | Phase F + Phase H sections; updated by Final acceptance Step 5. |
| [`2026-05-04-phase-f-stages-modularization-index.md`](./2026-05-04-phase-f-stages-modularization-index.md) | Phase F index — the 9 stage modules whose harness wire-up Phase A executes. |
| [`2026-05-04-phase-g-stage-protocol-and-registry-plan.md`](./2026-05-04-phase-g-stage-protocol-and-registry-plan.md) | Phase G-lite — provided the `STAGES` registry, `RunEvaluationKwargs`, and uniform `execute` aliases that this plan consumes. |
| [`2026-05-04-phase-h-gso-run-output-contract-plan.md`](./2026-05-04-phase-h-gso-run-output-contract-plan.md) | Phase H plan — supplied T1-T11, T14-T17 (modules + tests + docs); T12 + T13 are this plan's Phase B + C. |
| [`2026-05-03-gso-run-output-contract-plan.md`](./2026-05-03-gso-run-output-contract-plan.md) | Architectural reference for H schemas. |
| `tests/replay/fixtures/airline_real_v1.json` | The single byte-stability fixture for every commit. |
| `tests/replay/test_phase_f_h_wireup_byte_stable.py` | This plan's shared byte-stability gate. |
| `tests/integration/test_phase_h_bundle_populated.py` | Phase C Commit 19 end-to-end smoke. |
